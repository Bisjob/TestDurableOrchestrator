using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestDurableOrchestrator
{
    public class Orchestrations
    {
        private static ConcurrentDictionary<string, CancellationTokenSource> tokens = new();

        public Orchestrations()
        {
            // Constructor needed for some DependencyInjection services
        }

        [FunctionName(nameof(CleanAndRunScrapingWatchdog))]
        public async Task CleanAndRunScrapingWatchdog(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var poolName = context.GetInput<string>();
            var id = $"{context.InstanceId}:wd";
            await context.CallActivityAsync(nameof(Activities.CleanupPool), poolName);
            await context.CallActivityAsync(nameof(Activities.StartPool), poolName);

            if (!tokens.ContainsKey(poolName))
                tokens.TryAdd(poolName, new CancellationTokenSource());
            else
                tokens[poolName].TryReset();

            await context.CallSubOrchestratorAsync(nameof(RunTaskWatchdog), id, poolName);
        }

        [FunctionName(nameof(StopWatchdog))]
        public async Task StopWatchdog(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var poolName = context.GetInput<string>();

            if (!context.IsReplaying && tokens.ContainsKey(poolName))
                tokens[poolName].Cancel();

            await context.CallActivityAsync(nameof(Activities.StopPool), poolName);
        }

        [FunctionName(nameof(RunTaskWatchdog))]
        public async Task RunTaskWatchdog(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var poolName = context.GetInput<string>();
            TaskStatus output = new()
            {
                PoolName = poolName,
                Message = "Getting next task"
            };
            context.SetCustomStatus(output);

            // Retrieve the next task to execute
            var task = await context.CallActivityAsync<TaskExecution>(nameof(Activities.GetNextResearchForExecution), poolName);
            if (task == null)
            {
                output.Message = "Unable to get a task";
                context.SetCustomStatus(output);
                return;
            }
            if (string.IsNullOrEmpty(task.TaskName))
            {
                output.Message = task.Error;
                context.SetCustomStatus(output);
                return;
            }

            output.Message = "Got task";
            output.CurrentTask = task;
            if (task.NextExecution.HasValue)
            {
                // If the next task can't be executed before a date, sleep till the good date
                output.Message = "Need to wait to respect task delay";
                output.NextExcecution = task.NextExecution.Value;
                context.SetCustomStatus(output);

                try
                {
                    await context.CreateTimer(task.NextExecution.Value, tokens[poolName].Token);
                }
                catch (OperationCanceledException)
                {
                    output.Message = "Operation cancelled while sleeping";
                    output.NextExcecution = null;
                    context.SetCustomStatus(output);
                    var ok = await context.CallActivityAsync<bool>(nameof(Activities.CancelExecutingTask), poolName);
                    return;

                    // After this line, the CustomStatus has not changed, and the orchestration is still in running state
                }
            }
            else
            {
                context.SetCustomStatus(output);
            }

            var executionSuccess = await context.CallActivityAsync<bool>(nameof(Activities.ExecuteResearchActivity), task.TaskName);

            var doneSuccess = await context.CallActivityAsync<bool>(nameof(Activities.SetExecutingTaskDone), poolName);

            DateTime nextTask = context.CurrentUtcDateTime.AddSeconds(10);
            output.Message = "Waiting next execution";
            output.NextExcecution = nextTask;
            output.CurrentTask = null;
            context.SetCustomStatus(output);

            try
            {
                await context.CreateTimer(nextTask, tokens[poolName].Token);
            }
            catch (OperationCanceledException)
            {
                output.Message = "Operation cancelled";
                output.NextExcecution = null;
                context.SetCustomStatus(output);
                return;
            }

            context.ContinueAsNew(poolName);
        }
    }
}
