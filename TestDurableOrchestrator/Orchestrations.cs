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

            // Clean the pool before starting it
            await context.CallActivityAsync(nameof(Activities.CleanupPool), poolName);

            // Starts the pool
            await context.CallActivityAsync(nameof(Activities.StartPool), poolName);

            // Create a new CancellationTokenSource
            if (!tokens.ContainsKey(poolName))
                tokens.TryAdd(poolName, new CancellationTokenSource());
            else
                tokens[poolName].TryReset();

            // Run the watchdog
            await context.CallSubOrchestratorAsync(nameof(RunTaskWatchdog), id, poolName);
        }

        [FunctionName(nameof(StopWatchdog))]
        public async Task StopWatchdog(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var poolName = context.GetInput<string>();

            // Cancel Token
            if (!context.IsReplaying && tokens.ContainsKey(poolName))
                tokens[poolName].Cancel();

            // Stops the pool
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
            
            // Do some checks
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

            // Set output message we've got a new task to execute
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

                    // If the watchdog has been cancelled, we need to telle the pool the task is cancelled
                    var ok = await context.CallActivityAsync<bool>(nameof(Activities.CancelExecutingTask), poolName);

                    // Stop the watchdog
                    return;

                    // After this line, the CustomStatus has not changed, and the orchestration is still in running state
                }
            }
            else
            {
                context.SetCustomStatus(output);
            }

            // Execute the task
            var executionSuccess = await context.CallActivityAsync<bool>(nameof(Activities.ExecuteTaskActivity), task.TaskName);


            // Tells the pool the task has been executed
            var doneSuccess = await context.CallActivityAsync<bool>(nameof(Activities.SetExecutingTaskDone), poolName);

            // Sleep a bit before starting again
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
                // Same here if we need to cancel
                output.Message = "Operation cancelled";
                output.NextExcecution = null;
                context.SetCustomStatus(output);
                return;
            }

            context.ContinueAsNew(poolName);
        }
    }
}
