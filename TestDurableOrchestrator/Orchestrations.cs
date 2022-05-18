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

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Clean pool {poolName}");

            // Clean the pool before starting it
            await context.CallActivityAsync(nameof(Activities.CleanupPool), poolName);

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Start pool {poolName}");
            
            // Starts the pool
            await context.CallActivityAsync(nameof(Activities.StartPool), poolName);

            // Create a new CancellationTokenSource
            if (!tokens.ContainsKey(poolName))
                tokens.TryAdd(poolName, new CancellationTokenSource());
            else
                tokens[poolName].TryReset();

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Start watchdog {poolName}");
            
            // Run the watchdog
            await context.CallSubOrchestratorAsync(nameof(RunTaskWatchdog), id, poolName);
        }

        [FunctionName(nameof(StopWatchdog))]
        public async Task StopWatchdog(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var poolName = context.GetInput<string>();

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Stopping watchdog {poolName}");
            
            // Cancel Token
            if (!context.IsReplaying && tokens.ContainsKey(poolName))
                tokens[poolName].Cancel();

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Stop pool {poolName}");
                
            // Stops the pool
            await context.CallActivityAsync(nameof(Activities.StopPool), poolName);

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Watchdog stopped {poolName}");
        }

        [FunctionName(nameof(RunTaskWatchdog))]
        public async Task RunTaskWatchdog(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var poolName = context.GetInput<string>();

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Watchdog starts {poolName}");

            TaskStatus output = new()
            {
                PoolName = poolName,
                Message = "Getting next task"
            };
            context.SetCustomStatus(output);

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Get new task {poolName}");

            // Retrieve the next task to execute
            var task = await context.CallActivityAsync<TaskExecution>(nameof(Activities.GetNextResearchForExecution), poolName);


            // Do some checks
            if (task == null)
            {
                output.Message = "Unable to get a task";
                context.SetCustomStatus(output);

                if (!context.IsReplaying)
                    Console.WriteLine($"{context.InstanceId}: Task null {poolName}");

                return;
            }
            if (string.IsNullOrEmpty(task.TaskName))
            {
                output.Message = task.Error;
                context.SetCustomStatus(output);

                if (!context.IsReplaying)
                    Console.WriteLine($"{context.InstanceId}: No Task Name {poolName}");

                return;
            }

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Task obtained {poolName} : {task.TaskName}");

            // Set output message we've got a new task to execute
            output.Message = "Got task";
            output.CurrentTask = task;
            if (task.NextExecution.HasValue && task.NextExecution.Value > context.CurrentUtcDateTime)
            {
                if (!context.IsReplaying)
                    Console.WriteLine($"{context.InstanceId}: Need to wait {poolName} : {task.TaskName}");

                // If the next task can't be executed before a date, sleep till the good date
                output.Message = "Need to wait to respect task delay";
                output.NextExcecution = task.NextExecution.Value;
                context.SetCustomStatus(output);


                using (var cts = new CancellationTokenSource())
                {
                    Task sleepingTask = context.CreateTimer(task.NextExecution.Value, output, cts.Token);
                    Task timeoutTask = context.WaitForExternalEvent("stop");

                    Task winner = await Task.WhenAny(sleepingTask, timeoutTask);
                    if (winner == sleepingTask)
                    {
                        // success case
                        cts.Cancel();

                        // Can continue
                    }
                    else
                    {
                        if (!context.IsReplaying)
                            Console.WriteLine($"{context.InstanceId}: wait cancelled {poolName} : {task.TaskName}");

                        output.Message = "Operation cancelled while sleeping";
                        output.NextExcecution = null;
                        context.SetCustomStatus(output);

                        // If the watchdog has been cancelled, we need to telle the pool the task is cancelled
                        var ok = await context.CallActivityAsync<bool>(nameof(Activities.CancelExecutingTask), poolName);

                        if (!context.IsReplaying)
                            Console.WriteLine($"{context.InstanceId}: task correctly canceled {poolName} : {task.TaskName}");

                        // Stop the watchdog
                        return;
                    }
                }
            }
            else
            {
                context.SetCustomStatus(output);
            }

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Executng task {poolName} : {task.TaskName}");

            // Execute the task
            var executionSuccess = await context.CallActivityAsync<bool>(nameof(Activities.ExecuteTaskActivity), task.TaskName);

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: Set task done {poolName} : {task.TaskName}");

            // Tells the pool the task has been executed
            var doneSuccess = await context.CallActivityAsync<bool>(nameof(Activities.SetExecutingTaskDone), poolName);

            // Sleep a bit before starting again
            DateTime nextTask = context.CurrentUtcDateTime.AddSeconds(10);
            output.Message = "Waiting next execution";
            output.NextExcecution = nextTask;
            output.CurrentTask = null;
            context.SetCustomStatus(output);


            using (var cts = new CancellationTokenSource())
            {
                Task sleepingTask = context.CreateTimer(nextTask, output, cts.Token);
                Task timeoutTask = context.WaitForExternalEvent("stop");

                Task winner = await Task.WhenAny(sleepingTask, timeoutTask);
                if (winner == sleepingTask)
                {
                    // success case
                    cts.Cancel();

                    // Can continue
                }
                else
                {
                    output.Message = "Operation cancelled";
                    output.NextExcecution = null;
                    context.SetCustomStatus(output);
                    return;
                }
            }

            if (!context.IsReplaying)
                Console.WriteLine($"{context.InstanceId}: continue {poolName} : {task.TaskName}");
            context.ContinueAsNew(poolName);
        }
    }
}
