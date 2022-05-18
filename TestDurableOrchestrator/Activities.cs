using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestDurableOrchestrator
{
    public class Activities
    {
        // TaskService taskService

        public Activities(/*TaskService taskService*/)
        {
            // Constructor needed for some DependencyInjection services :
            // this.taskService = taskService;
        }

        [FunctionName(nameof(CleanupPool))]
        public async Task CleanupPool([ActivityTrigger] string poolName)
        {
            if (string.IsNullOrWhiteSpace(poolName))
                throw new ArgumentNullException(nameof(poolName));

            // Task which clean the tasks pool before starting
            // await taskService.Clean(taskName)
            await Task.Delay(200);
        }

        [FunctionName(nameof(StartPool))]
        public async Task StartPool([ActivityTrigger] string poolName)
        {
            if (string.IsNullOrWhiteSpace(poolName))
                throw new ArgumentNullException(nameof(poolName));

            // Tell the service the pool is started
            // await taskService.Start(taskName)
            await Task.Delay(200);
        }

        [FunctionName(nameof(StopPool))]
        public async Task StopPool([ActivityTrigger] string poolName)
        {
            if (string.IsNullOrWhiteSpace(poolName))
                throw new ArgumentNullException(nameof(poolName));

            // Tell the service the pool is stopped
            // await taskService.Stop(taskName)
            await Task.Delay(200);
        }

        [FunctionName(nameof(GetNextResearchForExecution))]
        public async Task<TaskExecution> GetNextResearchForExecution([ActivityTrigger] string poolName)
        {
            if (string.IsNullOrWhiteSpace(poolName))
                throw new ArgumentNullException(nameof(poolName));

            // Retrieve the next task to execute
            //var task = await taskService.GetNextTask(poolName);
            await Task.Delay(200);
            var task = new TaskExecution()
            {
                TaskName = "test task",
                NextExecution = DateTime.Now + TimeSpan.FromHours(1)
            };

            return task;
        }

        [FunctionName(nameof(CancelExecutingTask))]
        public async Task<bool> CancelExecutingTask([ActivityTrigger] string poolName)
        {
            if (string.IsNullOrWhiteSpace(poolName))
                throw new ArgumentNullException(nameof(poolName));

            // Cancel the current executing task
            // return await taskService.CancelTask(poolName);

            await Task.Delay(200);
            return true;
        }

        [FunctionName(nameof(SetExecutingTaskDone))]
        public async Task<bool> SetExecutingTaskDone([ActivityTrigger] string poolName)
        {
            if (string.IsNullOrWhiteSpace(poolName))
                throw new ArgumentNullException(nameof(poolName));

            //return await taskService.SetExecutingTaskDone(poolName);

            await Task.Delay(200);
            return true;
        }

        [FunctionName(nameof(ExecuteResearchActivity))]
        public async Task<bool> ExecuteResearchActivity([ActivityTrigger] string taskName)
        {
            if (string.IsNullOrWhiteSpace(taskName))
                throw new ArgumentNullException(nameof(taskName));

            bool error = false;

            try
            {
                // var task = await taskService.GetTask(taskName);
                // error = await task.Execute();

                await Task.Delay(Random.Shared.Next(1000, 10000));
            }
            catch (Exception e)
            {
                error = true;
            }
            return error;
        }

    }
}
