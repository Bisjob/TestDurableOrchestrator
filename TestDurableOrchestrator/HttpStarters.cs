using DurableTask.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestDurableOrchestrator
{
    public class HttpStarters
    {
        const string WatchdogScrapingFunctionId = "start-watchdog";
        const string StopWatchdogScrapingFunctionId = "stop-watchdog";

        public HttpStarters()
        {
            // Constructor needed for some DependencyInjection services
        }

        [FunctionName(nameof(CleanupOrchestration))]
        public async Task<IActionResult> CleanupOrchestration(
            [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient starter)
        {
            Contract.Assume(starter != null);
            var requestPurgeResult = await starter.PurgeInstanceHistoryAsync(DateTime.MinValue, null, ((OrchestrationStatus[])Enum.GetValues(typeof(OrchestrationStatus))));
            return new OkResult();
        }

        [FunctionName(nameof(TerminateAllOrchestration))]
        public async Task<IActionResult> TerminateAllOrchestration(
            [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient starter)
        {
            Contract.Assume(starter != null);
            var tasks = await starter.GetStatusAsync();
            foreach (var t in tasks)
            {
                try
                {
                    await starter.TerminateAsync(t.InstanceId, "Terminate all");
                }
                catch
                {

                }
            }
            return new OkResult();
        }

        [FunctionName(nameof(StartWatchdogTrigger))]
        public async Task<IActionResult> StartWatchdogTrigger(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "StartWatchdog")] HttpRequest request,
            [DurableClient] IDurableOrchestrationClient starter)
        {
            var taskName = "testTaskId";
            string id = $"{WatchdogScrapingFunctionId}_{taskName}";
            var existingInstance = await starter.GetStatusAsync(id);

            if (existingInstance == null
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Completed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Failed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Terminated)
            {
                await starter.StartNewAsync(nameof(Orchestrations.CleanAndRunScrapingWatchdog), id, taskName);
                return new OkObjectResult("Watchdog started");
            }

            return new BadRequestObjectResult("Watchdog already started");
        }


        [FunctionName(nameof(StopWatchdogTrigger))]
        public async Task<IActionResult> StopWatchdogTrigger(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "StopWatchdog")] HttpRequest request,
            [DurableClient] IDurableOrchestrationClient starter)
        {
            var taskName = "testTaskId";
            string id = $"{StopWatchdogScrapingFunctionId}_{taskName}";
            var existingInstance = await starter.GetStatusAsync(id);

            if (existingInstance == null
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Completed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Failed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Terminated)
            {
                await starter.StartNewAsync(nameof(Orchestrations.StopWatchdog), id, taskName);
                return new OkObjectResult("Watchdog stoping");
            }

            return new BadRequestObjectResult("Watchdog already stoping");
        }
    }
}
