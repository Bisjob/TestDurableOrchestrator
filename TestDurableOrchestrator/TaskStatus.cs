using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestDurableOrchestrator
{
    /// <summary>
    /// 
    /// </summary>
    public class TaskStatus
    {
        public string PoolName { get; set; }
        public string Message { get; set; }
        public bool InError { get; set; }
        public TaskExecution CurrentTask { get; set; }
        public DateTime? NextExcecution { get; set; }
    }
}
