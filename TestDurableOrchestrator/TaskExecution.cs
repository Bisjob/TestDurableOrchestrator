using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestDurableOrchestrator
{
    public class TaskExecution
    {
        public string TaskName { get; set; }

        public string Error { get; set; }

        public DateTime? NextExecution { get; set; }
    }
}
