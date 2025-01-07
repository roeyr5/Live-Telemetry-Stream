using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LTS.Services
{
        public class KafkaSettings
        {
            public string BootstrapServers { get; set; }
            public string Topic { get; set; }
            public string GroupId { get; set; }
        }
}
