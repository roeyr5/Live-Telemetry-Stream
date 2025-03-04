using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LTS.Entities
{
    public class ChannelDTO
    {
        public int uavNumber { get; set; }
        public string address { get; set; }
        public int port { get; set; }
#nullable enable

        public string? channel { get; set; }
#nullable enable

        public string? type { get; set; }
#nullable enable

        public bool? pcap { get; set; }
#nullable enable

        public string? status { get; set; }

    }
}
