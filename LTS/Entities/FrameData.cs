using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LTS.Entities
{
    public class FrameData
    {
        public string parameterName { get; set; }
        public string parameterValue { get; set; }

        public FrameData(string parameterName, string parameterValue)
        {
            this.parameterName = parameterName;
            this.parameterValue = parameterValue;
        }
    }
}
