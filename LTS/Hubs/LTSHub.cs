using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using LTS.Services;

namespace LTS.Hubs
{
    public class LTSHub : Hub
    {
        private readonly IConsume _iconsumeservice;
        public LTSHub(IConsume iconsumeService)
        {
            _iconsumeservice = iconsumeService;
        }

        public async Task AddParameter(string parameter)
        {
            _iconsumeservice.AddParameter(parameter);
            await Clients.All.SendAsync("ParameterAdded", parameter);
        }
        public async Task RemoveParameter(string parameter)
        {
            _iconsumeservice.RemoveParameter(parameter);
            await Clients.All.SendAsync("ParameterRemoved", parameter);
        }
        public async Task SendMessage(Dictionary<string,string> ParametersRequired , Confluent.Kafka.Partition partition)
        {
            await Clients.All.SendAsync("ReceiveMessage", ParametersRequired , partition);
        }
    }
}
