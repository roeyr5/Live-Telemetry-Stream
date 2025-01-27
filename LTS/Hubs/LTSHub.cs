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
        public async Task JoinGroup(string uavName)
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, uavName);
            await Clients.Caller.SendAsync("GroupJoined", uavName);
        }

        public async Task LeaveGroup(string uavName)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, uavName);
            await Clients.Caller.SendAsync("GroupLeft", uavName);
        }

        public async Task AddParameter(string uavName , string parameter)
        {
            _iconsumeservice.AddParameter(Context.ConnectionId, uavName, parameter);
            await Clients.Caller.SendAsync("ParameterAdded", uavName, parameter);
        }
        public async Task RemoveParameter(string uavName, string parameter)
        {
            _iconsumeservice.RemoveParameter(Context.ConnectionId, uavName, parameter);
            await Clients.Caller.SendAsync("ParameterRemoved", uavName, parameter);
        }
        public override Task OnDisconnectedAsync(Exception exception)
        {
            _iconsumeservice.RemoveConnection(Context.ConnectionId);
            return base.OnDisconnectedAsync(exception);
        }
        public async Task SendMessage(Dictionary<string,string> ParametersRequired , Confluent.Kafka.Partition partition)
        {
            await Clients.All.SendAsync("ReceiveMessage", ParametersRequired , partition);
        }

    }
}




