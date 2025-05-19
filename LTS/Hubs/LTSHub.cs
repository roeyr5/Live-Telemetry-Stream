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
        private static Dictionary<string, List<string>> _connectionGroups;

        public LTSHub(IConsume iconsumeService)
        {
            _iconsumeservice = iconsumeService;
            _connectionGroups = new();
        }
        public async Task JoinGroup(string groupName)
        {
            if (!_connectionGroups.ContainsKey(Context.ConnectionId))
            {
                _connectionGroups[Context.ConnectionId] = new List<string>();
            }

            if (!_connectionGroups[Context.ConnectionId].Contains(groupName))
            {
                _connectionGroups[Context.ConnectionId].Add(groupName);
                await Groups.AddToGroupAsync(Context.ConnectionId, groupName);
            }
            await Clients.Caller.SendAsync("GroupJoined", groupName);
        }

        public async Task LeaveGroup(string groupName)
        {
            if (_connectionGroups.ContainsKey(Context.ConnectionId) && _connectionGroups[Context.ConnectionId].Contains(groupName))
            {
                await Groups.RemoveFromGroupAsync(Context.ConnectionId, groupName);
                _connectionGroups[Context.ConnectionId].Remove(groupName);
                if (_connectionGroups[Context.ConnectionId].Count == 0)
                {
                    _connectionGroups.Remove(Context.ConnectionId);
                }
                await Clients.Caller.SendAsync("GroupLeft", groupName);
            }
            await Clients.Caller.SendAsync("GroupLeft", groupName);
        }

        //public bool IsUserInGroup(string groupName)
        //{
        //    return connectionGroups.ContainsKey(Context.ConnectionId) &&
        //           connectionGroups[Context.ConnectionId].Contains(groupName);
        //}

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

        public override async Task OnConnectedAsync()
        {
            _iconsumeservice.AddConnection(Context.ConnectionId);
        }
        public override async Task OnDisconnectedAsync(Exception exception)
        {
            if (_connectionGroups.ContainsKey(Context.ConnectionId))
            {
                List<string> groups = _connectionGroups[Context.ConnectionId];

                foreach (var group in groups)
                {
                    await Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
                }

                _connectionGroups.Remove(Context.ConnectionId);
            }
            _iconsumeservice.RemoveConnection(Context.ConnectionId);
            await base.OnDisconnectedAsync(exception);
        }

        public async Task SendMessage(Dictionary<string,string> ParametersRequired , Confluent.Kafka.Partition partition)
        {
            await Clients.All.SendAsync("ReceiveMessage", ParametersRequired , partition);
        }

    }
}




