using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LTS.Entities;

namespace LTS.Services
{
    public interface IConsume
    {
        public Task<OperationResult> StartConsume();
        public void StopConsume();
        public void AddParameter(string connectionId,string uavName , string parameter);
        public void RemoveParameter(string connectionId , string uavName ,string parameter);
        public void Start();
        public void RemoveConnection(string id);

    }
}

//            _iconsumeservice.AddParameter(Context.ConnectionId, uavName,parameter);

