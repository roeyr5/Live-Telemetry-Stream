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
        public void AddParameter(string parameter);
        public void RemoveParameter(string parameter);
        public void Start();
    }
}
