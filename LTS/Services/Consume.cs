using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using LTS.Entities;
using LTS.Hubs;
using Microsoft.AspNetCore.SignalR;

namespace LTS.Services
{
    public class Consume :IConsume
    {
        private static CancellationTokenSource source = new();
        private readonly KafkaSettings _kafkaSettings;
        private static Dictionary<string, Dictionary<string, string>> uavsData = new Dictionary<string, Dictionary<string, string>>(); // 100fiberbox - dict <height,100m>
        private static Dictionary<string, Dictionary<string, List<string>>> connectionParameters = new Dictionary<string, Dictionary<string, List<string>>>(); // connectionID - dict<100fiberboxup ,list<height,veolicty>>>

        protected static List<string> requiredValues = new List<string> {}; 
        private readonly IHubContext<LTSHub> _hubContext;


        public Consume(IOptions<KafkaSettings> kafkaSettings, IHubContext<LTSHub> hubContext)
        {
            _hubContext = hubContext;
            _kafkaSettings = kafkaSettings.Value;
        }
        public Task<OperationResult> StartConsume()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _kafkaSettings.BootstrapServers,
                GroupId = _kafkaSettings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            List<string> topics = new List<string> { "100", "200", "240" };


            var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(topics);

            Task.Run(async () =>
            {
                while (!source.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(source.Token);
                        string uavName = GetUAVName(consumeResult.Topic, consumeResult.Partition.Value);

                        string jsonMessage = consumeResult.Message.Value;
                        JObject jsonObject = JObject.Parse(jsonMessage);

                        if (!uavsData.ContainsKey(uavName))
                        {
                            uavsData[uavName] = new Dictionary<string, string>();
                        }
                        Console.WriteLine("consuming from  : "+ uavName);
                        foreach (var key in uavsData[uavName].Keys) 
                        {
                            if (jsonObject.ContainsKey(key))
                            {
                                uavsData[uavName][key] = jsonObject[key].ToString();
                            }
                        }

                        Console.WriteLine($"Updated Data for {uavName}:");
                        foreach (var item in uavsData[uavName])
                        {
                            Console.WriteLine($"{item.Key}: {item.Value}");
                        }

                        await _hubContext.Clients.Group(uavName).SendAsync("ReceiveMessage", uavsData[uavName], uavName);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"error : {e.Error.Reason}");
                    }

                }
            });

            return Task.FromResult(OperationResult.Success);
        }
        public void AddParameter(string connectionId, string uavName, string parameter) // //also to the uavsdata
        {
            Console.WriteLine("added : " +parameter);
            if (!connectionParameters.ContainsKey(connectionId))
            {
                connectionParameters[connectionId] = new Dictionary<string, List<string>>();
            }

            if (!connectionParameters[connectionId].ContainsKey(uavName))
            {
                connectionParameters[connectionId][uavName] = new List<string>();
            }

            if (!connectionParameters[connectionId][uavName].Contains(parameter))
            {
                connectionParameters[connectionId][uavName].Add(parameter);
            }

            if (!uavsData.ContainsKey(uavName)) //uav name = 100 + mission => 100MissionUp
            {
                uavsData[uavName] = new Dictionary<string, string>();
            }

            if (!uavsData[uavName].ContainsKey(parameter))
            {
                uavsData[uavName][parameter] = ""; 
            }

        }
        public void RemoveParameter(string connectionId, string uavName, string parameter) 
        {
            if (connectionParameters.ContainsKey(connectionId) &&
                connectionParameters[connectionId].ContainsKey(uavName))
            {
                connectionParameters[connectionId][uavName].Remove(parameter);

                if (connectionParameters[connectionId][uavName].Count == 0)
                {
                    connectionParameters[connectionId].Remove(uavName);
                }

                if (connectionParameters[connectionId].Count == 0)
                {
                    connectionParameters.Remove(connectionId);
                }
            }

            bool parameterRequired = false;
            foreach (var connection in connectionParameters)
            {
                if (connection.Value.ContainsKey(uavName) && connection.Value[uavName].Contains(parameter))
                {
                    parameterRequired = true;
                    break;
                }
            }

            if (!parameterRequired && uavsData.ContainsKey(uavName))
            {
                uavsData[uavName].Remove(parameter);

                if (uavsData[uavName].Count == 0)
                {
                    uavsData.Remove(uavName);
                }
            }
        }
        public void RemoveConnection(string connectionId)
        {
            if (connectionParameters.ContainsKey(connectionId))
            {
                foreach (var uavEntry in connectionParameters[connectionId])
                {
                    string uavName = uavEntry.Key;
                    List<string> parameters = uavEntry.Value;

                    foreach (var parameter in parameters)
                    {
                        bool parameterRequired = false;

                        foreach (var connection in connectionParameters)
                        {
                            if (connection.Key != connectionId && connection.Value.ContainsKey(uavName) && connection.Value[uavName].Contains(parameter))
                            {
                                parameterRequired = true;
                                break;
                            }
                        }

                        if (!parameterRequired && uavsData.ContainsKey(uavName))
                        {
                            uavsData[uavName].Remove(parameter);
                            if (uavsData[uavName].Count == 0)
                            {
                                uavsData.Remove(uavName);
                            }
                        }
                    }
                }
                connectionParameters.Remove(connectionId);
            }
        }

        public void StopConsume()
        {
            source.Cancel();
        }
        public void Start()
        {
            if (source.Token.IsCancellationRequested)
            {
                source = new();
            }
        }
        private string GetUAVName(string topic, int partition)
        {
            switch (partition)
            {
                case (int)UAVPartition.FiberBox.Down:
                    return $"{topic}FBDown";
                case (int)UAVPartition.FiberBox.Up:
                    return $"{topic}FBUp";
                case (int)UAVPartition.Mission.Down:
                    return $"{topic}MissionDown";
                case (int)UAVPartition.Mission.Up:
                    return $"{topic}MissionUp";
                default:
                    throw new ArgumentOutOfRangeException(nameof(partition), "Invalid partition value");
            }
        }


    }
}

