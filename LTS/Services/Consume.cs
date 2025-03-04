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
    public class Consume : IConsume
    {
        private static CancellationTokenSource source = new();
        private readonly KafkaSettings _kafkaSettings;
        //private static Frame
        //private static Dictionary<string, Frame[]> uavsData = new Dictionary<string,Frame[]>(); // 100fiberbox - dict <height,100m>
        private static Dictionary<string, List<FrameData>> uavNumComnToParams = new Dictionary<string, List<FrameData>>(); // key: uav number and comn (example: 100fiberbox), value: all the params names and values
        private static Dictionary<string, Dictionary<string, List<string>>> connectionParameters = new Dictionary<string, Dictionary<string, List<string>>>(); // connectionID - dict<100fiberboxup ,list<height,veolicty>>>
        private static bool isStartConsume = false;

        private readonly IHubContext<LTSHub> _hubContext;
        private List<int> subscribedTopics = new List<int>();
        private IConsumer<Ignore, string> consumer;
        private ConsumerConfig config;

        private static readonly object uavDataLock = new object();
        private static readonly object connectionParametersLock = new object();

        public Consume(IOptions<KafkaSettings> kafkaSettings, IHubContext<LTSHub> hubContext)
        {
            _hubContext = hubContext;
            _kafkaSettings = kafkaSettings.Value;

            config = new ConsumerConfig
            {
                BootstrapServers = _kafkaSettings.BootstrapServers,
                GroupId = _kafkaSettings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        public Task<OperationResult> Subscribe(int topic)
        {
            if (consumer == null)
                return Task.FromResult(OperationResult.Failed);

            if (!subscribedTopics.Contains(topic))
            {
                subscribedTopics.Add(topic);
            }

            List<TopicPartition> topicPartitions = new List<TopicPartition>();

            foreach (int currentTopic in subscribedTopics)
            {
                topicPartitions.Add(new TopicPartition(currentTopic.ToString(), new Partition(0)));
                topicPartitions.Add(new TopicPartition(currentTopic.ToString(), new Partition(1)));
                topicPartitions.Add(new TopicPartition(currentTopic.ToString(), new Partition(2)));
                topicPartitions.Add(new TopicPartition(currentTopic.ToString(), new Partition(3)));
            }

            consumer.Assign(topicPartitions);
            Console.WriteLine("Subscribed to new topics with partitions 0, 1, 2, and 3.");
            if (!isStartConsume)
                StartConsume();

            return Task.FromResult(OperationResult.Success);
        }
        public Task<OperationResult> StartConsume()
        {
            isStartConsume = true;
            Console.WriteLine("Starting stream data");

            Task.Run(async () =>
            {
                while (!source.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(source.Token);

                        string uavName = GetUAVName(consumeResult.Topic, consumeResult.Partition.Value);
                        if (!connectionParameters.Values.Any(c => c.ContainsKey(uavName)))
                            continue
                            ;
                        //add if
                        string jsonMessage = consumeResult.Message.Value;
                        JObject jsonObject = JObject.Parse(jsonMessage);

                        lock (uavDataLock)
                        {
                            //if (!uavsData.ContainsKey(uavName))
                            //{
                            //    uavsData[uavName] = new Dictionary<string, string>();
                            //}
                            Console.WriteLine("consuming from  : " + uavName);
                            foreach (FrameData frameData in uavNumComnToParams[uavName])
                            {
                                if (jsonObject.ContainsKey(frameData.parameterName))
                                {
                                    frameData.parameterValue = jsonObject[frameData.parameterName].ToString();
                                }
                            }
                            //foreach (var key in uavsData[uavName].Keys)
                            //{
                            //    if (jsonObject.ContainsKey(key))
                            //    {
                            //        uavsData[uavName][key] = jsonObject[key].ToString();
                            //    }
                            //}
                        }

                        //Console.WriteLine($"Updated Data for {uavName}:");
                        //foreach (var item in uavsData[uavName])
                        //{
                        //    Console.WriteLine($"{item.Key}: {item.Value}");
                        //}

                        await _hubContext.Clients.Group(uavName).SendAsync("ReceiveMessage", uavNumComnToParams[uavName], uavName);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"error : {e.Error.Reason}");
                    }

                }
            });

            return Task.FromResult(OperationResult.Success);
        }
        public void AddParameter(string connectionId, string uavName, string parameter) // also to the uavsdata
        {
            Console.WriteLine("added : " + parameter + " for uav: " + uavName);

            lock (connectionParametersLock)
            {
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
            }

            lock (uavDataLock)
            {
                if (!uavNumComnToParams.ContainsKey(uavName))
                {
                    uavNumComnToParams[uavName] = new List<FrameData>();
                }

                if (uavNumComnToParams[uavName].FirstOrDefault(frame => frame.parameterName == parameter) == null)
                {
                    FrameData frameData = new FrameData(parameter, "");
                    uavNumComnToParams[uavName].Add(frameData);
                }
                //if (!uavsData.ContainsKey(uavName)) //uav name = 100 + mission => 100MissionUp
                //{
                //    uavsData[uavName] = new Dictionary<string, string>();
                //}

                    //if (!uavsData[uavName].ContainsKey(parameter))
                    //{
                    //    uavsData[uavName][parameter] = "";
                    //}
            }
        }

        public void RemoveParameter(string connectionId, string uavName, string parameter)
        {
            lock (connectionParametersLock)
            {
                if (connectionParameters.ContainsKey(connectionId) && connectionParameters[connectionId].ContainsKey(uavName))
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

            if (!parameterRequired)
            {
                lock (uavDataLock)
                {
                    FrameData frameDataToRemove = uavNumComnToParams[uavName]?.FirstOrDefault(frame => frame.parameterName == parameter);
                    if (frameDataToRemove != null)
                    {
                        uavNumComnToParams[uavName].Remove(frameDataToRemove);
                    }
                }
            }
        }

        public void RemoveConnection(string connectionId)
        {
            lock (connectionParametersLock)
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

                            if (!parameterRequired)
                            {
                                lock (uavDataLock)
                                {
                                    FrameData frameDataToRemove = uavNumComnToParams[uavName].FirstOrDefault(frame => frame.parameterName == parameter);
                                    if (frameDataToRemove != null)
                                    {
                                        uavNumComnToParams[uavName].Remove(frameDataToRemove);
                                    }
                                }
                            }
                        }
                    }
                    connectionParameters.Remove(connectionId);
                }
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

