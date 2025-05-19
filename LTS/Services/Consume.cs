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
        private static Dictionary<string, Dictionary<string, string>> _uavsData;// <100fiberbox> - dict <height,100m>
        private static Dictionary<string, Dictionary<string, List<string>>> _connectionParameters; // <connectionID> - dict<100fiberboxup ,list<parametrsNames>>>
        private static bool isStartConsume = false;

        private readonly IHubContext<LTSHub> _hubContext;
        private List<int> _subscribedTailNumbers;
        private IConsumer<Ignore, string> _consumerBuilder;
        private ConsumerConfig _consumerConfig;

        private static readonly object uavDataLock = new();
        private static readonly object connectionParametersLock = new();

        private readonly string GROUP_MESSAGE = "ReceiveMessage";

        private Timer _timer;

        public Consume(IOptions<KafkaSettings> kafkaSettings, IHubContext<LTSHub> hubContext)
        {
            _hubContext = hubContext;
            _kafkaSettings = kafkaSettings.Value;

            _subscribedTailNumbers = new();
            _uavsData = new();
            _connectionParameters = new();

            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _kafkaSettings.BootstrapServers,
                GroupId = _kafkaSettings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            _consumerBuilder = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();
        }

        public OperationResult Subscribe(int topic)
        {
            if (_consumerBuilder == null)
                return OperationResult.Failed;

            if (!_subscribedTailNumbers.Contains(topic))
            {
                _subscribedTailNumbers.Add(topic);
            }
            _consumerBuilder.Subscribe(_subscribedTailNumbers.Select(t => t.ToString()).ToArray());


            Console.WriteLine("Subscribed to new topics with partitions 0-  3.");
            if (!isStartConsume)
                StartConsume();

            return OperationResult.Success;
        }
        public OperationResult StartConsume()
        {
            isStartConsume = true;
            Console.WriteLine("Starting stream data");

            Task.Run(async () =>
            {
                while (!source.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumerBuilder.Consume(source.Token);
                        string uavName = GetUAVName(consumeResult.Topic, consumeResult.Partition.Value);

                        if (!_connectionParameters.Values.Any(c => c.ContainsKey(uavName)))
                            continue;

                        string frameDataParameters = consumeResult.Message.Value;
                        JObject frameDataObject = JObject.Parse(frameDataParameters);

                        lock (uavDataLock)
                        {
                            if (!_uavsData.ContainsKey(uavName))
                                _uavsData[uavName] = new Dictionary<string, string>();

                            foreach (string parameter in _uavsData[uavName].Keys)
                            {
                                if (frameDataObject.ContainsKey(parameter))
                                {
                                    _uavsData[uavName][parameter] = frameDataObject[parameter].ToString();
                                }
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"error : {e.Error.Reason}");
                    }

                }
            });

            _timer = new Timer(_ => _ = SendDataToClientsAsync(), null, TimeSpan.Zero, TimeSpan.FromSeconds(1));

            return OperationResult.Success;
        }

        public void AddConnection(string connectionId)
        {
            lock (connectionParametersLock)
            {
                if (!_connectionParameters.ContainsKey(connectionId))
                    _connectionParameters[connectionId] = new Dictionary<string, List<string>>();
            }
        }
        public void AddParameter(string connectionId, string uavName, string parameter) // also to the uavsdata
        {

            lock (connectionParametersLock)
            {
                if (!_connectionParameters[connectionId].ContainsKey(uavName))
                    _connectionParameters[connectionId][uavName] = new List<string>();

                if (!_connectionParameters[connectionId][uavName].Contains(parameter))
                    _connectionParameters[connectionId][uavName].Add(parameter);
            }

            lock (uavDataLock)
            {
                if (!_uavsData.ContainsKey(uavName))
                    _uavsData[uavName] = new Dictionary<string, string>();

                if (!_uavsData[uavName].ContainsKey(parameter))
                    _uavsData[uavName][parameter] = string.Empty;
            }
        }

        public void RemoveParameter(string connectionId, string uavName, string parameter)
        {

            RemoveParameterFromConnection(connectionId, uavName, parameter);
            bool isRequired = IsParameterRequiredByAnyConnection(uavName, parameter);

            if (!isRequired)
                RemoveParameterFromUav(uavName, parameter);
        }

        private static void RemoveParameterFromConnection(string connectionId, string uavName, string parameter)
        {
            lock (connectionParametersLock)
            {
                if (_connectionParameters.ContainsKey(connectionId) && _connectionParameters[connectionId].ContainsKey(uavName))
                {
                    _connectionParameters[connectionId][uavName].Remove(parameter);

                    if (_connectionParameters[connectionId][uavName].Count == 0)
                        _connectionParameters[connectionId].Remove(uavName);

                    if (_connectionParameters[connectionId].Count == 0)
                        _connectionParameters.Remove(connectionId);
                }
            }
        }

        private bool IsParameterRequiredByAnyConnection(string uavName, string parameter)
        {
            lock (connectionParametersLock)
            {
                foreach (var connection in _connectionParameters)
                {
                    if (connection.Value.ContainsKey(uavName))
                    {
                        foreach (string connectionParameter in connection.Value[uavName])
                        {
                            if (connectionParameter == parameter)
                                return true;
                        }
                    }
                }
            }
            return false;
        }

        private static void RemoveParameterFromUav(string uavName, string parameter)
        {
            lock (uavDataLock)
            {
                if (_uavsData.ContainsKey(uavName))
                {
                    _uavsData[uavName].Remove(parameter);

                    if (_uavsData[uavName].Count == 0)
                        _uavsData.Remove(uavName);
                }
            }
        }


        public void RemoveConnection(string connectionId)
        {
            lock (connectionParametersLock)
            {
                if (!_connectionParameters.ContainsKey(connectionId))
                {
                    return;
                }

                foreach (KeyValuePair<string, List<string>> uavEntry in _connectionParameters[connectionId])
                {
                    string uavName = uavEntry.Key;
                    List<string> parameters = uavEntry.Value;

                    foreach (string parameter in parameters)
                    {
                        if (!IsParameterStillNeeded(uavName, parameter))
                            RemoveParameterFromUav(uavName, parameter);
                    }
                }

                _connectionParameters.Remove(connectionId);
            }
        }

        private bool IsParameterStillNeeded(string uavName, string parameter)
        {
            foreach (KeyValuePair<string, Dictionary<string, List<string>>> connection in _connectionParameters)
            {
                if (connection.Value.ContainsKey(uavName) && connection.Value[uavName].Contains(parameter))
                    return true;
            }
            return false;
        }

        private async Task SendDataToClientsAsync()
        {
            try
            {
                Dictionary<string, Dictionary<string, string>> uavsDataCopy;

                lock (uavDataLock)
                {
                    uavsDataCopy = _uavsData.ToDictionary(
                        entry => entry.Key,
                        entry => new Dictionary<string, string>(entry.Value)
                    );
                }

                foreach (var uavNumber in uavsDataCopy)
                {
                    string uavName = uavNumber.Key;
                    Dictionary<string, string> data = uavNumber.Value;

                    bool hasConnection;
                    lock (connectionParametersLock)
                        hasConnection = _connectionParameters.Values.Any(c => c.ContainsKey(uavName));

                    if (hasConnection)
                        await _hubContext.Clients.Group(uavName).SendAsync(GROUP_MESSAGE, data, uavName);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"error in group message : {ex.Message}");
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
        private static string GetUAVName(string topic, int partition)
        {
            switch (partition)
            {
                case (int)UAVPartition.FiberBox.Down:
                    return $"{topic}FiberBoxDown";
                case (int)UAVPartition.FiberBox.Up:
                    return $"{topic}FiberBoxUp";
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
