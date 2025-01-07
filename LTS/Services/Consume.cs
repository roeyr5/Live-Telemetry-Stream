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
            var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_kafkaSettings.Topic);

            Task.Run(async () =>
            {
                while (!source.Token.IsCancellationRequested)
                {
                    //List<string> requiredValues = requiredvvalues;
                    try
                    {
                        var consumeResult = consumer.Consume(source.Token);
                        string jsonMessage = consumeResult.Message.Value;
                        JObject jsonObject = JObject.Parse(jsonMessage);
                        Dictionary<string, string> values = new Dictionary<string, string>();

                        //Console.WriteLine("Received Message: " + jsonMessage);
                        foreach (string key in requiredValues) //requiredValues
                        {
                            Console.WriteLine(key);
                            if (jsonObject.ContainsKey(key))
                            {
                                string value = jsonObject[key].ToString();
                                values.Add(key, value);
                            }
                        }
                        await _hubContext.Clients.All.SendAsync("ReceiveMessage", values, consumeResult.Partition);
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Consume exception: {ex.Error.Reason}");
                    }

                }
            });

            return Task.FromResult(OperationResult.Success);
        }
        public void AddParameter(string parameter)
        {
            Console.WriteLine("added parameter : " + parameter);
            requiredValues.Add(parameter);
        }
        public void RemoveParameter(string parameter)
        {
            Console.WriteLine("removed parameter : " + parameter);
            requiredValues.Remove(parameter);
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

    }
}
