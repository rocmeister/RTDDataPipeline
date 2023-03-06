using System;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace RTDDataPipeline
{
    public class DataConsumer : IDisposable
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly string _server;
        private readonly string _groupId;
        private readonly string _topic;

        public event Action<string, TimeValuePair> NewData;

        public DataConsumer(string server, string groupId, string topic)
        {
            _server = server;
            _groupId = groupId;
            _topic = topic;

            var config = new ConsumerConfig
            {
                BootstrapServers = _server,
                AutoOffsetReset = AutoOffsetReset.Latest,
                GroupId = _groupId
            };
            _consumer = new ConsumerBuilder<string, string>(config)
                .SetKeyDeserializer(Deserializers.Utf8)
                .Build();
            _consumer.Subscribe(_topic);
        }

        public void Start(CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var message = _consumer.Consume();
                    if (message.Message == null) continue;
                    var key = message.Message.Key;
                    var obj = JsonConvert.DeserializeObject<TimeValuePair>(message.Message.Value);

                    NewData?.Invoke(key, obj);
                }
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                _consumer.Close();
            }
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }
    }
}
