using System;
using Confluent.Kafka;
using System.Timers;
using Newtonsoft.Json;

namespace RTDDataPipeline
{
    public class DataProducer : IDisposable
    {
        // Kafka producer
        private readonly IProducer<string, string> _producer;
        private readonly string _server;
        private readonly string _topic;

        // System timer
        private System.Timers.Timer _timer;
        private readonly int _interval;

        // Random generator
        private readonly Random _rnd;
        private readonly string[] keys = { "k1", "k2" };

        public DataProducer(string server, string topic, int interval)
        {
            _server = server;
            _topic = topic;
            _interval = interval;

            // Intializations
            var config = new ProducerConfig { BootstrapServers = _server };
            _producer = new ProducerBuilder<string, string>(config)
                .SetKeySerializer(Serializers.Utf8)
                .Build();

            _timer = new Timer(_interval);
            _timer.Elapsed += OnElapsedTime;
            _timer.AutoReset = true;

            _rnd = new Random();
        }

        public void Start()
        {
            _timer.Enabled = true;
        }

        public void Dispose()
        {
            _timer.Dispose();
            _producer.Dispose();
        }

        private void OnElapsedTime(object sender, ElapsedEventArgs args)
        {
            var message = new Message<string, string>
            {
                Key = keys[_rnd.Next(0, keys.Length)],
                Value = JsonConvert.SerializeObject(new TimeValuePair(args.SignalTime, (float)_rnd.NextDouble()))
            };
            try
            {
                _producer.Produce(_topic, message);
            }
            catch (ProduceException<string, string> ex)
            {
                Console.WriteLine(ex.Message);
            };
        }
    }

    public struct TimeValuePair { 
        public TimeValuePair(DateTime time, float value)
        {
            Time = time;
            Value = value;
        }

        public DateTime Time { get; set; }
        public float Value { get; set; }
    }
    
}
