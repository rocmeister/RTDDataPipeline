using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Timer = System.Timers.Timer;
using System.Configuration;

namespace RTDDataPipeline
{
    internal class Program
    {
        static void Main(string[] args)
        {
            /* For the sake of demo simplicity, both data producer and sql consumer
             * are started from this program entry point, running on separate threads. 
             * In reality these are two independent applications running on different app server instances.
             */

            // config
            var appSettings = ConfigurationManager.AppSettings;
            var server = appSettings["KafkaServer"];
            var groupId = appSettings["KafkaGroupId"];
            var topic = appSettings["KafkaTopic"];
            var interval = int.Parse(appSettings["Interval"]);
            var connString = ConfigurationManager.ConnectionStrings["testdb"].ConnectionString;

            // run the demo for 10 seconds
            using var cts = new CancellationTokenSource();
            using var timer = new Timer(10_000) { AutoReset = false, Enabled = true };
            timer.Elapsed += (s, e) => { cts.Cancel(); };

            using var publisher = new DataProducer(server, topic, interval);
            using var consumer = new DataConsumer(server, groupId, topic);
            using var sqlWriter = new SqlWriter(consumer, connString);

            Task.Run(() => publisher.Start()); // not entirely necessary as publisher._timer already runs on a background thread by default
            consumer.Start(cts.Token);
        }
    }
}
