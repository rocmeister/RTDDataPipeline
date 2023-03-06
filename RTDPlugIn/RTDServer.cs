using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ExcelDna.Integration.Rtd;
using RTDDataPipeline;

namespace RTDPlugIn
{
    [ComVisible(true)]
    [ProgId(ServerProgId)]
    public class RTDServer : ExcelRtdServer
    {
        public const string ServerProgId = "RTDServerId";

        private HashSet<Topic> _topics;
        private DataConsumer _consumer;
        private CancellationTokenSource _cts = new CancellationTokenSource();
        
        // caches latest value by key
        internal static IDictionary<string, TimeValuePair> _cache = new Dictionary<string, TimeValuePair>();

        // could be loaded from config
        private const string _server = "localhost:9092";
        private const string _groupId = "hartree-group2";
        private const string _topic = "hartree-data";

        protected override bool ServerStart()
        {
            _topics = new HashSet<Topic>();
            _consumer = new DataConsumer(_server, _groupId, _topic);
            return true;
        }

        // When the workbook is closed and reopened, this RTD server gets created again, calls
        // ConnectData to initialize active topics, and updates with new values.
        protected override object ConnectData(Topic topic, IList<string> topicInfo, ref bool newValues)
        {
            // Upon first time connecting, start up the Kafka consumer thread to listen to topic
            if (_topics.Add(topic))
            {
                _consumer.NewData += (key, obj) =>
                {
                    if (!_cache.ContainsKey(key) || obj.Time > _cache[key].Time)
                    {
                        _cache[key] = obj;
                        topic.UpdateValue(key);
                    }
                };
                var t = new Thread(() => _consumer.Start(_cts.Token));
                t.Start();
            }
            return null;
        }

        protected override void DisconnectData(Topic topic)
        {
            _topics.Remove(topic);
            _cts.Cancel();
        }
    }
}
