using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ExcelDna.Integration;
using RTDDataPipeline;
using System.Reactive.Disposables;
using ExcelDna.ComInterop;
using Microsoft.Office.Interop.Excel;

namespace RTDPlugIn
{
    public static class Function
    {
        // key, value pairs
        private static object[,] _storage = new object[1, 2] { { "key", "value" } };
        // row number of each key
        private static IDictionary<string, int> _dictionary = new Dictionary<string, int>();

        [ExcelFunction(Description = "Provides time value pairs")]
        public static object[,] RtdData()
        {
            var key = XlCall.RTD(RTDServer.ServerProgId, null, "") as string;
            if (key == null) return _storage;

            // update since number of keys can change dynamically
            var value = RTDServer._cache[key].Value;
            if (_dictionary.ContainsKey(key))
            {
                var rowNum = _dictionary[key];
                _storage[rowNum, 1] = value;
            }
            else
            {
                var rows = _storage.GetLength(0);
                _dictionary[key] = rows;

                object[,] tmp = new object[rows + 1, 2];
                for (int i = 0; i < rows; i++)
                {
                    for (int j = 0; j < 2; j++)
                    {
                        tmp[i, j] = _storage[i, j];
                    }
                }
                _storage = tmp;
                _storage[rows, 0] = key;
                _storage[rows, 1] = value;
            }
            return _storage;
        }


        /****** Alternative implementation using reactive extensions ******/
        [ExcelFunction(Description = "Provides time value pairs")]
        public static object[,] RtdData2()
        {
            ExcelAsyncUtil.Observe("RtdData2", new object[] { },
                new ExcelObservableSource(() => new KafkaObservable()));
            return _storage;
        }

        public class KafkaObservable : IExcelObservable
        {
            // could be loaded from config
            private const string _server = "localhost:9092";
            private const string _groupId = "hartree-group2";
            private const string _topic = "hartree-data";
            public IDisposable Subscribe(IExcelObserver observer)
            {
                var consumer = new DataConsumer(_server, _groupId, _topic);
                consumer.NewData += (key, obj) =>
                {
                    var value = obj.Value;
                    if (_dictionary.ContainsKey(key))
                    {
                        var rowNum = _dictionary[key];
                        _storage[rowNum, 1] = value;
                    }
                    else
                    {
                        var rows = _storage.GetLength(0);
                        _dictionary[key] = rows;

                        object[,] tmp = new object[rows + 1, 2];
                        for (int i = 0; i < rows; i++)
                        {
                            for (int j = 0; j < 2; j++)
                            {
                                tmp[i, j] = _storage[i, j];
                            }
                        }
                        _storage = tmp;
                        _storage[rows, 0] = key;
                        _storage[rows, 1] = value;
                    }
                    observer.OnNext("");
                };

                var cts = new CancellationTokenSource();
                Task.Run(() => consumer.Start(cts.Token));
                
                return Disposable.Create(() => {
                    cts.Cancel();
                    consumer.Dispose();
                });
            }
        }

        public class ExcelAddin : IExcelAddIn
        {
            public void AutoOpen()
            {
                ComServer.DllRegisterServer();
                var application = (Application)ExcelDnaUtil.Application;
                application.CalculateFullRebuild();
            }
            public void AutoClose()
            {
                ComServer.DllUnregisterServer();
            }
        }
    }
}
