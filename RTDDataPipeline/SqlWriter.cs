using System;
using Dapper;
using System.Data.SqlClient;

namespace RTDDataPipeline
{
    public class SqlWriter : IDisposable
    {
        private readonly SqlConnection _conn;
        private readonly string _connString;

        public SqlWriter(DataConsumer consumer, string connString)
        {
            consumer.NewData += OnNewData;
            _connString = connString;
            _conn = new SqlConnection(_connString);
        }

        private void OnNewData(string key, TimeValuePair obj)
        {
            var t = obj.Time;
            var v = obj.Value;
            _conn.Execute("INSERT INTO [TRADEDBRockyTest].[dbo].[TimeValuePairs] VALUES(@key, @t, @v)", new { key, t, v }); // Table can add an identity column in production

            Console.WriteLine($"key: {key}, time: {t}, value: {v}");
        }

        public void Dispose()
        {
            _conn.Dispose();
        }
    }
}
