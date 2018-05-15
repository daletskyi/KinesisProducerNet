using System;
using System.Text;

namespace KinesisProducerNet.Sample
{
    public class SimplePutTest
    {
        public void Run()
        {
            var data = "{ \"Id\": 1, \"Data\": \"data\" }";
            var bytes = Encoding.UTF8.GetBytes(data);
            var userRecord = new UserRecord("test", "1", bytes);

            var config = new KinesisProducerConfiguration
            {
                Region = "eu-west-1",
                MaxConnections = 1,
            };

            IKinesisProducer kinesisProducer = new KinesisProducer(config);
            kinesisProducer.AddUserRecord(userRecord);
            kinesisProducer.FlushSync();

            var metrics = kinesisProducer.GetMetrics("UserRecordsPut", 10);
            foreach (var metric in metrics)
            {
                if (metric.Dimensions.Count == 1 && metric.SampleCount > 0)
                {
                    Console.WriteLine(metric.ToString());
                }
            }

            kinesisProducer.Destroy();
        }
    }
}