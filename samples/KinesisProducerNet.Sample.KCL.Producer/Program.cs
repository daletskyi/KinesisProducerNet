using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace KinesisProducerNet.Sample.KCL.Producer
{
    // make sure you run sample with working directory equal to "../KinesisProducerNet.Sample.KCL.Consumer"
    // you'll need java installed to be able to run KCL consumer
    class Program
    {
        const string streamName = "test";
        const string region = "eu-west-1";

        static void Main(string[] args)
        {
            RunProducer();
            RunConsumer();
        }

        static void RunProducer()
        {
            var config = new KinesisProducerConfiguration
            {
                Region = region,
                MaxConnections = 1,
            };

            var kinesisProducer = new KinesisProducer(config);

            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine($"Writing record number {i}");

                var data = $"Record {i}";
                var bytes = Encoding.UTF8.GetBytes(data);
                var userRecord = new UserRecord(streamName, "partitionKey", bytes);

                kinesisProducer.AddUserRecord(userRecord);
            }

            kinesisProducer.FlushSync();
            kinesisProducer.Destroy();
        }

        static void RunConsumer()
        {
            var jarFolder = Path.Combine(Directory.GetCurrentDirectory(), "jars");
            var files = Directory.GetFiles(jarFolder).ToList();
            files.Add(Directory.GetCurrentDirectory());
            var javaClassPath = string.Join(Path.PathSeparator.ToString(), files);

            var cmd = new List<string>()
            {
                "-cp",
                javaClassPath,
                "com.amazonaws.services.kinesis.multilang.MultiLangDaemon",
                "kcl.properties"
            };

            var proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "java",
                    Arguments = string.Join(" ", cmd),
                    UseShellExecute = false
                }
            };

            proc.Start();
            proc.WaitForExit();
        }
    }
}
