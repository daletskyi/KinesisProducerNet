using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KinesisProducerNet.Sample
{
    public class ExtendedProducerTest
    {
        private const int DATA_SIZE = 128;
        private static readonly string TIMESTAMP = Stopwatch.GetTimestamp().ToString();
        private const int SECONDS_TO_RUN = 5;
        private const int RECORDS_PER_SECOND = 2000;
        private const string STREAM_NAME = "test";
        private const string REGION = "eu-west-1";

        public void Run()
        {
            var producer = this.GetKinesisProducer();
            long sequenceNumber = 0;
            long completed = 0;

            Action<Task<UserRecordResult>> callback = task =>
            {
                if (task.Status == TaskStatus.Faulted)
                {
                    var exception = task.Exception.InnerExceptions.First();
                    Console.WriteLine(exception.Message);
                    throw new Exception(string.Empty, exception);
                }

                Interlocked.Increment(ref completed);
            };

            Action putOneRecord = () =>
            {
                var data = Utils.GenerateData(Interlocked.Read(ref sequenceNumber), DATA_SIZE);
                var hash = Utils.RandomExplicitHashKey();
                var tcs = producer.AddUserRecord(STREAM_NAME, TIMESTAMP, hash, data);
                tcs.Task.ContinueWith(callback);
            };

            var cts = new CancellationTokenSource();

            var progress = Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    long put = Interlocked.Read(ref sequenceNumber);
                    long total = SECONDS_TO_RUN * RECORDS_PER_SECOND;
                    double putPercent = 100.0 * put / total;
                    long done = Interlocked.Read(ref completed);
                    double donePercent = 100.0 * done / total;
                    Console.WriteLine($"Put {put} of {total} so far ({putPercent}%) {done} have completed ({donePercent}%)");

                    Thread.Sleep(1000);
                }
            }, cts.Token);

            Console.WriteLine($"Starting puts... will run for {SECONDS_TO_RUN} seconds at {RECORDS_PER_SECOND} records per second");
            var startTime = Stopwatch.GetTimestamp();

            var puts = Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    double secondsRun = (Stopwatch.GetTimestamp() - startTime) / (double) Stopwatch.Frequency;
                    double targetCount = Math.Min(SECONDS_TO_RUN, secondsRun) * RECORDS_PER_SECOND;

                    while (Interlocked.Read(ref sequenceNumber) < targetCount)
                    {
                        Interlocked.Increment(ref sequenceNumber);
                        try
                        {
                            putOneRecord();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                            throw;
                        }
                    }

                    if (secondsRun >= SECONDS_TO_RUN)
                    {
                        break;
                    }

                    Thread.Sleep(1);
                }
            }, cts.Token);

            Console.WriteLine("Waiting for puts...");

            Task.WaitAll(new[] { puts }, TimeSpan.FromSeconds(SECONDS_TO_RUN + 1));

            Console.WriteLine("Waiting for remaining puts to finish...");
            producer.FlushSync();
            Console.WriteLine("All records complete.");

            producer.Destroy();
            cts.Cancel();
            Console.WriteLine("Finished.");
        }

        private KinesisProducer GetKinesisProducer()
        {
            var config = new KinesisProducerConfiguration
            {
                Region = REGION,
                MaxConnections = 2,
                RequestTimeout = 60000L,
            };

            var producer = new KinesisProducer(config);
            return producer;
        }
    }
}