using Amazon.Kinesis.ClientLibrary;
using System;
using System.Text;

namespace KinesisProducerNet.Sample.KCL.Consumer
{
    internal class SampleRecordProcessor : IRecordProcessor
    {
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);

        private string shardId;
        private DateTime nextCheckpoint = DateTime.UtcNow;

        public void Initialize(InitializationInput input)
        {
            Console.Error.WriteLine($"Initializing record processor for shard: {input.ShardId}");

            this.shardId = input.ShardId;
        }

        public void ProcessRecords(ProcessRecordsInput input)
        {
            Console.Error.WriteLine($"Processing {input.Records.Count} records from {this.shardId}");

            foreach (var record in input.Records)
            {
                var data = Encoding.UTF8.GetString(record.Data);
                Console.Error.WriteLine($"Got record data: {data}");
            }

            if (DateTime.UtcNow >= this.nextCheckpoint)
            {
                input.Checkpointer.Checkpoint();
                this.nextCheckpoint = DateTime.UtcNow + CheckpointInterval;
            }
        }

        public void Shutdown(ShutdownInput input)
        {
            Console.Error.WriteLine($"Shutting down record processor for shard: {this.shardId}");

            if (input.Reason == ShutdownReason.TERMINATE)
            {
                input.Checkpointer.Checkpoint();
            }
        }
    }
}
