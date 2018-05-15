namespace KinesisProducerNet
{
    public class UserRecord
    {
        public string StreamName { get; set; }

        public string PartitionKey { get; set; }

        public string ExplicitHashKey { get; set; }

        public byte[] Data { get; set; }

        public UserRecord()
        {
        }

        public UserRecord(string streamName, string partitionKey, byte[] data)
        {
            this.StreamName = streamName;
            this.PartitionKey = partitionKey;
            this.Data = data;
        }

        public UserRecord(string streamName, string partitionKey, string explicitHashKey, byte[] data)
        {
            this.StreamName = streamName;
            this.PartitionKey = partitionKey;
            this.ExplicitHashKey = explicitHashKey;
            this.Data = data;
        }
    }
}