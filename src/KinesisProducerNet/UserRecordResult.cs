using System.Collections.Generic;
using KinesisProducerNet.Protobuf;

namespace KinesisProducerNet
{
    public class UserRecordResult
    {
        public List<Attempt> Attempts { get; }
        public string SequenceNumber { get; }
        public string ShardId { get; }
        public bool Successful { get; }

        public UserRecordResult(List<Attempt> attempts, string sequenceNumber, string shardId, bool successful)
        {
            this.Attempts = attempts;
            this.SequenceNumber = sequenceNumber;
            this.ShardId = shardId;
            this.Successful = successful;
        }

        public static UserRecordResult FromProtobufMessage(PutRecordResult r)
        {
            var attempts = new List<Attempt>();
            foreach (var attempt in r.Attempts)
            {
                attempts.Add(Attempt.FromProtobufMessage(attempt));
            }

            return new UserRecordResult(
                attempts,
                r.SequenceNumber,
                r.ShardId,
                r.Success);
        }
    }
}