using System.Collections.Generic;
using System.Threading.Tasks;

namespace KinesisProducerNet
{
    public interface IKinesisProducer
    {
        TaskCompletionSource<UserRecordResult> AddUserRecord(string stream, string partitionKey, byte[] data);

        TaskCompletionSource<UserRecordResult> AddUserRecord(string stream, string partitionKey, string explicitHashKey, byte[] data);

        TaskCompletionSource<UserRecordResult> AddUserRecord(UserRecord userRecord);

        List<Metric> GetMetrics(string metricName, int windowSeconds);

        List<Metric> GetMetrics(string metricName);

        List<Metric> GetMetrics();

        List<Metric> GetMetrics(int windowSeconds);

        void Destroy();

        void Flush();

        void Flush(string stream);

        void FlushSync();

        int GetOutstandingRecordsCount();
    }
}