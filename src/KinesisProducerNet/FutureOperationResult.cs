using System.Collections.Generic;
using System.Threading.Tasks;

namespace KinesisProducerNet
{
    public class FutureOperationResult : Either<TaskCompletionSource<UserRecordResult>, TaskCompletionSource<List<Metric>>>
    {
        public FutureOperationResult(TaskCompletionSource<UserRecordResult> left) : base(left)
        {
        }

        public FutureOperationResult(TaskCompletionSource<List<Metric>> right) : base(right)
        {
        }
    }
}