using System;

namespace KinesisProducerNet
{
    public class UserRecordFailedException : Exception
    {
        public UserRecordResult Result { get; }

        public UserRecordFailedException(UserRecordResult result)
        {
            Result = result;
        }
    }
}