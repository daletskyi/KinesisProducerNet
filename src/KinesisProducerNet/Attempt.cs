namespace KinesisProducerNet
{
    public class Attempt
    {
        public Attempt(int delay, int duration, string errorMessage, string errorCode, bool success)
        {
            Delay = delay;
            Duration = duration;
            ErrorMessage = errorMessage;
            ErrorCode = errorCode;
            Success = success;
        }

        public int Delay { get; }

        public int Duration { get; }

        public string ErrorMessage { get; }

        public string ErrorCode { get; }

        public bool Success { get; }

        public static Attempt FromProtobufMessage(Protobuf.Attempt a)
        {
            return new Attempt(
                (int) a.Delay,
                (int) a.Duration,
                a.ErrorMessage,
                a.ErrorCode,
                a.Success);
        }
    }
}