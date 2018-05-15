using System;

namespace KinesisProducerNet
{
    public class DaemonException : Exception
    {
        public DaemonException(string message)
            : base(message)
        {
        }
    }
}