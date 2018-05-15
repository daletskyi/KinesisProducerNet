using System;

namespace KinesisProducerNet
{
    public class IrrecoverableError : Exception
    {
        public IrrecoverableError(string message)
            : base(message)
        {
        }

        public IrrecoverableError(string message, Exception t)
            : base(message, t)
        {
        }
    }
}