using System;
using System.IO;

namespace KinesisProducerNet
{
    public class StreamBuffer
    {
        private readonly Stream ioStream;

        public StreamBuffer(Stream ioStream)
        {
            this.ioStream = ioStream;
        }

        public byte[] ReadBuffer()
        {
            var len = 0;
            for (var i = 0; i < 4; i++)
            {
                len = len * 256 + ioStream.ReadByte();
            }

            var inBuffer = new byte[len];
            ioStream.Read(inBuffer, 0, len);
            return inBuffer;
        }

        public int WriteBuffer(byte[] outBuffer)
        {
            var len = outBuffer.Length;
            var lenBytes = BitConverter.GetBytes(len);
            for (int i = 3; i >= 0; i--)
            {
                ioStream.WriteByte(lenBytes[i]);
            }

            ioStream.Write(outBuffer, 0, len);
            ioStream.Flush();

            return outBuffer.Length + 4;
        }
    }
}