using System;
using System.Numerics;
using System.Text;

namespace KinesisProducerNet.Sample
{
    public static class Utils
    {
        public static string RandomExplicitHashKey()
        {
            var rnd = Guid.NewGuid().ToByteArray();
            var b = BigInteger.Abs(new BigInteger(rnd));
            return b.ToString("D");
        }

        public static byte[] GenerateData(long sequenceNumber, int totalLength)
        {
            var sb = new StringBuilder();
            sb.Append(sequenceNumber);
            sb.Append(" ");
            while (sb.Length < totalLength)
            {
                sb.Append("a");
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return bytes;
        }
    }
}