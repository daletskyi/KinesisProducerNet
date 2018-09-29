using Amazon.Kinesis.ClientLibrary;
using System;

namespace KinesisProducerNet.Sample.KCL.Consumer
{
    // you don't need to run consumer by your own, instead run producer only
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                KclProcess.Create(new SampleRecordProcessor()).Run();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("ERROR: " + e);
            }
        }
    }
}
