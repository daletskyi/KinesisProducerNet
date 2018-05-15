using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace KinesisProducerNet.Sample.Lambda
{
    public class Function
    {
        public void FunctionHandler(ILambdaContext context)
        {
            var simpleTest = new SimplePutTest();
            simpleTest.Run();
        }
    }
}
