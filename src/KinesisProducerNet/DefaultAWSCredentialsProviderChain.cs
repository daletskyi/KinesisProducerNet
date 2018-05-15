using Amazon.Runtime;

namespace KinesisProducerNet
{
    internal class DefaultAWSCredentialsProviderChain : AWSCredentials
    {
        public override ImmutableCredentials GetCredentials()
        {
            return FallbackCredentialsFactory.GetCredentials().GetCredentials();
        }
    }
}