# KinesisProducerNet

KinesisProducerNet is a .NET port of [Amazon Kinesis Producer Library] targeting .NET Standard 1.3+ and can be run on Windows, Linux and macOs.

## Installation
You can install KinesisProducerNet via NuGet:
```Install-Package KinesisProducerNet```
Apart of the main package you'll need to install package for each platform you want it to be run on:
```Install-Package KinesisProducerNet.Linux```

## Documentation
Please refer to the original KPL [documentation].

## Sample code
A sample project is available in `samples/KinesisProducerNet.Sample`.

## Building
There aren't any additional requirements for build process. You can either use MS Visual Studio or dotnet CLI.

## Todos
 - Add "Getting started" section here
 - Add sample with KCL ([waiting for release])
 - Update code to match the latest release of KPL (now it matches 0.12.8 release)

## License
MIT


   [Amazon Kinesis Producer Library]: <https://github.com/awslabs/amazon-kinesis-producer>
   [documentation]: <https://github.com/awslabs/amazon-kinesis-producer>
   [waiting for release]: <https://github.com/awslabs/amazon-kinesis-client-net/issues/8>
