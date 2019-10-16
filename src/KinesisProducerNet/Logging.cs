using Microsoft.Extensions.Logging;

namespace KinesisProducerNet
{
    public static class Logging
    {
        public static ILoggerFactory LoggerFactory(LogLevel logLevel) => new LoggerFactory().AddConsole(logLevel);
        public static ILogger CreateLogger<T>(LogLevel loglevel) => LoggerFactory(loglevel).CreateLogger<T>();
    }
}