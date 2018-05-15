﻿using Microsoft.Extensions.Logging;

namespace KinesisProducerNet
{
    public static class Logging
    {
        public static ILoggerFactory LoggerFactory { get; } = new LoggerFactory().AddConsole(LogLevel.Debug);
        public static ILogger CreateLogger<T>() => LoggerFactory.CreateLogger<T>();
    }
}