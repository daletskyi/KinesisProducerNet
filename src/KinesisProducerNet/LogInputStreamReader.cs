using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

namespace KinesisProducerNet
{
    public class LogInputStreamReader
    {

        private static readonly Regex LevelRegex = new Regex(@"(?<level>trace|debug|info|warn(?:ing)?|error|fatal)",
            RegexOptions.Multiline | RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private static readonly Dictionary<string, Action<ILogger, string>> Emitters =
            new Dictionary<string, Action<ILogger, string>>
            {
                {"trace", (log, message) => { log.LogTrace(message); }},
                {"debug", (log, message) => { log.LogDebug(message); }},
                {"info", (log, message) => { log.LogInformation(message); }},
                {"warn", (log, message) => { log.LogWarning(message); }},
                {"warning", (log, message) => { log.LogWarning(message); }},
                {"error", (log, message) => { log.LogError(message); }},
                {"fatal", (log, message) => { log.LogError(message); }}
            };

        private readonly string streamType;
        private readonly StreamReader reader;
        private readonly Action<ILogger, string> logFunction;
        private volatile bool running = true;
        private volatile bool shuttingDown = false;
        private bool isReadingRecord;
        private readonly LinkedList<string> messageData = new LinkedList<string>();
        private readonly ILogger logger;
        public LogInputStreamReader(
            StreamReader reader,
            string streamType,
            Action<ILogger, string> logFunction,
            LogLevel logLevel)
        {
            this.reader = reader;
            this.streamType = streamType;
            this.logFunction = logFunction;
            this.logger = Logging.CreateLogger<LogInputStreamReader>(logLevel);
        }

        public void Run()
        {
            while (this.running)
            {
                try
                {
                    var logLine = reader.ReadLine();
                    if (logLine == null)
                    {
                        continue;
                    }
                    if (logLine.StartsWith("++++"))
                    {
                        StartRead();
                    }
                    else if (logLine.StartsWith("----"))
                    {
                        FinishRead();
                    }
                    else if (isReadingRecord)
                    {
                        messageData.AddLast(logLine);
                    }
                    else
                    {
                        this.logFunction(this.logger, logLine);
                    }
                }
                catch (IOException ioex)
                {
                    if (this.shuttingDown)
                    {
                        //
                        // Since the Daemon calls destroy instead of letting the process exit normally
                        // the input streams coming from the process will end up truncated.
                        // When we know the process is shutting down we can report the exception as info
                        //
                        if (ioex.Message == null || !ioex.Message.Contains("Stream closed"))
                        {
                            //
                            // If the message is "Stream closed" we can safely ignore it. This is probably a bug
                            // with the UNIXProcess#ProcessPipeInputStream that it throws the exception. There
                            // is no other way to detect the other side of the request being closed.
                            //
                            this.logger.LogInformation($"Received IO Exception during shutdown.  This can happen, but should indicate "
                                     + "that the stream has been closed: {ioex.Message}");

                        }
                    }
                    else
                    {
                        this.logger.LogError("Caught IO Exception while reading log line", ioex);
                    }
                }
            }
            if (messageData.Count != 0)
            {
                logFunction(this.logger, this.MakeMessage());
            }
        }

        public void Shutdown()
        {
            this.running = false;
        }

        public void PrepareForShutdown()
        {
            this.shuttingDown = true;
        }

        private void StartRead()
        {
            this.isReadingRecord = true;
            if (this.messageData.Count != 0)
            {
                this.logger.LogWarning($"{streamType}: New log record started, but message data has existing data: {this.MakeMessage()}");
                messageData.Clear();
            }
        }

        private void FinishRead()
        {
            if (!this.isReadingRecord)
            {
                this.logger.LogWarning($"{streamType}: Terminator encountered, but wasn't reading record.");
            }

            this.isReadingRecord = false;
            if (this.messageData.Count != 0)
            {
                var message = this.MakeMessage();
                var loggingLevel = GetLevelOrDefault(message);
                loggingLevel.LoggingFunc(this.logger, string.IsNullOrEmpty(loggingLevel.Level) ? message : message.Replace(loggingLevel.Level + ":", ""));
            }
            else
            {
                this.logger.LogWarning($"{streamType}: Finished reading record, but didn't find any message data.");
            }

            this.messageData.Clear();
        }

        private LoggingLevel GetLevelOrDefault(string message)
        {
            var match = LevelRegex.Match(message);

            if (match.Success)
            {
                var level = match.Groups["level"].Value;
                var res = Emitters[level.ToLowerInvariant()];
                if (res != null) return new LoggingLevel { Level = level, LoggingFunc = res };
            }

            return new LoggingLevel { LoggingFunc = (log, msg) => { this.logger.LogWarning("!!Failed to extract level!! - " + msg); } };
        }

        private string MakeMessage()
        {
            return string.Join("\n", messageData);
        }

        private class LoggingLevel
        {
            public string Level { get; set; }

            public Action<ILogger, string> LoggingFunc { get; set; }
        }
    }
}