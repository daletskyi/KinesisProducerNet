using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using KinesisProducerNet.Protobuf;
using Microsoft.Extensions.Logging;
using ProtoBuf;

namespace KinesisProducerNet
{
    internal class DaemonUnix : Daemon
    {
        private FileStream inFileStream;
        private FileStream outFileStream;

        public DaemonUnix(string pathToExecutable, string workingDir, Action<Message> onMessage, Action<Exception> onException, KinesisProducerConfiguration config, IDictionary<string, string> environmentVariables)
            : base(pathToExecutable, workingDir, onMessage, onException, config, environmentVariables)
        {
        }

        protected override string InPipeAbsolutePath => Path.Combine(Path.GetFullPath(this.workingDir), this.InPipe);
        protected override string OutPipeAbsolutePath => Path.Combine(Path.GetFullPath(this.workingDir), this.OutPipe);

        protected override void CreatePipes()
        {
            this.logger.LogDebug("Creating pipes..");

            if (!Directory.Exists(this.workingDir))
            {
                Directory.CreateDirectory(this.workingDir);
            }

            do
            {
                InPipe = InPipePrefix + Guid8Chars();
            } while (File.Exists(InPipeAbsolutePath));

            do
            {
                OutPipe = OutPipePrefix + Guid8Chars();
            } while (File.Exists(OutPipeAbsolutePath));

            this.logger.LogDebug($"InPipeAbsolutePath = {InPipeAbsolutePath}");
            this.logger.LogDebug($"OutPipeAbsolutePath = {OutPipeAbsolutePath}");

            try
            {
                var cmd = $"mkfifo -m=0666 {InPipeAbsolutePath} {OutPipeAbsolutePath}";
                this.logger.LogDebug($"[BASH] {cmd}");
                var mkFifoResult = BashHelper.Run(cmd);
                this.logger.LogInformation($"[BASH] {cmd} result: {mkFifoResult}");
            }
            catch (Exception e)
            {
                this.FatalError("Error creating pipes", e, false);
            }

            var time = Stopwatch.GetTimestamp();
            while (!File.Exists(InPipeAbsolutePath) || !File.Exists(OutPipeAbsolutePath))
            {
                try
                {
                    Thread.Sleep(10);
                }
                catch (Exception)
                {
                }

                if ((Stopwatch.GetTimestamp() - time) / Stopwatch.Frequency > 15)
                {
                    this.FatalError("Pipes did not show up after calling mkfifo", false);
                }
            }

            this.logger.LogDebug("Pipes created successfully.");
        }

        protected override void ConnectToChild()
        {
            this.logger.LogDebug("CONNECTING TO I/O PIPES..");
            this.logger.LogDebug("IN PIPE = " + InPipeAbsolutePath);
            this.logger.LogDebug("OUT PIPE = " + OutPipeAbsolutePath);

            var start = DateTime.UtcNow;
            while (true)
            {
                try
                {
                    this.inFileStream = new FileStream(InPipeAbsolutePath, FileMode.Open, FileAccess.Read,
                        FileShare.ReadWrite, 8192, FileOptions.Asynchronous);
                    this.outFileStream = new FileStream(OutPipeAbsolutePath, FileMode.Open, FileAccess.Write,
                        FileShare.ReadWrite, 8192, FileOptions.Asynchronous);

                    this.logger.LogDebug("CONNECTED TO I/O PIPES!");
                    break;
                }
                catch (IOException e)
                {
                    this.logger.LogError("Failed to connect to child process", e);
                    this.inFileStream?.Dispose();
                    this.outFileStream?.Dispose();

                    try
                    {
                        Thread.Sleep(100);
                    }
                    catch (Exception)
                    {
                    }

                    if (DateTime.Now > start.AddSeconds(2))
                    {
                        throw;
                    }
                }
                catch (Exception e)
                {
                    this.logger.LogError("EXCEPTION WHILE CONNECTING TO I/O PIPES..", e);
                    this.logger.LogError(e.StackTrace);
                    break;
                }
            }
        }

        protected override void SendMessage()
        {
            try
            {
                var message = this.outgoingMessages.Take(this.processCancellationToken);

                if (this.outStreamBuffer == null)
                {
                    this.outStreamBuffer = new StreamBuffer(this.outFileStream);
                }

                var data = MessageToByteArray(message);
                this.outStreamBuffer.WriteBuffer(data);
            }
            catch (IOException e)
            {
                this.FatalError("Error writing message to daemon", e);
            }
            catch (Exception e)
            {
                this.FatalError(e.Message, e);
            }
        }

        protected override void ReceiveMessage()
        {
            try
            {
                if (this.inStreamBuffer == null)
                {
                    this.inStreamBuffer = new StreamBuffer(this.inFileStream);
                }

                receiveBuffer = this.inStreamBuffer.ReadBuffer();
                using (var ms = new MemoryStream(receiveBuffer))
                {
                    var msg = Serializer.Deserialize<Message>(ms);
                    this.incomingMessages.Add(msg);
                }
            }
            catch (IOException e)
            {
                FatalError("Error reading message from daemon", e);
            }
            catch (Exception e)
            {
                FatalError(e.Message, e);
            }
        }

        protected override void DeletePipes()
        {
            this.logger.LogDebug("Deleting pipes..");

            try
            {
                this.inFileStream?.Dispose();
                this.inFileStream?.Dispose();
                File.Delete(this.InPipeAbsolutePath);
                File.Delete(this.OutPipeAbsolutePath);
            }
            catch (Exception e)
            {
                this.logger.LogDebug(e.Message);
            }

            this.logger.LogDebug("Pipes deleted.");
        }
    }
}