using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Threading;
using KinesisProducerNet.Protobuf;
using Microsoft.Extensions.Logging;
using ProtoBuf;

namespace KinesisProducerNet
{
    internal class DaemonWindows : Daemon
    {
        private NamedPipeClientStream inPipeClient;
        private NamedPipeClientStream outPipeClient;

        public DaemonWindows(string pathToExecutable, string workingDir, Action<Message> onMessage, Action<Exception> onException, KinesisProducerConfiguration config, IDictionary<string, string> environmentVariables)
            : base(pathToExecutable, workingDir, onMessage, onException, config, environmentVariables)
        {
        }

        protected override string InPipeAbsolutePath => @"\\.\pipe\" + this.InPipe;
        protected override string OutPipeAbsolutePath => @"\\.\pipe\" + this.OutPipe;

        protected override void CreatePipes()
        {
            do
            {
                InPipe = InPipePrefix + Guid8Chars();
            } while (File.Exists(InPipeAbsolutePath));

            do
            {
                OutPipe = OutPipePrefix + Guid8Chars();
            } while (File.Exists(OutPipeAbsolutePath));
        }

        protected override void ConnectToChild()
        {
            this.logger.LogDebug("CONNECTING TO I/O PIPES..");
            this.logger.LogDebug("IN PIPE = " + InPipe);
            this.logger.LogDebug("OUT PIPE = " + OutPipe);

            var start = DateTime.UtcNow;
            while (true)
            {
                try
                {
                    this.inPipeClient = new NamedPipeClientStream(".", InPipe, PipeDirection.In);
                    this.inPipeClient.Connect();
                    this.outPipeClient = new NamedPipeClientStream(".", OutPipe, PipeDirection.Out);
                    this.outPipeClient.Connect();

                    this.logger.LogDebug("CONNECTED TO I/O PIPES!");
                    break;
                }
                catch (IOException e)
                {
                    this.logger.LogError("Failed to connect to child process", e);

                    if (this.inPipeClient != null && this.inPipeClient.IsConnected)
                    {
                        this.inPipeClient.Dispose();
                    }

                    if (this.outPipeClient != null && this.outPipeClient.IsConnected)
                    {
                        this.outPipeClient.Dispose();
                    }

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
                if (!this.outPipeClient.IsConnected)
                {
                    this.outPipeClient.Connect();
                }

                var message = this.outgoingMessages.Take(this.processCancellationToken);

                if (this.outStreamBuffer == null)
                {
                    this.outStreamBuffer = new StreamBuffer(this.outPipeClient);
                }

                var data = MessageToByteArray(message);
                this.outStreamBuffer.WriteBuffer(data);
            }
            catch (IOException e)
            {
                FatalError("Error writing message to daemon", e);
            }
            catch (Exception e)
            {
                FatalError(e.Message, e);
            }
        }

        protected override void ReceiveMessage()
        {
            try
            {
                if (!this.inPipeClient.IsConnected)
                {
                    this.inPipeClient.Connect();
                }

                if (this.inStreamBuffer == null)
                {
                    this.inStreamBuffer = new StreamBuffer(this.inPipeClient);
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
                this.inPipeClient?.Dispose();
                this.outPipeClient?.Dispose();
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