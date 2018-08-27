using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using KinesisProducerNet.Protobuf;
using Microsoft.Extensions.Logging;
using ProtoBuf;

namespace KinesisProducerNet
{
    public abstract class Daemon
    {
        protected const string InPipePrefix = "amz-aws-kpl-in-pipe-";
        protected const string OutPipePrefix = "amz-aws-kpl-out-pipe-";

        protected string InPipe;
        protected string OutPipe;

        protected abstract string InPipeAbsolutePath { get; }
        protected abstract string OutPipeAbsolutePath { get; }

        protected LogInputStreamReader stdOutReader;
        protected LogInputStreamReader stdErrReader;
        protected StreamBuffer inStreamBuffer;
        protected StreamBuffer outStreamBuffer;
        protected long shutdown;
        protected Process process;
        protected CancellationTokenSource cancelTokenSource;
        protected CancellationToken processCancellationToken;
        protected byte[] receiveBuffer = new byte[8 * 1024 * 1024];
        protected readonly ILogger logger = Logging.CreateLogger<Daemon>();

        protected readonly BlockingCollection<Message> outgoingMessages =
            new BlockingCollection<Message>(new ConcurrentQueue<Message>());

        protected readonly BlockingCollection<Message> incomingMessages =
            new BlockingCollection<Message>(new ConcurrentQueue<Message>());

        protected readonly string pathToExecutable;
        protected readonly string workingDir;
        protected readonly Action<Message> onMessage;
        protected readonly Action<Exception> onException;
        protected readonly KinesisProducerConfiguration config;
        protected readonly IDictionary<string, string> environmentVariables;

        public Daemon(
            string pathToExecutable,
            string workingDir,
            Action<Message> onMessage,
            Action<Exception> onException,
            KinesisProducerConfiguration config,
            IDictionary<string, string> environmentVariables)
        {
            this.pathToExecutable = pathToExecutable;
            this.workingDir = workingDir;
            this.onMessage = onMessage;
            this.onException = onException;
            this.config = config;
            this.environmentVariables = environmentVariables;
            this.cancelTokenSource = new CancellationTokenSource();
            this.processCancellationToken = cancelTokenSource.Token;

            Task.Run(() =>
            {
                try
                {
                    CreatePipes();
                    StartChildProcess();
                }
                catch (Exception e)
                {
                    FatalError("Error running child process", e);
                }
            });
        }

        protected abstract void CreatePipes();
        protected abstract void ConnectToChild();
        protected abstract void SendMessage();
        protected abstract void ReceiveMessage();
        protected abstract void DeletePipes();

        public void Add(Message msg)
        {
            if (Interlocked.Read(ref shutdown) != 0)
            {
                throw new DaemonException("The child process has been shutdown and can no longer accept messages.");
            }
            
            try
            {
                this.outgoingMessages.Add(msg);
            }
            catch (Exception e)
            {
                this.FatalError("Unexpected error", e);
            }
        }

        public void Destroy()
        {
            this.FatalError("Destroy is called", false);
            this.cancelTokenSource.Cancel();
        }

        protected void StartChildProcess()
        {
            this.logger.LogInformation("Asking for trace");
            this.logger.LogInformation($"Path to executable: {this.pathToExecutable}");
            var startInfo = new ProcessStartInfo(this.pathToExecutable);

            var configString = SerializeMessageToHexString(this.config.ToProtobufMessage());
            var credsString = SerializeMessageToHexString(MakeSetCredentialsMessage(this.config.CredentialsProvider, false));

            var metricsCredsProvider = this.config.MetricsCredentialsProvider ?? this.config.CredentialsProvider;
            var metricsCredsString = SerializeMessageToHexString(MakeSetCredentialsMessage(metricsCredsProvider, true));

            var args = $"-i {InPipeAbsolutePath} -o {OutPipeAbsolutePath} -c {configString} -k {credsString} -w {metricsCredsString} -t";
            this.logger.LogDebug($"Starting Native Process: {args}");

            startInfo.Arguments = args;
            startInfo.UseShellExecute = false;
            foreach (var ev in this.environmentVariables)
            {
                startInfo.Environment[ev.Key] = ev.Value;
            }

            try
            {
                startInfo.RedirectStandardOutput = true;
                startInfo.RedirectStandardError = true;
                this.process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                this.logger.LogError(e.StackTrace);
                this.logger.LogError(e.Message + " " + e.GetType());
                this.FatalError("Error starting child process", e, false);
            }

            Task.Run(() =>
            {
                try
                {
                    ConnectToChild();
                    StartMessageLoops();
                }
                catch (IOException e)
                {
                    FatalError("Unexpected error connecting to child process", e, false);
                }
            });

            this.stdOutReader = new LogInputStreamReader(process.StandardOutput, "StdOut", (log, message) => { log.LogInformation(message); }, this.logger);
            this.stdErrReader = new LogInputStreamReader(process.StandardError, "StdErr", (log, message) => { log.LogError(message); }, this.logger);

            Task.Run(() => stdOutReader.Run(), processCancellationToken);
            Task.Run(() => stdErrReader.Run(), processCancellationToken);

            try
            {
                this.process.WaitForExit();
                var code = process.ExitCode;
                this.FatalError("Child process exited with code " + code, code != 1);
            }
            finally
            {
                this.stdOutReader.Shutdown();
                this.stdErrReader.Shutdown();
                this.DeletePipes();
            }
        }

        protected void StartMessageLoops()
        {
            Task.Run(() =>
            {
                while (Interlocked.Read(ref shutdown) == 0) this.SendMessage();
            }, processCancellationToken);

            Task.Run(() =>
            {
                while (Interlocked.Read(ref shutdown) == 0) this.ReceiveMessage();
            }, processCancellationToken);

            Task.Run(() =>
            {
                while (Interlocked.Read(ref shutdown) == 0) this.ReturnMessage();
            }, processCancellationToken);

            Task.Run(() =>
            {
                while (Interlocked.Read(ref shutdown) == 0) this.UpdateCredentials();
            }, processCancellationToken);
        }

        protected void ReturnMessage()
        {
            try
            {
                var msg = this.incomingMessages.Take(this.processCancellationToken);
                if (this.onMessage != null)
                {
                    try
                    {
                        this.onMessage(msg);
                    }
                    catch (Exception e)
                    {
                        this.logger.LogError("Error in message handler", e);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                FatalError("Unexpected error", e);
            }
        }

        protected void UpdateCredentials()
        {
            try
            {
                var credsMsg = MakeSetCredentialsMessage(this.config.CredentialsProvider, false);
                this.outgoingMessages.Add(credsMsg);

                var metricsCredsProvider = this.config.MetricsCredentialsProvider ?? this.config.CredentialsProvider;
                var metricsCredsMsg = MakeSetCredentialsMessage(metricsCredsProvider, true);
                this.outgoingMessages.Add(metricsCredsMsg);

                Thread.Sleep((int)config.CredentialsRefreshDelay);
            }
            catch (Exception e)
            {
                this.logger.LogWarning("Exception during updateCredentials", e);
            }
        }

        protected string Guid8Chars()
        {
            return Guid.NewGuid().ToString().Substring(0, 8);
        }

        protected static byte[] MessageToByteArray(Message msg)
        {
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, msg);
                ms.TryGetBuffer(out ArraySegment<byte> buffer);
                var result = new byte[buffer.Count];
                for (int i = 0; i < buffer.Count; i++)
                {
                    result[i] = buffer.Array[i];
                }

                return result;
            }
        }

        protected static string SerializeMessageToHexString(Message msg)
        {
            var data = MessageToByteArray(msg);
            return BitConverter.ToString(data).Replace("-", string.Empty);
        }

        protected static byte[] StringToByteArray(string hex)
        {
            if (hex.Length % 2 == 1)
            {
                throw new Exception("The binary key cannot have an odd number of digits");
            }

            byte[] arr = new byte[hex.Length >> 1];

            for (int i = 0; i < (hex.Length >> 1); ++i)
            {
                arr[i] = (byte)((GetHexVal(hex[i << 1]) << 4) + (GetHexVal(hex[(i << 1) + 1])));
            }

            return arr;
        }

        public static int GetHexVal(char hex)
        {
            int val = (int)hex;
            return val - (val < 58 ? 48 : 55);
        }

        protected Message MakeSetCredentialsMessage(AWSCredentials awsCredentials, bool forMetrics)
        {
            var immutableCredentials = awsCredentials.GetCredentials();

            var creds = new Credentials
            {
                Akid = immutableCredentials.AccessKey,
                SecretKey = immutableCredentials.SecretKey
            };

            if (immutableCredentials.UseToken)
            {
                creds.Token = immutableCredentials.Token;
            }

            var setCredentials = new SetCredentials
            {
                Credentials = creds,
                ForMetrics = forMetrics
            };

            return new Message
            {
                SetCredentials = setCredentials,
                Id = ulong.MaxValue
            };
        }

        protected void FatalError(string message, bool retryable = true)
        {
            FatalError(message, null, retryable);
        }

        protected void FatalError(string message, Exception exception, bool retryable = true)
        {
            if (Interlocked.CompareExchange(ref shutdown, 1, 0) == 0)
            {
                stdErrReader?.PrepareForShutdown();
                stdOutReader?.PrepareForShutdown();
                process?.Kill();

                if (this.onException != null)
                {
                    if (retryable)
                    {
                        this.onException(exception != null
                            ? new Exception(message, exception)
                            : new Exception(message));
                    }
                    else
                    {
                        this.onException(exception != null
                            ? new IrrecoverableError(message, exception)
                            : new IrrecoverableError(message));
                    }
                }
            }
        }
    }
}