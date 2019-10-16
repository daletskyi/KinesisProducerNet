using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KinesisProducerNet.Protobuf;
using Microsoft.Extensions.Logging;

namespace KinesisProducerNet
{
    public class KinesisProducer : IKinesisProducer
    {
        private static readonly object EXTRACT_BIN_MUTEX = new object();
        private static readonly BigInteger UINT_128_MAX = BigInteger.Pow(ulong.MaxValue, 2);

        private readonly KinesisProducerConfiguration config;
        private Daemon child;
        private string pathToTmpDir;
        private string pathToExecutable;
        private string pathToLibDir;
        private readonly Dictionary<string, string> envParams;
        private long messageNumber = 1;
        private readonly ILogger logger;
        private readonly ConcurrentDictionary<long, FutureOperationResult> futureOperationResults =
            new ConcurrentDictionary<long, FutureOperationResult>();

        private DateTime lastChild = DateTime.Now;
        private bool destroyed = false;
        private ProcessFailureBehavior processFailureBehavior = ProcessFailureBehavior.AutoRestart;

        public KinesisProducer(KinesisProducerConfiguration config)
        {
            this.config = config;
            this.logger = Logging.CreateLogger<KinesisProducer>(config.LogLevel);
            this.logger.LogInformation($"Platform: {RuntimeInformation.OSDescription}. Arch: {RuntimeInformation.OSArchitecture}");

            var caDirectory = ExtractBinaries();

            this.envParams = new Dictionary<string, string>
            {
                {"LD_LIBRARY_PATH", this.pathToLibDir},
                {"DYLD_LIBRARY_PATH", this.pathToLibDir},
                {"CA_DIR", caDirectory}
            };

            CreateDaemon();
        }

        private void CreateDaemon()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                this.child = new DaemonWindows(this.pathToExecutable, this.pathToTmpDir, OnMessage, OnException, this.config, this.envParams);
            }
            else
            {
                this.child = new DaemonUnix(this.pathToExecutable, this.pathToTmpDir, OnMessage, OnException, this.config, this.envParams);
            }
        }

        private void OnMessage(Message message)
        {
            Task.Run(() =>
            {
                if (message?.PutRecordResult != null)
                {
                    OnPutRecordResult(message);
                }
                else if (message?.MetricsResponse != null)
                {
                    OnMetricsResponse(message);
                }
                else
                {
                    this.logger.LogError("Unexpected message type from child process");
                }
            });
        }

        private void OnPutRecordResult(Message message)
        {
            var future = this.GetFuture(message);
            var result = UserRecordResult.FromProtobufMessage(message.PutRecordResult);

            if (result.Successful)
            {
                future.LeftOrDefault?.SetResult(result);
            }
            else
            {
                future.LeftOrDefault?.SetException(new UserRecordFailedException(result));
            }
        }

        private void OnMetricsResponse(Message message)
        {
            var future = this.GetFuture(message);
            var metricsResponse = message.MetricsResponse;
            var userMetrics = metricsResponse.Metrics.Select(metric => new Metric(metric)).ToList();
            future.RightOrDefault?.SetResult(userMetrics);
        }

        private FutureOperationResult GetFuture(Message message)
        {
            ulong id = message.SourceId;

            if (!futureOperationResults.TryRemove((long)id, out FutureOperationResult future) || future == null)
            {
                throw new Exception($"Future for message id {id} not found");
            }

            return future;
        }

        private void OnException(Exception exception)
        {
            // Don't log error if the user called destroy
            if (!this.destroyed)
            {
                this.logger.LogError("Error in child process", exception);
            }

            // Fail all outstanding futures
            foreach (var entry in futureOperationResults)
            {
                Task.Run(() =>
                {
                    entry.Value.LeftOrDefault?.SetException(exception);
                    entry.Value.RightOrDefault?.SetException(exception);
                });
            }

            futureOperationResults.Clear();

            if (this.processFailureBehavior == ProcessFailureBehavior.AutoRestart && !this.destroyed)
            {
                this.logger.LogInformation("Restarting native producer process.");
                this.CreateDaemon();
            }
            else
            {
                // Only restart child if it's not an irrecoverable error, and if
                // there has been some time (3 seconds) between the last child
                // creation. If the child process crashes almost immediately, we're
                // going to abort to avoid going into a loop.
                if (!(exception is IrrecoverableError) && DateTime.Now > lastChild.AddSeconds(3))
                {
                    lastChild = DateTime.Now;
                    this.CreateDaemon();
                }
            }
        }

        public TaskCompletionSource<UserRecordResult> AddUserRecord(string stream, string partitionKey, byte[] data)
        {
            return this.AddUserRecord(stream, partitionKey, null, data);
        }

        public TaskCompletionSource<UserRecordResult> AddUserRecord(UserRecord userRecord)
        {
            return this.AddUserRecord(userRecord.StreamName, userRecord.PartitionKey, userRecord.ExplicitHashKey, userRecord.Data);
        }

        public TaskCompletionSource<UserRecordResult> AddUserRecord(string stream, string partitionKey, string explicitHashKey, byte[] data)
        {
            if (string.IsNullOrEmpty(stream))
            {
                throw new ArgumentException("Stream name cannot be null or empty", nameof(stream));
            }

            stream = stream.Trim();
            if (stream.Length == 0)
            {
                throw new ArgumentException("Stream name cannot be empty", nameof(stream));
            }

            if (partitionKey == null)
            {
                throw new ArgumentException("PartitionKey cannot be null", nameof(partitionKey));
            }

            if (partitionKey.Length < 1 || partitionKey.Length > 256)
            {
                throw new ArgumentException($"Invalid parition key. Length must be at least 1 and at most 256, got {partitionKey.Length}", nameof(partitionKey));
            }

            try
            {
                Encoding.UTF8.GetBytes(partitionKey);
            }
            catch (Exception)
            {
                throw new ArgumentException("Partition key must be valid UTF-8", nameof(partitionKey));
            }

            BigInteger? b = null;
            if (explicitHashKey != null)
            {
                explicitHashKey = explicitHashKey.Trim();
                try
                {
                    b = BigInteger.Parse(explicitHashKey);
                }
                catch (FormatException)
                {
                    throw new ArgumentException($"Invalid explicitHashKey, must be an integer, got {explicitHashKey}", nameof(explicitHashKey));
                }

                if (b.Value.CompareTo(UINT_128_MAX) > 0 || b.Value.CompareTo(BigInteger.Zero) < 0)
                {
                    throw new ArgumentException($"Invalid explicitHashKey, must be greater or equal to zero and less than or equal to (2^128 - 1), got {explicitHashKey}", nameof(explicitHashKey));
                }
            }

            if (data != null && data.Length > 1024 * 1024)
            {
                throw new ArgumentException($"Data must be less than or equal to 1MB in size, got {data.Length} bytes", nameof(data));
            }

            long id = Interlocked.Increment(ref messageNumber) - 1;
            var future = new FutureOperationResult(new TaskCompletionSource<UserRecordResult>());
            futureOperationResults.TryAdd(id, future);

            var putRecord = new PutRecord
            {
                Data = data ?? new byte[0],
                StreamName = stream,
                PartitionKey = partitionKey
            };

            if (b.HasValue)
            {
                putRecord.ExplicitHashKey = b.ToString();
            }

            var msg = new Message
            {
                Id = (ulong) id,
                PutRecord = putRecord
            };

            child.Add(msg);

            return future.LeftOrDefault;
        }

        public List<Metric> GetMetrics(string metricName, int windowSeconds)
        {
            var metric = new MetricsRequest();
            if (metricName != null)
            {
                metric.Name = metricName;
            }

            if (windowSeconds > 0)
            {
                metric.Seconds = (ulong)windowSeconds;
            }

            var id = Interlocked.Increment(ref messageNumber) - 1;
            var future = new FutureOperationResult(new TaskCompletionSource<List<Metric>>());
            this.futureOperationResults.TryAdd(id, future);

            var msg = new Message
            {
                Id = (ulong)id,
                MetricsRequest = metric
            };

            this.child.Add(msg);

            return future.RightOrDefault?.Task.Result;
        }

        public List<Metric> GetMetrics(string metricName)
        {
            return this.GetMetrics(metricName, -1);
        }

        public List<Metric> GetMetrics()
        {
            return this.GetMetrics(null);
        }

        public List<Metric> GetMetrics(int windowSeconds)
        {
            return this.GetMetrics(null, windowSeconds);
        }

        public int GetOutstandingRecordsCount()
        {
            return this.futureOperationResults.Count;
        }

        public void Destroy()
        {
            this.destroyed = true;
            this.child.Destroy();
        }

        public void Flush(string stream)
        {
            var flush = new Flush();
            if (stream != null)
            {
                flush.StreamName = stream;
            }

            var id = (ulong) (Interlocked.Increment(ref messageNumber) - 1);
            var msg = new Message
            {
                Id = id,
                Flush = flush
            };

            this.child.Add(msg);
        }

        public void Flush()
        {
            this.Flush(null);
        }

        public void FlushSync()
        {
            while (this.GetOutstandingRecordsCount() > 0)
            {
                this.Flush();
                try
                {
                    Thread.Sleep(500);
                }
                catch (Exception) // ThreadInterruptedException is available from netstandard2.0
                {
                }
            }
        }

        private string ExtractBinaries()
        {
            lock (EXTRACT_BIN_MUTEX)
            {
                var watchFiles = new List<string>();
                var os = GetOSPlatform();
                var root = "amazon_kinesis_producer_native_binaries";
                var tempDirectory = this.config.TempDirectory;
                if (tempDirectory.Trim().Length == 0)
                {
                    tempDirectory = Path.GetTempPath();
                }

                tempDirectory = Path.Combine(tempDirectory, root);
                this.pathToTmpDir = tempDirectory;

                var binPath = config.NativeExecutable;
                if (!string.IsNullOrWhiteSpace(binPath))
                {
                    this.pathToExecutable = binPath.Trim();
                    this.logger.LogWarning("Using non-default native binary at " + this.pathToExecutable);
                    this.pathToLibDir = string.Empty;
                    return string.Empty;
                }
                else
                {
                    this.logger.LogInformation("Extracting binaries to " + tempDirectory);
                    try
                    {
                        if (!Directory.Exists(tempDirectory) && Directory.CreateDirectory(tempDirectory) == null)
                        {
                            throw new IOException("Could not create tmp dir " + tempDirectory);
                        }

                        var extension = os == "windows" ? ".exe" : "";
                        var executableName = "kinesis_producer" + extension;
                        var bin = IOUtils.GetResourceFile($"{root}.{os}.{executableName}", os);
                        var mdHex = SHA1Hash(bin);

                        this.pathToExecutable = Path.Combine(this.pathToTmpDir, $"kinesis_producer_{mdHex}{extension}");
                        watchFiles.Add(this.pathToExecutable);

                        var pathToLock = Path.Combine(this.pathToTmpDir, $"kinesis_producer_{mdHex}.lock");
                        using (var fileLock = new FileStream(pathToLock, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
                        {
                            if (File.Exists(this.pathToExecutable))
                            {
                                var contentEqual = false;
                                if (new FileInfo(this.pathToExecutable).Length == bin.Length)
                                {
                                    byte[] existingBin = File.ReadAllBytes(this.pathToExecutable);
                                    contentEqual = bin.SequenceEqual(existingBin);
                                }

                                if (!contentEqual)
                                {
                                    throw new SecurityException($"The contents of the binary {Path.GetFullPath(pathToExecutable)} is not what it's expected to be.");
                                }
                            }
                            else
                            {
                                File.WriteAllBytes(pathToExecutable, bin);

                                if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                                {
                                    this.logger.LogDebug("Setting execute permissions on native executable..");
                                    var setExecutePermissionsResult = BashHelper.Run($"chmod +x {pathToExecutable}");
                                    this.logger.LogDebug($"Bash result: {setExecutePermissionsResult}");
                                }
                            }
                        }

                        var certificateExtractor = new CertificateExtractor(config.LogLevel);
                        var caDirectory = certificateExtractor.ExtractCertificates(pathToTmpDir);

                        watchFiles.AddRange(certificateExtractor.ExtractedCertificates);
                        this.pathToLibDir = this.pathToTmpDir;
                        FileAgeManager.Instance.RegisterFiles(watchFiles);

                        return caDirectory;
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Could not copy native binaries to temp directory " + tempDirectory, e);
                    }
                }
            }
        }

        private static string GetOSPlatform()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return "windows";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) return "linux";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) return "osx";

            throw new Exception($"Your operation system is not supported ({RuntimeInformation.OSDescription}), the library only supports Linux, OSX and Windows");
        }

        private string SHA1Hash(byte[] inputBytes)
        {
            var sha1 = SHA1.Create();
            byte[] outputBytes = sha1.ComputeHash(inputBytes);
            return BitConverter.ToString(outputBytes).Replace("-", "").ToLower();
        }
    }
}