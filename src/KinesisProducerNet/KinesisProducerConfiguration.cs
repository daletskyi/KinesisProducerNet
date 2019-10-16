using System.Collections.Generic;
using Amazon.Runtime;
using KinesisProducerNet.Protobuf;
using Microsoft.Extensions.Logging;

namespace KinesisProducerNet
{
    /// <summary>
    /// Configuration for <see cref="KinesisProducer"/>
    /// </summary>
    public class KinesisProducerConfiguration
    {
        private readonly List<AdditionalDimension> additionalDimensions = new List<AdditionalDimension>();

        /// <summary>
        /// Enable aggregation. With aggregation, multiple user records are packed into a single
        /// KinesisRecord. If disabled, each user record is sent in its own KinesisRecord.
        /// <para>
        /// If your records are small, enabling aggregation will allow you to put many more records
        /// than you would otherwise be able to for a shard before getting throttled.
        /// </para>
        /// Default: true
        /// </summary>
        public bool AggregationEnabled { get; set; } = true;

        /// <summary>
        /// Maximum number of items to pack into an aggregated record.
        /// <para>
        /// There should be normally no need to adjust this. If you want to limit the time records
        /// spend buffering, look into <see cref="RecordMaxBufferedTime"/> instead.
        /// </para>
        /// </summary>
        public ulong AggregationMaxCount { get; set; } = 4294967295L;

        /// <summary>
        /// Maximum number of bytes to pack into an aggregated Kinesis record.
        /// <para>
        /// There should be normally no need to adjust this. If you want to limit the time records
        /// spend buffering, look into <see cref="RecordMaxBufferedTime"/> instead.
        /// </para>
        /// <para>
        /// If a record has more data by itself than this limit, it will bypass the aggregator. Note
        /// the backend enforces a limit of 50KB on record size. If you set this beyond 50KB, oversize
        /// records will be rejected at the backend.
        /// </para>
        /// Default: 51200
        /// Minimun: 64
        /// Maximum (inclusive): 1048576
        /// </summary>
        public ulong AggregationMaxSize { get; set; } = 51200L;

        /// <summary>
        /// Use a custom CloudWatch endpoint.
        /// <para>
        /// Note this does not accept protocols or paths, only host names or ip addresses. There is no
        /// way to disable TLS. The library always connects with TLS.
        /// </para>
        /// Expected pattern: ^([A-Za-z0-9-\\.]+)?$
        /// </summary>
        public string CloudwatchEndpoint { get; set; } = "";

        /// <summary>
        /// Server port to connect to for CloudWatch.
        /// Default: 443
        /// Minimum: 1
        /// Maximum (inclusive): 65535
        /// </summary>
        public ulong CloudwatchPort { get; set; } = 443L;

        /// <summary>
        /// Maximum number of items to pack into an PutRecords request.
        /// <para>
        /// There should be normally no need to adjust this. If you want to limit the time records
        /// spend buffering, look into <see cref="RecordMaxBufferedTime"/> instead.
        /// </para>
        /// Default: 500
        /// Minimum: 1
        /// Maximum (inclusive): 500
        /// </summary>
        public ulong CollectionMaxCount { get; set; } = 500L;

        /// <summary>
        /// Maximum amount of data to send with a PutRecords request.
        /// <para>
        /// There should be normally no need to adjust this. If you want to limit the time records
        /// spend buffering, look into <see cref="RecordMaxBufferedTime"/> instead.
        /// </para>
        /// <para>Records larger than the limit will still be sent, but will not be grouped with others.</para>
        /// Default: 5242880
        /// Minimum: 52224
        /// Maximum (inclusive): 9223372036854775807
        /// </summary>
        public ulong CollectionMaxSize { get; set; } = 5242880L;

        /// <summary>
        /// Timeout (milliseconds) for establishing TLS connections.
        /// Default: 6000
        /// Minimum: 100
        /// Maximum (inclusive): 300000
        /// </summary>
        public ulong ConnectTimeout { get; set; } = 6000L;

        /// <summary>
        /// How often to refresh credentials (in milliseconds).
        /// <para>During a refresh, credentials are retrieved from any SDK credentials providers attached to the wrapper and pushed to the core.</para>
        /// Default: 5000
        /// Minimum: 1
        /// Maximum (inclusive): 300000
        /// </summary>
        public ulong CredentialsRefreshDelay { get; set; } = 5000L;

        /// <summary>
        /// This has no effect on Windows.
        /// <para>
        /// If set to true, the KPL native process will attempt to raise its own core file size soft
        /// limit to 128MB, or the hard limit, whichever is lower. If the soft limit is already at or
        /// above the target amount, it is not changed.
        /// </para>
        /// <para>
        /// Note that even if the limit is successfully raised (or already sufficient), it does not
        /// guarantee that core files will be written on a crash, since that is dependent on operation
        /// system settings that's beyond the control of individual processes.
        /// </para>
        /// Default: false
        /// </summary>
        public bool EnableCoreDumps { get; set; } = false;

        /// <summary>
        /// If true, throttled puts are not retried. The records that got throttled will be failed
        /// immediately upon receiving the throttling error. This is useful if you want to react
        /// immediately to any throttling without waiting for the KPL to retry. For example, you can
        /// use a different hash key to send the throttled record to a backup shard.
        /// <para>
        /// If false, the KPL will automatically retry throttled puts. The KPL performs backoff for
        /// shards that it has received throttling errors from, and will avoid flooding them with
        /// retries. Note that records may fail from expiration (<see cref="RecordTtl"/>) if they get delayed
        /// for too long because of throttling.
        /// </para>
        /// Default: false
        /// </summary>
        public bool FailIfThrottled { get; set; } = false;

        /// <summary>
        /// Use a custom Kinesis endpoint.
        /// <para>
        /// Note this does not accept protocols or paths, only host names or ip addresses. There is no
        /// way to disable TLS. The library always connects with TLS.
        /// </para>
        /// Expected pattern: ^([A-Za-z0-9-\\.]+)?$
        /// </summary>
        public string KinesisEndpoint { get; set; } = "";

        /// <summary>
        /// Server port to connect to for Kinesis.
        /// Default: 443
        /// Minimum: 1
        /// Maximum (inclusive): 65535
        /// </summary>
        public ulong KinesisPort { get; set; } = 443L;

        /// <summary>
        /// Minimum level of logs. Messages below the specified level will not be logged.
        /// Logs for the native KPL daemon show up on stderr.
        /// Default: info
        /// Expected pattern: info|warning|error
        /// </summary>
        public LogLevel LogLevel { get; set; } = LogLevel.Debug;

        /// <summary>
        /// Maximum number of connections to open to the backend. HTTP requests are sent in parallel over multiple connections.
        /// <para>
        /// Setting this too high may impact latency and consume additional resources without increasing throughput.
        /// </para>
        /// Default: 24
        /// Minimum: 1
        /// Maximum (inclusive): 256
        /// </summary>
        public ulong MaxConnections { get; set; } = 24L;

        /// <summary>
        /// Controls the granularity of metrics that are uploaded to CloudWatch. Greater granularity produces more metrics.
        /// <para>
        /// When "shard" is selected, metrics are emitted with the stream name and shard id as
        /// dimensions. On top of this, the same metric is also emitted with only the stream name
        /// dimension, and lastly, without the stream name. This means for a particular metric, 2
        /// streams with 2 shards (each) will produce 7 CloudWatch metrics, one for each shard, one for
        /// each stream, and one overall, all describing the same statistics, but at different levels
        /// of granularity.
        /// </para>
        /// <para>
        /// When "stream" is selected, per shard metrics are not uploaded; when "global" is selected,
        /// only the total aggregate for all streams and all shards are uploaded.
        /// </para>
        /// <para>
        /// Consider reducing the granularity if you're not interested in shard-level metrics, or if
        /// you have a large number of shards.
        /// </para>
        /// <para>
        /// If you only have 1 stream, select "global"; the global data will be equivalent to that for the stream.
        /// </para>
        /// <para>Refer to the metrics documentation for details about each metric.</para>
        /// Default: shard
        /// Expected pattern: global|stream|shard
        /// </summary>
        public string MetricsGranularity { get; set; } = "shard";

        /// <summary>
        /// Controls the number of metrics that are uploaded to CloudWatch.
        /// <para>"none" disables all metrics.</para>
        /// <para>"summary" enables the following metrics: UserRecordsPut, KinesisRecordsPut, ErrorsByCode, AllErrors, BufferingTime.</para>
        /// <para>"detailed" enables all remaining metrics.</para>
        /// <para>Refer to the metrics documentation for details about each metric.</para>
        /// Default: detailed
        /// Expected pattern: none|summary|detailed
        /// </summary>
        public string MetricsLevel { get; set; } = "detailed";

        /// <summary>
        /// The namespace to upload metrics under.
        /// <para>
        /// If you have multiple applications running the KPL under the same AWS account, you should
        /// use a different namespace for each application.
        /// </para>
        /// <para>
        /// If you are also using the KCL, you may wish to use the application name you have configured
        /// for the KCL as the the namespace here. This way both your KPL and KCL metrics show up under
        /// the same namespace.
        /// </para>
        /// Default: KinesisProducerLibrary
        /// Expected pattern: (?!AWS/).{1,255}
        /// </summary>
        public string MetricsNamespace { get; set; } = "KinesisProducerLibrary";

        /// <summary>
        /// Delay (in milliseconds) between each metrics upload.
        /// <para>For testing only. There is no benefit in setting this lower or higher in production.</para>
        /// Default: 60000
        /// Minimum: 1
        /// Maximum (inclusive): 60000
        /// </summary>
        public ulong MetricsUploadDelay { get; set; } = 60000L;

        /// <summary>
        /// Minimum number of connections to keep open to the backend.
        /// <para>There should be no need to increase this in general.</para>
        /// Default: 1
        /// Minimum: 1
        /// Maximum (inclusive): 16
        /// </summary>
        public ulong MinConnections { get; set; } = 1L;

        /// <summary>
        /// Path to the native KPL binary. Only use this setting if you want to use a custom build of the native code.
        /// </summary>
        public string NativeExecutable { get; set; } = "";

        /// <summary>
        /// Limits the maximum allowed put rate for a shard, as a percentage of the backend limits.
        /// <para>
        /// The rate limit prevents the producer from sending data too fast to a shard. Such a limit is
        /// useful for reducing bandwidth and CPU cycle wastage from sending requests that we know are
        /// going to fail from throttling.
        /// </para>
        /// <para>
        /// Kinesis enforces limits on both the number of records and number of bytes per second. This setting applies to both.
        /// </para>
        /// <para>
        /// The default value of 150% is chosen to allow a single producer instance to completely
        /// saturate the allowance for a shard. This is an aggressive setting. If you prefer to reduce
        /// throttling errors rather than completely saturate the shard, consider reducing this setting.
        /// </para>
        /// Default: 150
        /// Minimum: 1
        /// Maximum (inclusive): 9223372036854775807
        /// </summary>
        public ulong RateLimit { get; set; } = 150L;

        /// <summary>
        /// Maximum amount of itme (milliseconds) a record may spend being buffered before it gets
        /// sent. Records may be sent sooner than this depending on the other buffering limits.
        /// <para>
        /// This setting provides coarse ordering among records - any two records will be reordered by
        /// no more than twice this amount (assuming no failures and retries and equal network
        /// latency).
        /// </para>
        /// <para>
        /// The library makes a best effort to enforce this time, but cannot guarantee that it will be
        /// precisely met. In general, if the CPU is not overloaded, the library will meet this
        /// deadline to within 10ms.
        /// </para>
        /// <para>
        /// Failures and retries can additionally increase the amount of time records spend in the KPL.
        /// If your application cannot tolerate late records, use the <see cref="RecordTtl"/> setting to drop
        /// records that do not get transmitted in time.
        /// </para>
        /// <para>Setting this too low can negatively impact throughput.</para>
        /// Default: 100
        /// Maximum (inclusive): 9223372036854775807
        /// </summary>
        public ulong RecordMaxBufferedTime { get; set; } = 100L;

        /// <summary>
        /// Set a time-to-live on records (milliseconds). Records that do not get successfully put
        /// within the limit are failed.
        /// <para>
        /// This setting is useful if your application cannot or does not wish to tolerate late
        /// records. Records will still incur network latency after they leave the KPL, so take that
        /// into consideration when choosing a value for this setting.
        /// </para>
        /// <para>
        /// If you do not wish to lose records and prefer to retry indefinitely, set value to a
        /// <see cref="int.MaxValue"/>  This has the potential to cause head-of-line blocking if network
        /// issues or throttling occur. You can respond to such situations by using the metrics
        /// reporting functions of the KPL. You may also set <see cref="FailIfThrottled"/> to true to prevent
        /// automatic retries in case of throttling.
        /// </para>
        /// Default: 30000
        /// Minimum: 100
        /// Maximum (inclusive): 9223372036854775807
        /// </summary>
        public ulong RecordTtl { get; set; } = 30000L;

        /// <summary>
        /// Which region to send records to.
        /// <para>
        /// If you do not specify the region and are running in EC2, the library will use the region the instance is in.
        /// </para>
        /// <para>The region is also used to sign requests.</para>
        /// Expected pattern: ^([a-z]+-[a-z]+-[0-9])?$
        /// </summary>
        public string Region { get; set; } = "";

        /// <summary>
        /// The maximum total time (milliseconds) elapsed between when we begin a HTTP request and
        /// receiving all of the response. If it goes over, the request will be timed-out.
        /// <para>
        /// Note that a timed-out request may actually succeed at the backend. Retrying then leads to
        /// duplicates. Setting the timeout too low will therefore increase the probability of
        /// duplicates.
        /// </para>
        /// Default: 6000
        /// Minimum: 100
        /// Maximum (inclusive): 600000
        /// </summary>
        public ulong RequestTimeout { get; set; } = 6000L;

        /// <summary>
        /// Temp directory into which to extract the native binaries. The KPL requires write permissions in this directory.
        /// <para>If not specified, defaults to /tmp in Unix or C:\Users\{CurrentUser}\AppData\Local\Temp\ in Windows</para>
        /// </summary>
        public string TempDirectory { get; set; } = "";

        /// <summary>
        /// Verify SSL certificates. Always enable in production for security.
        /// Default: true
        /// </summary>
        public bool VerifyCertificate { get; set; } = true;

        /// <summary>
        /// Configures the threading model used by the native process for handling requests to AWS Services.
        /// <see cref="Configuration.ThreadConfig.PerRequest"/> Tells the native process to create a thread for each request.
        /// <see cref="Configuration.ThreadConfig.Pooled"/> Tells the native process to use a thread pool. The size of the pool can be controlled by <see cref="ThreadPoolSize"/>
        /// </summary>
        public Configuration.ThreadConfig ThreadingModel { get; set; } = Configuration.ThreadConfig.PerRequest;

        /// <summary>
        /// This configures the maximum number of threads the thread pool in the native process will use. This is only used
        /// when <see cref="ThreadingModel"/> is set to <see cref="Configuration.ThreadConfig.Pooled"/>.
        /// The default value is 0 which allows the native process to choose the size of the thread pool.
        /// There is no specific maximum, but operation systems may impose a maximum. If the native process exceeds that maximum it may be terminated.
        /// </summary>
        public uint ThreadPoolSize { get; set; } = 0;

        /// <summary>
        /// <see cref="AWSCredentials"/> that supplies credentials used to put records to Kinesis. These credentials will
        /// also be used to upload metrics to CloudWatch, unless <see cref="MetricsCredentialsProvider"/> is used to provide
        /// separate credentials for that.
        /// Defaults to an instance of <see cref="DefaultAWSCredentialsProviderChain"/>
        /// </summary>
        public AWSCredentials CredentialsProvider { get; set; } = new DefaultAWSCredentialsProviderChain();

        /// <summary>
        /// <see cref="AWSCredentials"/> that supplies credentials used to upload
        /// metrics to CloudWatch. If not given, the credentials used to put records
        /// to Kinesis are also used for CloudWatch.
        /// </summary>
        public AWSCredentials MetricsCredentialsProvider { get; set; }

        /// <summary>
        /// Add an additional, custom dimension to the metrics emitted by the library.
        /// <para>
        /// For example, you can make the library emit per-host metrics by adding HostName as the key and the domain name of the
        /// current host as the value.
        /// </para>
        /// <para>
        /// The granularity of the custom dimension must be specified with the granularity parameter. The options are
        /// "global", "stream" and "shard", just like <see cref="MetricsGranularity"/>. If global is chosen, the custom
        /// dimension will be inserted before the stream name; if stream is chosen then the custom metric will be inserted
        /// after the stream name, but before the shard id. Lastly, if shard is chosen, the custom metric is inserted after
        /// the shard id.
        /// </para>
        /// <para>
        /// For example, if you want to see how different hosts are affecting a single stream, you can choose a granularity
        /// of stream for your HostName custom dimension. This will produce per-host metrics for every stream. On the other
        /// hand, if you want to see how a single host is distributing its load across different streams, you can choose a
        /// granularity of global. This will produce per-stream metrics for each host.
        /// </para>
        /// <para>
        /// Note that custom dimensions will multiplicatively increase the number of metrics emitted by the library into CloudWatch.
        /// </para>
        /// </summary>
        /// <param name="key">Name of the dimension, e.g. "HostName". Length must be between 1 and 255.</param>
        /// <param name="value">Value of the dimension, e.g. "my-host-1.my-domain.com". Length must be between 1 and 255.</param>
        /// <param name="granularity">Granularity of the custom dimension, must be one of "global", "stream" or "shard"</param>
        public void AddAdditionalMetricsDimension(string key, string value, string granularity)
        {
            this.additionalDimensions.Add(new AdditionalDimension
            {
                Key = key,
                Value = value,
                Granularity = granularity
            });
        }

        public Message ToProtobufMessage()
        {
            var c = new Configuration
            {
                AggregationEnabled = AggregationEnabled,
                AggregationMaxCount = AggregationMaxCount,
                AggregationMaxSize = AggregationMaxSize,
                CloudwatchEndpoint = CloudwatchEndpoint,
                CloudwatchPort = CloudwatchPort,
                CollectionMaxCount = CollectionMaxCount,
                CollectionMaxSize = CollectionMaxSize,
                ConnectTimeout = ConnectTimeout,
                EnableCoreDumps = EnableCoreDumps,
                FailIfThrottled = FailIfThrottled,
                KinesisEndpoint = KinesisEndpoint,
                KinesisPort = KinesisPort,
                LogLevel = LogLevel,
                MaxConnections = MaxConnections,
                MetricsGranularity = MetricsGranularity,
                MetricsLevel = MetricsLevel,
                MetricsNamespace = MetricsNamespace,
                MetricsUploadDelay = MetricsUploadDelay,
                MinConnections = MinConnections,
                RateLimit = RateLimit,
                RecordMaxBufferedTime = RecordMaxBufferedTime,
                RecordTtl = RecordTtl,
                Region = Region,
                RequestTimeout = RequestTimeout,
                VerifyCertificate = VerifyCertificate
            };

            if (this.ThreadPoolSize > 0)
            {
                c.ThreadPoolSize = ThreadPoolSize;
            }

            c.thread_config = ThreadingModel;
            c.AdditionalMetricDims.AddRange(this.additionalDimensions);

            return new Message
            {
                Id = 0,
                Configuration = c
            };
        }
    }
}