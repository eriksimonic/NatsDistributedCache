using System.Text.Json;

namespace NatsDistributedCache;

/// <summary>Global cache options (design section 4).</summary>
public sealed class NatsCacheOptions
{
    private readonly Dictionary<Type, int> _typeVersions = new();

    /// <summary>Comma-separated NATS server URLs, used when no connection is supplied.</summary>
    public string Url { get; set; } = "nats://localhost:4222";

    /// <summary>Required store prefix: stores are named {Prefix}_cache, {Prefix}_locks, {Prefix}_notifications, {Prefix}_objects.</summary>
    public string Prefix { get; set; } = "";

    /// <summary>Identifies this node in entry headers and lock tokens.</summary>
    public string NodeId { get; set; } = Environment.MachineName;

    /// <summary>Default entry options for calls that pass none.</summary>
    public CacheEntryOptions DefaultEntryOptions { get; set; } = CacheEntryOptions.Default;

    /// <summary>Default failure mode; overridable per call.</summary>
    public FailureMode FailureMode { get; set; } = FailureMode.Open;

    /// <summary>Brotli-compress payloads above this size; 0 turns compression off.</summary>
    public int CompressionThresholdBytes { get; set; } = 4096;

    /// <summary>L1 size limit, counted in serialized bytes.</summary>
    public long L1SizeLimitBytes { get; set; } = 64L * 1024 * 1024;

    /// <summary>Values above this size (after compression) are stored in the Object Store and skip L1.</summary>
    public int LargeValueThresholdBytes { get; set; } = 512 * 1024;

    /// <summary>Delete-marker TTL on both KV buckets; at least 1 s.</summary>
    public TimeSpan LimitMarkerTtl { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Upper bound for any lease; the locks bucket MaxAge is twice this.</summary>
    public TimeSpan MaxLeaseTtl { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Lower bound for any lease (design section 5): renewals run every LeaseTtl / 3 and each can take a 2 s request
    /// timeout, so a shorter lease is lost on one slow ack. Default 6 s (3 × the request timeout); at least 1 s.
    /// </summary>
    public TimeSpan MinLeaseTtl { get; set; } = TimeSpan.FromSeconds(6);

    /// <summary>How long DisposeAsync waits for factories that hold a distributed lock to finish and release it.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>MaxAge of the cache bucket: a safety net above any L2 TTL plus grace.</summary>
    public TimeSpan CacheMaxAge { get; set; } = TimeSpan.FromHours(24);

    /// <summary>Optional MaxBytes cap of the cache bucket; -1 = unlimited.</summary>
    public long CacheMaxBytes { get; set; } = -1;

    /// <summary>Retention of the notifications stream.</summary>
    public TimeSpan NotificationsMaxAge { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Message cap of the notifications stream.</summary>
    public long NotificationsMaxMsgs { get; set; } = 1_000_000;

    /// <summary>Replicas of every store. 3 in production; 1 only for single-node tests.</summary>
    public int Replicas { get; set; } = 3;

    /// <summary>Default schema version, the last internal key segment (_s{n}).</summary>
    public int SchemaVersion { get; set; } = 1;

    /// <summary>Schema versions RemoveAsync deletes; add the previous one during a rolling deploy.</summary>
    public int[] KnownSchemaVersions { get; set; } = [1];

    /// <summary>JSON options of the default serializer; set a TypeInfoResolver to use source generation.</summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Called after every factory run with its outcome (cached, fenced, uncached, local-only or failed), after the
    /// write and the lock release. Runs inline; exceptions are logged and swallowed. Used by the TestApi to feed the
    /// Origin call ledger (design section 9).
    /// </summary>
    public Action<FactoryCompletion>? OnFactoryCompleted { get; set; }

    /// <summary>Upper bound on keys and prefixes journaled while NATS is down (Open mode); beyond it L1 is still flushed on recovery.</summary>
    public int OutageJournalCapacity { get; set; } = 100_000;

    /// <summary>Overrides the schema version for values of type <typeparamref name="T"/>.</summary>
    public NatsCacheOptions ForType<T>(int schemaVersion)
    {
        if (schemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(schemaVersion), "Schema versions start at 1.");
        _typeVersions[typeof(T)] = schemaVersion;
        return this;
    }

    internal int SchemaVersionFor(Type type) =>
        _typeVersions.TryGetValue(type, out var v) ? v : SchemaVersion;

    internal IEnumerable<int> AllKnownSchemaVersions() =>
        KnownSchemaVersions.Concat(_typeVersions.Values).Append(SchemaVersion).Distinct();
}
