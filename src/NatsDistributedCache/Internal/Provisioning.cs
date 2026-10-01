using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using NATS.Client.ObjectStore;

namespace NatsDistributedCache.Internal;

/// <summary>Store names derived from the prefix (design section 3).</summary>
internal sealed record StoreNames(string Prefix)
{
    public string Cache => $"{Prefix}_cache";
    public string Locks => $"{Prefix}_locks";
    public string Notifications => $"{Prefix}_notifications";
    public string Objects => $"{Prefix}_objects";
    public string NotifySubjects => $"{Prefix}.notify.>";
}

/// <summary>
/// Creates the four stores if missing and verifies them if present (design section 3, Q4). Every store is
/// file-backed; an existing memory store, or a KV bucket with History &gt; 1 or without TTL support, is a
/// provisioning failure (NATS cannot change those in place), never a crash.
/// </summary>
internal sealed class Provisioner
{
    private readonly NatsCacheOptions _options;
    private readonly StoreNames _names;

    public Provisioner(NatsCacheOptions options)
    {
        _options = options;
        _names = new StoreNames(options.Prefix);
    }

    public NatsKVConfig CacheConfig() => new(_names.Cache)
    {
        History = 1,
        Storage = NatsKVStorageType.File,
        NumberOfReplicas = _options.Replicas,
        LimitMarkerTTL = _options.LimitMarkerTtl,
        MaxAge = _options.CacheMaxAge,
        MaxBytes = _options.CacheMaxBytes,
    };

    public NatsKVConfig LocksConfig() => new(_names.Locks)
    {
        History = 1,
        Storage = NatsKVStorageType.File,
        NumberOfReplicas = _options.Replicas,
        LimitMarkerTTL = _options.LimitMarkerTtl,
        MaxAge = TimeSpan.FromTicks(_options.MaxLeaseTtl.Ticks * 2),
    };

    public StreamConfig NotificationsConfig() => new(_names.Notifications, [_names.NotifySubjects])
    {
        Storage = StreamConfigStorage.File,
        NumReplicas = _options.Replicas,
        MaxAge = _options.NotificationsMaxAge,
        MaxMsgs = _options.NotificationsMaxMsgs,
        Discard = StreamConfigDiscard.Old,
    };

    public NatsObjConfig ObjectsConfig() => new(_names.Objects)
    {
        Storage = NatsObjStorageType.File,
        NumberOfReplicas = _options.Replicas,
        MaxAge = _options.CacheMaxAge,
    };

    public static void ValidateOptions(NatsCacheOptions o)
    {
        CacheKeys.ValidateStorePrefix(o.Prefix);
        if (o.LimitMarkerTtl < TimeSpan.FromSeconds(1)) throw new ArgumentOutOfRangeException(nameof(o.LimitMarkerTtl), "LimitMarkerTtl must be at least 1 s.");
        if (o.Replicas is < 1 or > 5) throw new ArgumentOutOfRangeException(nameof(o.Replicas), "Replicas must be 1..5.");
        if (o.MaxLeaseTtl <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(o.MaxLeaseTtl));
        if (o.SchemaVersion < 1) throw new ArgumentOutOfRangeException(nameof(o.SchemaVersion), "Schema versions start at 1.");
        if (o.KnownSchemaVersions.Any(v => v < 1)) throw new ArgumentOutOfRangeException(nameof(o.KnownSchemaVersions), "Schema versions start at 1.");
        if (string.IsNullOrWhiteSpace(o.NodeId)) throw new ArgumentException("NodeId must be set.", nameof(o.NodeId));
    }

    /// <summary>Checks an existing KV bucket's backing stream; returns the reasons it is unusable.</summary>
    public static IReadOnlyList<string> CheckKv(StreamConfig existing)
    {
        var problems = new List<string>();
        if (existing.Storage != StreamConfigStorage.File) problems.Add($"storage is {existing.Storage}, must be File");
        if (existing.MaxMsgsPerSubject != 1) problems.Add($"history is {existing.MaxMsgsPerSubject}, must be 1 (a longer history silently stretches short TTLs)");
        if (!existing.AllowMsgTTL) problems.Add("per-message TTL is not enabled");
        if (existing.SubjectDeleteMarkerTTL <= TimeSpan.Zero) problems.Add("delete markers (LimitMarkerTTL) are not enabled");
        return problems;
    }

    public static IReadOnlyList<string> CheckStorageOnly(StreamConfig existing) =>
        existing.Storage == StreamConfigStorage.File ? [] : [$"storage is {existing.Storage}, must be File"];

    public async Task EnsureAsync(INatsJSContext js, ILogger logger, CancellationToken ct)
    {
        var kv = new NatsKVContext(js);
        await EnsureKvAsync(js, kv, CacheConfig(), logger, ct).ConfigureAwait(false);
        await EnsureKvAsync(js, kv, LocksConfig(), logger, ct).ConfigureAwait(false);

        var notify = NotificationsConfig();
        var existingNotify = await TryGetStreamConfigAsync(js, notify.Name!, ct).ConfigureAwait(false);
        if (existingNotify is null)
        {
            await js.CreateStreamAsync(notify, ct).ConfigureAwait(false);
            logger.LogInformation("Created stream {Stream}", notify.Name);
        }
        else
        {
            Fail(notify.Name!, CheckStorageOnly(existingNotify));
        }

        var objects = ObjectsConfig();
        var existingObjects = await TryGetStreamConfigAsync(js, $"OBJ_{objects.Bucket}", ct).ConfigureAwait(false);
        if (existingObjects is null)
        {
            await new NatsObjContext(js).CreateObjectStoreAsync(objects, ct).ConfigureAwait(false);
            logger.LogInformation("Created object store {Bucket}", objects.Bucket);
        }
        else
        {
            Fail(objects.Bucket, CheckStorageOnly(existingObjects));
        }
    }

    private static async Task EnsureKvAsync(INatsJSContext js, NatsKVContext kv, NatsKVConfig config, ILogger logger, CancellationToken ct)
    {
        var existing = await TryGetStreamConfigAsync(js, $"KV_{config.Bucket}", ct).ConfigureAwait(false);
        if (existing is null)
        {
            await kv.CreateStoreAsync(config, ct).ConfigureAwait(false);
            logger.LogInformation("Created KV bucket {Bucket}", config.Bucket);
            return;
        }

        Fail(config.Bucket, CheckKv(existing));
    }

    private static void Fail(string store, IReadOnlyList<string> problems)
    {
        if (problems.Count > 0)
            throw new CacheProvisioningException($"Store '{store}' exists but is unusable: {string.Join("; ", problems)}. Recreate it or use another prefix.");
    }

    private static async Task<StreamConfig?> TryGetStreamConfigAsync(INatsJSContext js, string stream, CancellationToken ct)
    {
        try
        {
            return (await js.GetStreamAsync(stream, cancellationToken: ct).ConfigureAwait(false)).Info.Config;
        }
        catch (NatsJSApiException ex) when (ex.Error.ErrCode == 10059)
        {
            return null; // stream not found
        }
    }
}
