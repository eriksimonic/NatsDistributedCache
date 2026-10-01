using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache;

/// <summary>
/// The cache facade (design section 4): L1 → local single-flight → L2 → factory → fenced L2 write.
/// Milestone 2 scope: the distributed lock (milestone 3), invalidation events (milestone 4) and large
/// values in the Object Store are not wired in yet; large values are returned uncached.
/// </summary>
public sealed class NatsCache : INatsCache, IAsyncDisposable, IDisposable
{
    internal const string HeaderCreated = "x-cache-created";
    internal const string HeaderExpires = "x-cache-expires";
    internal const string HeaderTtl = "x-cache-ttl";
    internal const string HeaderNode = "x-cache-node";
    internal const string HeaderSchema = "x-cache-schema";
    internal const string HeaderEncoding = "x-cache-enc";

    private const int MaxFenceAttempts = 3;
    private static readonly TimeSpan ReadyWait = TimeSpan.FromSeconds(2);

    private readonly NatsCacheOptions _options;
    private readonly IL2Store _l2;
    private readonly L1Store _l1;
    private readonly SingleFlight _flight = new();
    private readonly Expiration _expiration;
    private readonly ICacheSerializer _serializer;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly CacheMetrics _metrics = new();
    private readonly Func<CancellationToken, Task> _provision;
    private readonly INatsConnection? _ownedConnection;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly object _startLock = new();
    private Task? _provisioning;
    private volatile bool _ready;
    private volatile Exception? _fatal;
    private int _disposed;

    /// <summary>Creates a cache over <paramref name="connection"/>, or over a new connection to <see cref="NatsCacheOptions.Url"/>.</summary>
    public NatsCache(NatsCacheOptions options, INatsConnection? connection = null, ILoggerFactory? loggerFactory = null,
        TimeProvider? timeProvider = null, ICacheSerializer? serializer = null)
        : this(options, Build(options, connection, out var owned), loggerFactory, timeProvider, serializer, SharedRandomSource.Instance)
    {
        _ownedConnection = owned;
    }

    private NatsCache(NatsCacheOptions options, (IL2Store L2, Func<CancellationToken, ILogger, Task> Provision) nats,
        ILoggerFactory? loggerFactory, TimeProvider? timeProvider, ICacheSerializer? serializer, IRandomSource random)
        : this(options, nats.L2, null, loggerFactory, timeProvider, serializer, random)
    {
        _provision = ct => nats.Provision(ct, _logger);
    }

    /// <summary>Test seam: a custom L2 and provisioning step.</summary>
    internal NatsCache(NatsCacheOptions options, IL2Store l2, Func<CancellationToken, Task>? provision,
        ILoggerFactory? loggerFactory, TimeProvider? timeProvider, ICacheSerializer? serializer, IRandomSource random)
    {
        Provisioner.ValidateOptions(options);
        _options = options;
        _l2 = l2;
        _time = timeProvider ?? TimeProvider.System;
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<NatsCache>();
        _serializer = serializer ?? new SystemTextJsonCacheSerializer(options.JsonSerializerOptions);
        _expiration = new Expiration(options, random);
        _l1 = new L1Store(options.L1SizeLimitBytes, _time);
        _provision = provision ?? (_ => Task.CompletedTask);
    }

    /// <summary>True once the NATS stores are provisioned and verified.</summary>
    public bool IsReady => _ready;

    /// <summary>Set when provisioning failed permanently (an existing store is unusable).</summary>
    public Exception? ProvisioningError => _fatal;

    /// <summary>Starts background provisioning; never blocks on NATS (design section 8).</summary>
    public void Start()
    {
        lock (_startLock) _provisioning ??= Task.Run(() => ProvisionLoopAsync(_shutdown.Token));
    }

    public async ValueTask<T> GetOrCreateAsync<T>(string key, Func<FactoryContext, CancellationToken, ValueTask<T>> factory,
        CacheEntryOptions? options = null, CancellationToken ct = default)
    {
        CacheKeys.Validate(key);
        if (factory is null) throw new ArgumentNullException(nameof(factory));
        var o = options ?? _options.DefaultEntryOptions;
        var ik = InternalKey<T>(key);

        if (TryL1<T>(ik, out var cached))
        {
            _metrics.L1Hits.Add(1);
            return cached;
        }

        return await _flight.RunAsync($"{ik}|{typeof(T).FullName}", () => LoadAsync(key, ik, factory, o), ct).ConfigureAwait(false);
    }

    public async ValueTask<CacheResult<T>> TryGetAsync<T>(string key, CancellationToken ct = default)
    {
        CacheKeys.Validate(key);
        var ik = InternalKey<T>(key);
        if (_l1.TryGet(ik, out var item) && (item.Value is T || (item.Value is null && default(T) is null)))
        {
            _metrics.L1Hits.Add(1);
            return new CacheResult<T>(true, (T?)item.Value, item.Revision);
        }

        try
        {
            await EnsureReadyAsync(ct).ConfigureAwait(false);
            var entry = await ReadAboveFloorAsync(ik, ct).ConfigureAwait(false);
            if (TryUse<T>(entry, _options.DefaultEntryOptions, out var value))
                return new CacheResult<T>(true, value, entry!.Revision);
            return default;
        }
        catch (L2UnavailableException ex)
        {
            ThrowIfClosed(null, ex);
            return default;
        }
    }

    public async ValueTask SetAsync<T>(string key, T value, CacheEntryOptions? options = null, CancellationToken ct = default)
    {
        CacheKeys.Validate(key);
        var o = options ?? _options.DefaultEntryOptions;
        var ik = InternalKey<T>(key);
        try
        {
            await EnsureReadyAsync(ct).ConfigureAwait(false);
            var write = Prepare(value, o);
            if (write is null) { _l1.Evict(ik, 0, TimeSpan.Zero); return; }
            var r = await _l2.PutAsync(ik, write.Payload, write.Headers, write.Plan.NatsTtl, expected: null, ct).ConfigureAwait(false);
            if (r.Status == WriteStatus.Committed) FillL1(ik, value, r.Seq, write, o);
            else if (r.Status == WriteStatus.Unknown) throw new L2UnavailableException($"Set of '{key}' has an unknown outcome ({r.Error}).");
            else if (r.ErrCode != 0) HandleRejected(key, r);
        }
        catch (L2UnavailableException ex)
        {
            ThrowIfClosed(o, ex);
            // Open mode: L1 only. Outage writes are journaled and replayed as fenced deletes in milestone 4.
            DegradedFill(ik, value, o);
        }
    }

    public async ValueTask<bool> UpdateAsync<T>(string key, ulong expectedRevision, T value, CacheEntryOptions? options = null, CancellationToken ct = default)
    {
        CacheKeys.Validate(key);
        var o = options ?? _options.DefaultEntryOptions;
        var ik = InternalKey<T>(key);
        try
        {
            await EnsureReadyAsync(ct).ConfigureAwait(false);
            var write = Prepare(value, o) ?? throw new ArgumentException("The value cannot be cached with these options.", nameof(value));
            var r = await _l2.PutAsync(ik, write.Payload, write.Headers, write.Plan.NatsTtl, expectedRevision, ct).ConfigureAwait(false);
            if (r.Status == WriteStatus.Unknown)
            {
                // Settle through the leader: our own msg id at the head of the subject means it committed.
                var head = await _l2.ReadAsync(ik, leader: true, ct).ConfigureAwait(false);
                if (head?.Header("Nats-Msg-Id") != r.MsgId) throw new L2UnavailableException($"Update of '{key}' has an unknown outcome.");
                r = r with { Status = WriteStatus.Committed, Seq = head.Revision };
            }

            if (r.Status == WriteStatus.Committed)
            {
                FillL1(ik, value, r.Seq, write, o);
                return true;
            }

            if (r.IsWrongLastSequence) return false;
            HandleRejected(key, r);
            return false;
        }
        catch (L2UnavailableException ex)
        {
            // A compare-and-swap cannot be emulated without L2, whatever the failure mode.
            throw new CacheUnavailableException($"UpdateAsync('{key}') needs NATS, which is unavailable.", ex);
        }
    }

    public async ValueTask RemoveAsync(string key, CancellationToken ct = default)
    {
        CacheKeys.Validate(key);
        var floorTtl = FloorTtl(_options.DefaultEntryOptions);
        var versions = _options.AllKnownSchemaVersions().ToList();
        try
        {
            await EnsureReadyAsync(ct).ConfigureAwait(false);
            foreach (var v in versions)
            {
                var ik = CacheKeys.Internal(key, v);
                var r = await _l2.DeleteAsync(ik, expected: null, ct).ConfigureAwait(false);
                if (r.Status == WriteStatus.Unknown) throw new L2UnavailableException($"Remove of '{ik}' has an unknown outcome.");
                if (r.Status == WriteStatus.Rejected) HandleRejected(key, r);
                _l1.Evict(ik, r.Seq, floorTtl);
            }
        }
        catch (L2UnavailableException ex)
        {
            foreach (var v in versions) _l1.Evict(CacheKeys.Internal(key, v), 0, TimeSpan.Zero);
            ThrowIfClosed(null, ex);
        }
    }

    public async ValueTask RemoveByTagAsync(string prefix, CancellationToken ct = default)
    {
        CacheKeys.ValidatePrefix(prefix);
        var floorTtl = FloorTtl(_options.DefaultEntryOptions);
        ulong highest = 0;
        try
        {
            await EnsureReadyAsync(ct).ConfigureAwait(false);
            await foreach (var ik in _l2.ListKeysAsync($"{prefix}.>", ct).ConfigureAwait(false))
            {
                var r = await _l2.DeleteAsync(ik, expected: null, ct).ConfigureAwait(false);
                if (r.Status == WriteStatus.Unknown) throw new L2UnavailableException($"Tag delete of '{ik}' has an unknown outcome.");
                if (r.Status == WriteStatus.Rejected) HandleRejected(ik, r);
                if (r.Seq > highest) highest = r.Seq;
            }

            _l1.EvictPrefix($"{prefix}.", highest, floorTtl);
        }
        catch (L2UnavailableException ex)
        {
            _l1.EvictPrefix($"{prefix}.", 0, TimeSpan.Zero);
            ThrowIfClosed(null, ex);
        }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        // Idempotent: DI disposes the instance once per registration (NatsCache and INatsCache).
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _shutdown.Cancel();
        if (_provisioning is { } p)
        {
            try { await p.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }

        _l1.Dispose();
        _metrics.Dispose();
        if (_ownedConnection is not null) await _ownedConnection.DisposeAsync().ConfigureAwait(false);
        _shutdown.Dispose();
    }

    // ------------------------------------------------------------------ read path

    private async Task<T> LoadAsync<T>(string key, string ik, Func<FactoryContext, CancellationToken, ValueTask<T>> factory, CacheEntryOptions o)
    {
        if (TryL1<T>(ik, out var cached)) return cached; // filled while we queued

        L2Entry? head;
        try
        {
            await EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);
            if (TryUse<T>(await ReadAboveFloorAsync(ik, CancellationToken.None).ConfigureAwait(false), o, out var hit))
            {
                _metrics.L2Hits.Add(1);
                return hit;
            }

            // Milestone 3 takes the distributed lock here. The leader double-check below is coherent and
            // yields the revision the fenced write expects (design section 4, step 4.1).
            head = await _l2.ReadAsync(ik, leader: true, CancellationToken.None).ConfigureAwait(false);
            if (TryUse<T>(head, o, out hit))
            {
                _metrics.L2Hits.Add(1);
                return hit;
            }
        }
        catch (L2UnavailableException ex)
        {
            return await DegradedAsync(key, ik, factory, o, ex).ConfigureAwait(false);
        }

        _metrics.Misses.Add(1);
        var value = await RunFactoryAsync(key, factory, FactoryReason.Miss, null, o).ConfigureAwait(false);
        try
        {
            return await WriteFencedAsync(ik, value, head?.Revision ?? 0, o).ConfigureAwait(false);
        }
        catch (L2UnavailableException ex)
        {
            ThrowIfClosed(o, ex);
            DegradedFill(ik, value, o);
            return value;
        }
    }

    /// <summary>
    /// Direct Get, re-read once through the leader when the result is below the key's high-water revision
    /// (a lagging replica after a delete or a newer write; design section 7). A result still below the floor
    /// is discarded rather than served.
    /// </summary>
    private async Task<L2Entry?> ReadAboveFloorAsync(string ik, CancellationToken ct)
    {
        var entry = await _l2.ReadAsync(ik, leader: false, ct).ConfigureAwait(false);
        var floor = _l1.Floor(ik);
        if (entry is null || entry.Revision >= floor) return entry;
        entry = await _l2.ReadAsync(ik, leader: true, ct).ConfigureAwait(false);
        return entry is not null && entry.Revision < floor ? null : entry;
    }

    /// <summary>
    /// Fence rule (design section 5): write expecting <paramref name="expected"/>. On 10071 read the leader:
    /// empty, marker, purge or a logically expired value = not a conflict, retry with that revision; DEL =
    /// fenced, return the factory result uncached; live newer value = fenced, return it. At most 3 attempts.
    /// </summary>
    private async Task<T> WriteFencedAsync<T>(string ik, T value, ulong expected, CacheEntryOptions o)
    {
        var write = Prepare(value, o);
        if (write is null) return value;

        for (var attempt = 1; attempt <= MaxFenceAttempts; attempt++)
        {
            var r = await _l2.PutAsync(ik, write.Payload, write.Headers, write.Plan.NatsTtl, expected, CancellationToken.None).ConfigureAwait(false);
            if (r.Status == WriteStatus.Committed)
            {
                FillL1(ik, value, r.Seq, write, o);
                return value;
            }

            if (r.Status == WriteStatus.Rejected && !r.IsWrongLastSequence)
            {
                HandleRejected(ik, r);
                return value;
            }

            var head = await _l2.ReadAsync(ik, leader: true, CancellationToken.None).ConfigureAwait(false);
            if (r.Status == WriteStatus.Unknown && head?.Header("Nats-Msg-Id") == r.MsgId)
            {
                FillL1(ik, value, head.Revision, write, o);
                return value;
            }

            if (head is null) { expected = 0; continue; }
            if (head.Op is L2Op.Marker or L2Op.Purge) { expected = head.Revision; continue; }
            if (head.Op == L2Op.Delete)
            {
                _metrics.FencedWrites.Add(1, new KeyValuePair<string, object?>("outcome", "fenced-del"));
                return value;
            }

            if (TryUse<T>(head, o, out var newer))
            {
                _metrics.FencedWrites.Add(1, new KeyValuePair<string, object?>("outcome", "fenced-newer"));
                return newer;
            }

            expected = head.Revision; // logically expired PUT: not a conflict
        }

        _metrics.FencedWrites.Add(1, new KeyValuePair<string, object?>("outcome", "fenced-exhausted"));
        return value;
    }

    private async Task<T> DegradedAsync<T>(string key, string ik, Func<FactoryContext, CancellationToken, ValueTask<T>> factory, CacheEntryOptions o, Exception cause)
    {
        ThrowIfClosed(o, cause);
        _metrics.Degraded.Add(1);
        _logger.LogWarning(cause, "L2 unavailable; running the factory for {Key} locally (FailureMode.Open)", key);
        var value = await RunFactoryAsync(key, factory, FactoryReason.Degraded, null, o).ConfigureAwait(false);
        DegradedFill(ik, value, o);
        return value;
    }

    private async Task<T> RunFactoryAsync<T>(string key, Func<FactoryContext, CancellationToken, ValueTask<T>> factory,
        FactoryReason reason, string? lockToken, CacheEntryOptions o)
    {
        _metrics.FactoryCalls.Add(1);
        using var timeout = new CancellationTokenSource(o.FactoryTimeout);
        return await factory(new FactoryContext(key, reason, lockToken, 1), timeout.Token).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ entries

    private sealed record PreparedWrite(byte[] Payload, Dictionary<string, string> Headers, WritePlan Plan, long Size);

    private PreparedWrite? Prepare<T>(T value, CacheEntryOptions o)
    {
        if (value is null && o.CacheNullFor is null) return null; // exceptions and nulls are not cached by default
        var now = _time.GetUtcNow();
        var plan = _expiration.PlanWrite(o, now, value is null ? o.CacheNullFor : null);
        if (plan is null) return null;

        var raw = _serializer.Serialize(value);
        var (payload, encoding) = PayloadCodec.Encode(raw, _options.CompressionThresholdBytes);
        if (payload.Length > _options.LargeValueThresholdBytes)
        {
            _metrics.LargeValuesSkipped.Add(1);
            _logger.LogWarning("Value of {Size} bytes exceeds LargeValueThresholdBytes; Object Store support is not implemented yet, returning it uncached", payload.Length);
            return null;
        }

        var nowMs = now.ToUnixTimeMilliseconds();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [HeaderCreated] = nowMs.ToString(CultureInfo.InvariantCulture),
            [HeaderExpires] = (nowMs + (long)plan.Value.L2Ttl.TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
            [HeaderTtl] = ((long)plan.Value.L2Ttl.TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
            [HeaderNode] = _options.NodeId,
            [HeaderSchema] = _options.SchemaVersionFor(typeof(T)).ToString(CultureInfo.InvariantCulture),
        };
        if (encoding is not null) headers[HeaderEncoding] = encoding;
        return new PreparedWrite(payload, headers, plan.Value, raw.Length);
    }

    /// <summary>A live entry whose logical expiry (server Created + x-cache-ttl) is in the future; fills L1.</summary>
    private bool TryUse<T>(L2Entry? entry, CacheEntryOptions o, out T value)
    {
        value = default!;
        if (entry is null || entry.Op != L2Op.Put) return false;
        if (!long.TryParse(entry.Header(HeaderTtl), NumberStyles.Integer, CultureInfo.InvariantCulture, out var ttlMs)) return false;
        var expiry = entry.Created + TimeSpan.FromMilliseconds(ttlMs);
        var now = _time.GetUtcNow();
        if (expiry <= now) return false;

        try
        {
            var raw = PayloadCodec.Decode(entry.Payload, entry.Header(HeaderEncoding));
            value = _serializer.Deserialize<T>(raw)!;
            var ttl = _expiration.PlanL1(o, now, expiry);
            _l1.Set(entry.Key, new L1Item(value, entry.Revision, now + ttl, raw.Length), ttl);
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _logger.LogWarning(ex, "Discarding unreadable L2 entry {Key} rev {Revision}", entry.Key, entry.Revision);
            return false;
        }
    }

    private bool TryL1<T>(string ik, out T value)
    {
        if (_l1.TryGet(ik, out var item) && (item.Value is T || (item.Value is null && default(T) is null)))
        {
            value = (T)item.Value!;
            return true;
        }

        value = default!;
        return false;
    }

    private void FillL1<T>(string ik, T value, ulong revision, PreparedWrite write, CacheEntryOptions o)
    {
        var now = _time.GetUtcNow();
        var ttl = _expiration.PlanL1(o, now, now + write.Plan.L2Ttl);
        _l1.Set(ik, new L1Item(value, revision, now + ttl, write.Size), ttl);
    }

    private void DegradedFill<T>(string ik, T value, CacheEntryOptions o)
    {
        if (value is null && o.CacheNullFor is null) return;
        var now = _time.GetUtcNow();
        var ttl = _expiration.PlanL1(o, now, now + (value is null ? o.CacheNullFor!.Value : o.L2Ttl));
        _l1.Set(ik, new L1Item(value, 0, now + ttl, 1), ttl);
    }

    private void HandleRejected(string key, WriteResult r)
    {
        if (r.ErrCode == 10077 || (r.Error?.IndexOf("maximum bytes", StringComparison.OrdinalIgnoreCase) ?? -1) >= 0)
        {
            _metrics.L2Full.Add(1);
            _logger.LogWarning("Cache bucket is full; {Key} served uncached ({Error})", key, r.Error);
            return;
        }

        _logger.LogWarning("L2 rejected the write of {Key}: {Code} {Error}", key, r.ErrCode, r.Error);
    }

    private TimeSpan FloorTtl(CacheEntryOptions o) => TimeSpan.FromTicks((long)(o.L1Ttl.Ticks * (1 + o.JitterRatio))) + TimeSpan.FromSeconds(1);

    private string InternalKey<T>(string key) => CacheKeys.Internal(key, _options.SchemaVersionFor(typeof(T)));

    private void ThrowIfClosed(CacheEntryOptions? o, Exception cause)
    {
        if ((o?.FailureMode ?? _options.FailureMode) == FailureMode.Closed)
            throw new CacheUnavailableException("NATS is unavailable and FailureMode is Closed.", cause);
    }

    // ------------------------------------------------------------------ provisioning

    private async Task EnsureReadyAsync(CancellationToken ct)
    {
        if (_ready) return;
        if (_fatal is { } fatal) throw new L2UnavailableException("Cache stores are unusable.", fatal);
        Start();
        var provisioning = _provisioning!;
        using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var done = await Task.WhenAny(provisioning, Task.Delay(ReadyWait, wait.Token)).ConfigureAwait(false);
        wait.Cancel();
        if (!_ready) throw new L2UnavailableException("Cache stores are not provisioned yet.", _fatal);
        _ = done;
    }

    private async Task ProvisionLoopAsync(CancellationToken ct)
    {
        var delay = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await _provision(ct).ConfigureAwait(false);
                _ready = true;
                _logger.LogInformation("NatsCache stores ready (prefix {Prefix})", _options.Prefix);
                return;
            }
            catch (CacheProvisioningException ex)
            {
                _fatal = ex;
                _logger.LogError(ex, "NatsCache stores are unusable; the cache runs degraded");
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Provisioning NatsCache stores failed; retrying in {Delay}", delay);
                try { await Task.Delay(delay, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromSeconds(30).Ticks));
            }
        }
    }

    private static (IL2Store, Func<CancellationToken, ILogger, Task>) Build(NatsCacheOptions options, INatsConnection? connection, out INatsConnection? owned)
    {
        Provisioner.ValidateOptions(options);
        owned = null;
        if (connection is null)
        {
            owned = connection = new NatsConnection(new NatsOpts
            {
                Url = options.Url,
                Name = $"NatsCache-{options.NodeId}",
                MaxReconnectRetry = -1,
                RequestTimeout = TimeSpan.FromSeconds(2), // design section 8; the unknown-outcome path depends on it
            });
        }

        var js = new NatsJSContext(connection);
        var names = new StoreNames(options.Prefix);
        var provisioner = new Provisioner(options);
        return (new NatsL2Store(js, names.Cache), (ct, logger) => provisioner.EnsureAsync(js, logger, ct));
    }
}
