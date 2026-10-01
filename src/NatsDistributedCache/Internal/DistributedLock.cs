using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;

namespace NatsDistributedCache.Internal;

/// <summary>
/// Lock token (design section 5): <c>{nodeId}:{guid}:{unixMs}:{leaseMs}:{factoryTimeoutMs}</c>. It carries the
/// owner's LeaseTtl and FactoryTimeout so waiters cap their wait with the owner's values, not their own.
/// </summary>
internal sealed record LockToken(string NodeId, string Id, long UnixMs, long LeaseMs, long FactoryTimeoutMs)
{
    /// <summary>Upper bound for durations read from another node's token, so a corrupt token cannot overflow the wait math.</summary>
    internal static readonly long MaxDurationMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;

    public TimeSpan Lease => TimeSpan.FromMilliseconds(LeaseMs);

    public TimeSpan FactoryTimeout => TimeSpan.FromMilliseconds(FactoryTimeoutMs);

    /// <summary>How long the owner renews at most (design section 5): FactoryTimeout + LeaseTtl after the acquire.</summary>
    public TimeSpan RenewalCap => FactoryTimeout + Lease;

    public static LockToken New(string nodeId, DateTimeOffset now, TimeSpan lease, TimeSpan factoryTimeout) =>
        new(nodeId, Guid.NewGuid().ToString("N"), now.ToUnixTimeMilliseconds(),
            (long)lease.TotalMilliseconds, (long)factoryTimeout.TotalMilliseconds);

    public override string ToString() => string.Join(":", NodeId, Id,
        UnixMs.ToString(CultureInfo.InvariantCulture), LeaseMs.ToString(CultureInfo.InvariantCulture),
        FactoryTimeoutMs.ToString(CultureInfo.InvariantCulture));

    /// <summary>
    /// Parses from the right, so a node id containing ':' still round-trips. Null when malformed or when a duration
    /// is outside (0, 1 day]; waiters then fall back to their own values.
    /// </summary>
    public static LockToken? Parse(ReadOnlyMemory<byte> payload)
    {
        var parts = Encoding.UTF8.GetString(payload.ToArray()).Split(':');
        if (parts.Length < 5) return null;
        var n = parts.Length;
        if (!long.TryParse(parts[n - 3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var unixMs)
            || !long.TryParse(parts[n - 2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var leaseMs)
            || !long.TryParse(parts[n - 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var factoryMs))
            return null;
        if (leaseMs is <= 0 || leaseMs > MaxDurationMs || factoryMs is <= 0 || factoryMs > MaxDurationMs) return null;
        return new LockToken(string.Join(":", parts, 0, n - 4), parts[n - 4], unixMs, leaseMs, factoryMs);
    }
}

/// <summary>Result of an acquire: the lease when won, otherwise the holder's lock message (null if unknown).</summary>
internal readonly record struct AcquireResult(LockLease? Lease, L2Entry? Holder);

internal enum RenewStatus
{
    /// <summary>The lock message at <see cref="RenewResult.Revision"/> is ours and carries a fresh TTL.</summary>
    Renewed,

    /// <summary>Still ours at the old revision, but the TTL was not extended (unknown outcome or a transient rejection): retry soon.</summary>
    NotRenewed,

    /// <summary>Someone else's message (or a free marker) is at the head: the lease is gone.</summary>
    Lost,
}

internal readonly record struct RenewResult(RenewStatus Status, ulong Revision, DateTimeOffset ConfirmedAt);

/// <summary>
/// Distributed single-flight lock over the <c>{prefix}_locks</c> bucket (design section 5), ported from the
/// spike's HelperAcquire / HelperRelease. Every write goes through the publish helper with Nats-TTL = lease,
/// never NATS.Net CreateAsync (spurious failures on tombstoned keys) or UpdateAsync (drops the TTL).
/// </summary>
internal sealed class DistributedLock
{
    internal const int MaxAcquireAttempts = 3;

    /// <summary>A release is best effort: past this budget the lock is left to expire with its lease (design section 5).</summary>
    internal static readonly TimeSpan ReleaseBudget = TimeSpan.FromSeconds(5);

    private static readonly IReadOnlyDictionary<string, string> NoHeaders = new Dictionary<string, string>();

    private readonly IL2Store _locks;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly string _nodeId;
    private readonly CacheMetrics? _metrics;

    public DistributedLock(IL2Store locks, string nodeId, TimeProvider time, ILogger logger, CacheMetrics? metrics = null)
    {
        _locks = locks;
        _nodeId = nodeId;
        _time = time;
        _logger = logger;
        _metrics = metrics;
    }

    internal TimeProvider Time => _time;

    internal ILogger Logger => _logger;

    /// <summary>The lock message for <paramref name="key"/> (Direct Get unless <paramref name="leader"/>).</summary>
    public ValueTask<L2Entry?> ReadAsync(string key, bool leader, CancellationToken ct) => _locks.ReadAsync(key, leader, ct);

    /// <summary>A live lock message: a token whose server TTL has not produced a marker yet, so its owner renewed within one lease.</summary>
    public static bool IsHeld(L2Entry? head) => head is { Op: L2Op.Put };

    /// <summary>
    /// Acquire (design section 5): publish a fresh token expecting an empty subject. On 10071 or an unknown outcome,
    /// leader-read the subject: our own token = owned (an earlier send committed although we did not see the ack),
    /// a tombstone or TTL marker = free, so publish expecting its sequence (a new msg id), empty = publish
    /// expecting 0, someone else's live token = held. A 10071 naming sequence 0 (the marker expired meanwhile)
    /// goes straight back to expecting 0. At most 3 publishes, then lost.
    /// </summary>
    public async Task<AcquireResult> TryAcquireAsync(string key, TimeSpan lease, TimeSpan factoryTimeout, CancellationToken ct)
    {
        var token = LockToken.New(_nodeId, _time.GetUtcNow(), lease, factoryTimeout);
        var bytes = Encoding.UTF8.GetBytes(token.ToString());
        var expected = 0UL;
        L2Entry? last = null;
        for (var attempt = 0; attempt < MaxAcquireAttempts; attempt++)
        {
            var r = await _locks.PutAsync(key, bytes, NoHeaders, lease, expected, ct).ConfigureAwait(false);
            if (r.Status == WriteStatus.Committed)
                return new(new LockLease(this, key, token, bytes, r.Seq, _time.GetUtcNow()), null);
            if (r.Status == WriteStatus.Rejected && !r.IsWrongLastSequence)
            {
                // No lock can be taken (e.g. the locks bucket is full or gone): the caller applies FailureMode.
                _metrics?.LockRejected.Add(1);
                _logger.LogError("The locks bucket rejected the acquire of {Key}: {Code} {Error}", key, r.ErrCode, r.Error);
                throw new L2UnavailableException($"The locks bucket rejected the acquire of '{key}' ({r.ErrCode} {r.Error}); no distributed lock is possible.");
            }

            if (r.IsWrongLastSequence && r.LastSequenceFromError() == 0)
            {
                expected = 0;
                continue;
            }

            last = await _locks.ReadAsync(key, leader: true, ct).ConfigureAwait(false);
            if (last is null)
            {
                expected = 0;
                continue;
            }

            if (IsOurs(last, bytes))
            {
                // Our write committed behind an unknown outcome: the lease started at its server time.
                return new(new LockLease(this, key, token, bytes, last.Revision, Earliest(last.Created)), null);
            }

            if (IsHeld(last)) return new(null, last);
            expected = last.Revision;
        }

        return new(null, IsHeld(last) ? last : null);
    }

    /// <summary>
    /// Renew: republish the token expecting the lease's latest revision, with a fresh Nats-TTL. Any outcome other
    /// than a commit is settled by a leader read: our token at a newer revision = an earlier renewal landed late
    /// (adopt it), our token at the same revision = not renewed (retry), anything else = lost.
    /// </summary>
    internal async Task<RenewResult> RenewAsync(LockLease lease, ulong revision)
    {
        var r = await _locks.PutAsync(lease.Key, lease.TokenBytes, NoHeaders, lease.Token.Lease, revision, CancellationToken.None).ConfigureAwait(false);
        if (r.Status == WriteStatus.Committed) return new(RenewStatus.Renewed, r.Seq, _time.GetUtcNow());

        var head = await _locks.ReadAsync(lease.Key, leader: true, CancellationToken.None).ConfigureAwait(false);
        if (head is null || !IsOurs(head, lease.TokenBytes)) return new(RenewStatus.Lost, revision, default);
        if (head.Revision != revision) return new(RenewStatus.Renewed, head.Revision, Earliest(head.Created));
        return new(RenewStatus.NotRenewed, revision, default);
    }

    /// <summary>
    /// Release: DEL marker expecting the owner's latest revision. A stale owner's release gets 10071 and is a no-op
    /// (spike-verified), unless the head is still our token at a newer revision (a renewal that landed late), which
    /// is released once more. An unknown outcome is settled by a leader read. Best effort within
    /// <see cref="ReleaseBudget"/>: a lock that is not released expires with its lease.
    /// </summary>
    internal async Task<bool> ReleaseAsync(LockLease lease, ulong revision)
    {
        using var budget = new CancellationTokenSource(ReleaseBudget);
        try
        {
            for (var attempt = 0; attempt < 2; attempt++)
            {
                var r = await _locks.DeleteAsync(lease.Key, revision, budget.Token).ConfigureAwait(false);
                if (r.Status == WriteStatus.Committed) return true;
                var head = await _locks.ReadAsync(lease.Key, leader: true, budget.Token).ConfigureAwait(false);
                if (head is null || (head.IsFree && head.Revision > revision)) return true;
                if (!IsOurs(head, lease.TokenBytes) || head.Revision == revision) return false;
                revision = head.Revision; // our late renewal is at the head: release that one
            }

            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Releasing lock {Key} failed; it expires with its lease", lease.Key);
            return false;
        }
    }

    private static bool IsOurs(L2Entry head, byte[] token) => head.Op == L2Op.Put && head.Payload.Span.SequenceEqual(token);

    private DateTimeOffset Earliest(DateTimeOffset serverTime)
    {
        var now = _time.GetUtcNow();
        return serverTime < now ? serverTime : now;
    }
}

/// <summary>
/// A held lock. Renews every LeaseTtl / 3 while the owner works (factory and fenced write), and stops renewing
/// FactoryTimeout + LeaseTtl after the acquire, so an owner whose factory ignores its timeout hands the lock over
/// through expiry instead of making waiters run without it. Disposing stops renewal, waits for an in-flight
/// renewal so the release expects the latest revision, then releases (unless the lease was lost or abandoned).
/// </summary>
internal sealed class LockLease : IAsyncDisposable
{
    private readonly DistributedLock _owner;
    private readonly CancellationTokenSource _stop = new();
    private readonly object _gate = new();
    private Task? _renewal;
    private ulong _revision;
    private DateTimeOffset _lastConfirmedAt;
    private int _released;
    private volatile bool _lost;
    private volatile bool _abandoned;
    private int _renewals;

    internal LockLease(DistributedLock owner, string key, LockToken token, byte[] tokenBytes, ulong revision, DateTimeOffset acquiredAt)
    {
        _owner = owner;
        Key = key;
        Token = token;
        TokenBytes = tokenBytes;
        _revision = revision;
        AcquiredAt = acquiredAt;
        _lastConfirmedAt = acquiredAt;
    }

    public string Key { get; }

    public LockToken Token { get; }

    internal byte[] TokenBytes { get; }

    public DateTimeOffset AcquiredAt { get; }

    /// <summary>Server-side start of the TTL that currently protects the lock.</summary>
    public DateTimeOffset LastConfirmedAt
    {
        get { lock (_gate) return _lastConfirmedAt; }
    }

    /// <summary>The revision of the owner's latest lock message (the last renewal's, not the acquire's).</summary>
    public ulong Revision
    {
        get { lock (_gate) return _revision; }
    }

    /// <summary>Set when a renewal found someone else's message, or the lease ran out without a confirmed renewal.</summary>
    public bool Lost => _lost;

    public int Renewals => Volatile.Read(ref _renewals);

    /// <summary>Starts the renewal loop.</summary>
    public LockLease StartRenewal()
    {
        lock (_gate) _renewal ??= Task.Run(RenewLoopAsync);
        return this;
    }

    /// <summary>Skips the release on dispose: NATS is unreachable, so the lock is left to expire with its lease.</summary>
    public void Abandon() => _abandoned = true;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _released, 1) == 1) return;
        _stop.Cancel();
        Task? renewal;
        lock (_gate) renewal = _renewal;
        if (renewal is not null)
        {
            try { await renewal.ConfigureAwait(false); }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _owner.Logger.LogWarning(ex, "Renewal of lock {Key} failed unexpectedly", Key);
            }
            catch (OperationCanceledException) { }
        }

        _stop.Dispose();
        if (!_lost && !_abandoned) await _owner.ReleaseAsync(this, Revision).ConfigureAwait(false);
    }

    private static TimeSpan RetryDelay(TimeSpan interval) =>
        TimeSpan.FromTicks(Math.Min(interval.Ticks, TimeSpan.FromMilliseconds(250).Ticks));

    private async Task RenewLoopAsync()
    {
        var interval = TimeSpan.FromTicks(Token.Lease.Ticks / 3);
        var time = _owner.Time;
        var stopAt = AcquiredAt + Token.RenewalCap;
        // The first renewal is due LeaseTtl / 3 after the lease started (its server time when settled late).
        var delay = interval - (time.GetUtcNow() - AcquiredAt);
        while (true)
        {
            if (delay > TimeSpan.Zero) await time.DelayAsync(delay, _stop.Token).ConfigureAwait(false);
            _stop.Token.ThrowIfCancellationRequested();
            var now = time.GetUtcNow();
            if (now >= stopAt)
            {
                _owner.Logger.LogWarning("Lock {Key} reached FactoryTimeout + LeaseTtl; renewal stops and the lease expires", Key);
                return;
            }

            delay = interval;
            try
            {
                var r = await _owner.RenewAsync(this, Revision).ConfigureAwait(false);
                switch (r.Status)
                {
                    case RenewStatus.Renewed:
                        lock (_gate)
                        {
                            _revision = r.Revision;
                            _lastConfirmedAt = r.ConfirmedAt;
                        }

                        Interlocked.Increment(ref _renewals);
                        continue;
                    case RenewStatus.Lost:
                        _lost = true;
                        _owner.Logger.LogWarning("Lock {Key} was lost during renewal; the fenced write protects the value", Key);
                        return;
                    default:
                        delay = RetryDelay(interval); // not extended: retry soon, the old TTL keeps running
                        break;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !_stop.IsCancellationRequested)
            {
                // Outage or unexpected error: keep trying while the last confirmed TTL still protects the lock.
                _owner.Logger.LogDebug(ex, "Renewing lock {Key} failed; retrying", Key);
                delay = RetryDelay(interval);
            }

            if (time.GetUtcNow() - LastConfirmedAt >= Token.Lease)
            {
                _lost = true;
                _owner.Logger.LogWarning("Lock {Key} expired before a renewal was confirmed", Key);
                return;
            }
        }
    }
}

internal static class TimeProviderExtensions
{
    public static Task DelayAsync(this TimeProvider time, TimeSpan delay, CancellationToken ct) =>
#if NET8_0_OR_GREATER
        Task.Delay(delay, time, ct);
#else
        time.Delay(delay, ct);
#endif
}
