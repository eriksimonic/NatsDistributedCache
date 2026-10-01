using System.Collections.Concurrent;
using Microsoft.Extensions.Caching.Memory;

namespace NatsDistributedCache.Internal;

/// <summary>
/// One L1 entry: its L2 revision, its jittered expiry and, when early refresh is on, the moment the value
/// enters the last part of its L2 life (design section 6). The value lives in <see cref="L1Item{T}"/>, so a
/// value type is stored unboxed. A class, not a record: the refresh claim is mutable state that must not take
/// part in equality.
/// </summary>
internal abstract class L1Item
{
    private int _refreshClaimed;

    protected L1Item(ulong revision, DateTimeOffset expiresAt, long size, DateTimeOffset? refreshAt)
    {
        Revision = revision;
        ExpiresAt = expiresAt;
        Size = size;
        RefreshAt = refreshAt;
    }

    public ulong Revision { get; }

    public DateTimeOffset ExpiresAt { get; }

    public long Size { get; }

    public DateTimeOffset? RefreshAt { get; }

    /// <summary>True for the first caller only, so a hot key starts at most one early refresh per L1 fill.</summary>
    public bool TryClaimRefresh() => Interlocked.Exchange(ref _refreshClaimed, 1) == 0;

    /// <summary>The value as <typeparamref name="TOut"/>: unboxed when the types match, else a type test.</summary>
    public abstract bool TryGetValue<TOut>(out TOut value);
}

internal sealed class L1Item<T> : L1Item
{
    public L1Item(T value, ulong revision, DateTimeOffset expiresAt, long size, DateTimeOffset? refreshAt = null)
        : base(revision, expiresAt, size, refreshAt) => Value = value;

    public T Value { get; }

    public override bool TryGetValue<TOut>(out TOut value)
    {
        if (this is L1Item<TOut> same)
        {
            value = same.Value;
            return true;
        }

        // A reader with another T for the same key (e.g. object); boxes, but only on this rare path.
        switch (Value)
        {
            case TOut cast:
                value = cast;
                return true;
            case null when default(TOut) is null:
                value = default!;
                return true;
            default:
                value = default!;
                return false;
        }
    }
}

/// <summary>
/// In-process L1 (design sections 4 and 7). Every write compares revisions, so an older value never
/// replaces a newer one, and a per-key revision floor (high-water revision) blocks refills older than a
/// delete or a newer event. The size limit counts serialized bytes.
/// </summary>
internal sealed class L1Store : IDisposable
{
    private const long FloorSize = 64;
    private static readonly string FloorPrefix = "\u0001floor:";

    private readonly MemoryCache _cache;
    private readonly ConcurrentDictionary<string, byte> _keys = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, (ulong Revision, DateTimeOffset Until)> _prefixFloors = new(StringComparer.Ordinal);
    private readonly object[] _stripes = Enumerable.Range(0, 64).Select(_ => new object()).ToArray();
    private readonly TimeProvider _time;

    public L1Store(long sizeLimitBytes, TimeProvider time)
    {
        _time = time;
        _cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = sizeLimitBytes });
    }

    public int Count => _keys.Count;

    public bool TryGet(string key, out L1Item item)
    {
        if (_cache.TryGetValue(key, out var raw) && raw is L1Item i && i.ExpiresAt > _time.GetUtcNow())
        {
            item = i;
            return true;
        }

        item = null!;
        return false;
    }

    /// <summary>The highest revision this node has seen deleted or superseded for the key (0 = none).</summary>
    public ulong Floor(string key)
    {
        var floor = _cache.TryGetValue(FloorPrefix + key, out var raw) && raw is ulong f ? f : 0UL;
        var now = _time.GetUtcNow();
        foreach (var p in _prefixFloors)
        {
            if (p.Value.Until <= now) { _prefixFloors.TryRemove(p.Key, out _); continue; }
            if (key.StartsWith(p.Key, StringComparison.Ordinal) && p.Value.Revision > floor) floor = p.Value.Revision;
        }

        return floor;
    }

    /// <summary>Stores the item unless L1 already holds a newer revision or the item is below the floor.</summary>
    public bool Set(string key, L1Item item, TimeSpan ttl)
    {
        if (ttl <= TimeSpan.Zero) return false;
        lock (Stripe(key))
        {
            if (item.Revision != 0 && item.Revision < Floor(key)) return false;
            if (_cache.TryGetValue(key, out var raw) && raw is L1Item existing && existing.Revision > item.Revision) return false;

            var entry = new MemoryCacheEntryOptions
            {
                Size = Math.Max(1, item.Size),
                AbsoluteExpirationRelativeToNow = ttl,
            };
            // A replaced entry fires the callback too; dropping the key then would hide the new entry from Clear().
            entry.RegisterPostEvictionCallback(static (k, value, reason, state) =>
            {
                if (reason != EvictionReason.Replaced) ((ConcurrentDictionary<string, byte>)state!).TryRemove((string)k, out _);
            }, _keys);
            _keys[key] = 0;
            _cache.Set(key, item, entry);
            return true;
        }
    }

    /// <summary>
    /// A <c>set</c> event (design section 7, rules 3 and HWM): evict the local copy only if it is older than
    /// <paramref name="revision"/>, and raise the key's high-water revision to it, so a lagging read of the old
    /// value cannot refill L1.
    /// </summary>
    public void Invalidate(string key, ulong revision, TimeSpan floorTtl)
    {
        lock (Stripe(key))
        {
            if (_cache.TryGetValue(key, out var raw) && raw is L1Item existing && existing.Revision < revision) _cache.Remove(key);
            SetFloor(key, revision, floorTtl);
        }
    }

    /// <summary>Raises the key's high-water revision to a revision this node has served (I5: no revision regression).</summary>
    public void RaiseFloor(string key, ulong revision, TimeSpan floorTtl)
    {
        if (revision == 0) return;
        lock (Stripe(key)) SetFloor(key, revision, floorTtl);
    }

    /// <summary>Evicts the key and raises its floor to <paramref name="floorRevision"/> for <paramref name="floorTtl"/>.</summary>
    public void Evict(string key, ulong floorRevision, TimeSpan floorTtl)
    {
        lock (Stripe(key))
        {
            _cache.Remove(key);
            SetFloor(key, floorRevision, floorTtl);
        }
    }

    private void SetFloor(string key, ulong revision, TimeSpan floorTtl)
    {
        if (revision == 0 || floorTtl <= TimeSpan.Zero) return;
        var own = _cache.TryGetValue(FloorPrefix + key, out var raw) && raw is ulong f ? f : 0UL;
        if (revision > own)
            _cache.Set(FloorPrefix + key, revision, new MemoryCacheEntryOptions { Size = FloorSize, AbsoluteExpirationRelativeToNow = floorTtl });
    }

    /// <summary>Evicts every key under <paramref name="prefix"/> and holds a prefix floor (tag delete path).</summary>
    public void EvictPrefix(string prefix, ulong floorRevision, TimeSpan floorTtl)
    {
        if (floorRevision > 0) _prefixFloors[prefix] = (floorRevision, _time.GetUtcNow() + floorTtl);
        foreach (var key in _keys.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList())
        {
            lock (Stripe(key)) _cache.Remove(key);
        }
    }

    public void Clear()
    {
        // Values only: per-key and prefix floors survive a flush and expire on their own. A tag-deleted key has no per-key
        // floor on other nodes, so its prefix floor is all that stops a lagging replica refilling it (review 8, I5).
        foreach (var key in _keys.Keys.ToList()) _cache.Remove(key);
    }

    public void Dispose() => _cache.Dispose();

    private object Stripe(string key) => _stripes[(key.GetHashCode() & int.MaxValue) % _stripes.Length];
}
