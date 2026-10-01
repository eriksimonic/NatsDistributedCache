using System.Collections.Concurrent;
using Microsoft.Extensions.Caching.Memory;

namespace NatsDistributedCache.Internal;

/// <summary>One L1 entry: the deserialized value, its L2 revision and its jittered expiry.</summary>
internal sealed record L1Item(object? Value, ulong Revision, DateTimeOffset ExpiresAt, long Size);

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
            entry.RegisterPostEvictionCallback(static (k, _, _, state) => ((ConcurrentDictionary<string, byte>)state!).TryRemove((string)k, out _), _keys);
            _keys[key] = 0;
            _cache.Set(key, item, entry);
            return true;
        }
    }

    /// <summary>Evicts the key and raises its floor to <paramref name="floorRevision"/> for <paramref name="floorTtl"/>.</summary>
    public void Evict(string key, ulong floorRevision, TimeSpan floorTtl)
    {
        lock (Stripe(key))
        {
            _cache.Remove(key);
            if (floorRevision > 0 && floorTtl > TimeSpan.Zero && floorRevision > Floor(key))
                _cache.Set(FloorPrefix + key, floorRevision, new MemoryCacheEntryOptions { Size = FloorSize, AbsoluteExpirationRelativeToNow = floorTtl });
        }
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
        foreach (var key in _keys.Keys.ToList()) _cache.Remove(key);
        _prefixFloors.Clear();
    }

    public void Dispose() => _cache.Dispose();

    private object Stripe(string key) => _stripes[(key.GetHashCode() & int.MaxValue) % _stripes.Length];
}
