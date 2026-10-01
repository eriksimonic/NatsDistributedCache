using System.Collections.Concurrent;

namespace NatsDistributedCache.Internal;

/// <summary>
/// Keys (and tag prefixes) this node wrote L1-only while NATS was unreachable in Open mode (design Q2, section 8
/// recovery step 2), each with the time of its last outage write. On recovery every journaled key is re-read
/// through the leader and deleted at that revision when the L2 entry predates the outage write (minus a 1 s skew
/// margin), so no node keeps serving the pre-outage value. The latest time is the right fence (review 8): an entry
/// another node wrote between two of this node's outage writes is older than the last one, and deleting a cache
/// entry is always safe.
/// </summary>
internal sealed class OutageJournal
{
    private readonly int _capacity;
    private ConcurrentDictionary<string, DateTimeOffset> _keys = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, DateTimeOffset> _prefixes = new(StringComparer.Ordinal);
    private int _overflowed;

    public OutageJournal(int capacity) => _capacity = capacity;

    public bool IsEmpty => _keys.IsEmpty && _prefixes.IsEmpty;

    /// <summary>Set when a write could not be journaled because the journal was full.</summary>
    public bool Overflowed => Volatile.Read(ref _overflowed) == 1;

    public void RecordKey(string internalKey, DateTimeOffset at) => Record(_keys, internalKey, at);

    public void RecordPrefix(string prefix, DateTimeOffset at) => Record(_prefixes, prefix, at);

    /// <summary>Takes everything journaled so far; writes after this go into a fresh journal.</summary>
    public (IReadOnlyDictionary<string, DateTimeOffset> Keys, IReadOnlyDictionary<string, DateTimeOffset> Prefixes, bool Overflowed) Drain()
    {
        var keys = Interlocked.Exchange(ref _keys, new ConcurrentDictionary<string, DateTimeOffset>(StringComparer.Ordinal));
        var prefixes = Interlocked.Exchange(ref _prefixes, new ConcurrentDictionary<string, DateTimeOffset>(StringComparer.Ordinal));
        var overflowed = Interlocked.Exchange(ref _overflowed, 0) == 1;
        return (keys, prefixes, overflowed);
    }

    /// <summary>Puts entries back after a replay that failed, keeping the latest write time.</summary>
    public void Restore(IEnumerable<KeyValuePair<string, DateTimeOffset>> keys, IEnumerable<KeyValuePair<string, DateTimeOffset>> prefixes)
    {
        foreach (var k in keys) Record(_keys, k.Key, k.Value);
        foreach (var p in prefixes) Record(_prefixes, p.Key, p.Value);
    }

    private void Record(ConcurrentDictionary<string, DateTimeOffset> map, string key, DateTimeOffset at)
    {
        if (map.Count >= _capacity && !map.ContainsKey(key))
        {
            Interlocked.Exchange(ref _overflowed, 1);
            return;
        }

        map.AddOrUpdate(key, at, (_, existing) => existing > at ? existing : at);
    }
}
