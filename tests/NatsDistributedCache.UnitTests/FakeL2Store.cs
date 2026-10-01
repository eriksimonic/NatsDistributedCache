using System.Runtime.CompilerServices;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

/// <summary>
/// In-memory KV bucket with the semantics the spike verified on NATS 2.15.0: one message per subject
/// (History 1), a global sequence, expected-last-subject-sequence fencing (10071 naming the last sequence),
/// msg-id dedup, per-key TTL that turns into a MaxAge marker which itself expires, DEL tombstones, and
/// optionally stale Direct Gets.
/// </summary>
internal sealed class FakeL2Store : IL2Store
{
    private readonly object _gate = new();
    private readonly Dictionary<string, Msg> _subjects = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Msg> _previous = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ulong> _msgIds = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;
    private ulong _seq;

    public FakeL2Store(TimeProvider time, TimeSpan? markerTtl = null)
    {
        _time = time;
        MarkerTtl = markerTtl ?? TimeSpan.FromSeconds(30);
    }

    public TimeSpan MarkerTtl { get; }

    /// <summary>Every call throws <see cref="L2UnavailableException"/>.</summary>
    public bool Unavailable { get; set; }

    /// <summary>Direct Gets return the subject's previous message (a lagging replica).</summary>
    public bool StaleDirectReads { get; set; }

    /// <summary>Every write is rejected with this API error code (e.g. 10059 stream not found); 0 = off.</summary>
    public int RejectWritesWith { get; set; }

    /// <summary>The next put is not stored and reports Unknown (a send that never reached the leader).</summary>
    public bool NextPutUnknownNotApplied { get; set; }

    /// <summary>The next delete commits but reports Unknown (lost reply).</summary>
    public bool NextDeleteLosesReply { get; set; }

    /// <summary>The next put commits but reports Unknown (lost reply).</summary>
    public bool NextPutLosesReply { get; set; }

    /// <summary>Runs before each put is applied; lets a test inject a concurrent write.</summary>
    public Func<string, Task>? BeforePut { get; set; }

    public int DirectReads { get; private set; }
    public int LeaderReads { get; private set; }

    /// <summary>Calls of LastSequenceAsync, which is also the recovery probe; counted even while unavailable.</summary>
    public int LastSequenceCalls { get; private set; }
    public List<(string Key, ulong? Expected, WriteResult Result, IReadOnlyDictionary<string, string> Headers, TimeSpan NatsTtl)> Puts { get; } = [];

    public ValueTask<L2Entry?> ReadAsync(string key, bool leader, CancellationToken ct)
    {
        ThrowIfUnavailable();
        lock (_gate)
        {
            if (leader) LeaderReads++;
            else DirectReads++;
            Expire(key);
            if (!leader && StaleDirectReads && _previous.TryGetValue(key, out var prev)) return new(prev.ToEntry(key));
            return new(_subjects.TryGetValue(key, out var m) ? m.ToEntry(key) : null);
        }
    }

    public async ValueTask<WriteResult> PutAsync(string key, byte[] payload, IReadOnlyDictionary<string, string> headers, TimeSpan natsTtl, ulong? expected, CancellationToken ct)
    {
        ThrowIfUnavailable();
        if (BeforePut is { } hook)
        {
            BeforePut = null; // one-shot, so the hook's own writes are not intercepted
            await hook(key);
        }

        var h = new Dictionary<string, string>(headers, StringComparer.OrdinalIgnoreCase);
        if (NextPutUnknownNotApplied)
        {
            NextPutUnknownNotApplied = false;
            var lost = new WriteResult(WriteStatus.Unknown, 0, 0, "NatsJSPublishNoResponseException", Guid.NewGuid().ToString("N"));
            lock (_gate) Puts.Add((key, expected, lost, h, natsTtl));
            return lost;
        }

        var result = Append(key, payload, h, natsTtl, expected, L2Op.Put);
        lock (_gate) Puts.Add((key, expected, result, h, natsTtl));
        if (NextPutLosesReply && result.Status == WriteStatus.Committed)
        {
            NextPutLosesReply = false;
            return result with { Status = WriteStatus.Unknown, Seq = 0, Error = "NatsJSPublishNoResponseException" };
        }

        return result;
    }

    public ValueTask<WriteResult> DeleteAsync(string key, ulong? expected, CancellationToken ct)
    {
        ThrowIfUnavailable();
        var h = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["KV-Operation"] = "DEL" };
        var result = Append(key, [], h, null, expected, L2Op.Delete);
        if (NextDeleteLosesReply && result.Status == WriteStatus.Committed)
        {
            NextDeleteLosesReply = false;
            return new(result with { Status = WriteStatus.Unknown, Seq = 0, Error = "NatsJSPublishNoResponseException" });
        }

        return new(result);
    }

    public async IAsyncEnumerable<string> ListKeysAsync(string filter, [EnumeratorCancellation] CancellationToken ct)
    {
        ThrowIfUnavailable();
        var prefix = filter.EndsWith(".>", StringComparison.Ordinal) ? filter.Substring(0, filter.Length - 1) : filter;
        List<string> keys;
        lock (_gate)
        {
            foreach (var k in _subjects.Keys.ToList()) Expire(k);
            keys = _subjects.Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal) && p.Value.Op == L2Op.Put).Select(p => p.Key).ToList();
        }

        foreach (var k in keys)
        {
            await Task.Yield();
            yield return k;
        }
    }

    public ValueTask<ulong> LastSequenceAsync(CancellationToken ct)
    {
        lock (_gate) LastSequenceCalls++;
        ThrowIfUnavailable();
        lock (_gate) return new(_seq);
    }

    /// <summary>Test helper: a copy of <see cref="Puts"/> taken under the store's lock (background renewals append to it).</summary>
    public List<(string Key, ulong? Expected, WriteResult Result, IReadOnlyDictionary<string, string> Headers, TimeSpan NatsTtl)> PutsSnapshot()
    {
        lock (_gate) return [.. Puts];
    }

    /// <summary>Test helper: the current message for a key, after TTL processing.</summary>
    public L2Entry? Peek(string key)
    {
        lock (_gate)
        {
            Expire(key);
            return _subjects.TryGetValue(key, out var m) ? m.ToEntry(key) : null;
        }
    }

    private WriteResult Append(string key, byte[] payload, Dictionary<string, string> headers, TimeSpan? ttl, ulong? expected, L2Op op)
    {
        lock (_gate)
        {
            if (RejectWritesWith != 0) return new(WriteStatus.Rejected, 0, RejectWritesWith, "rejected by test", "");
            Expire(key);
            headers.TryGetValue("Nats-Msg-Id", out var msgId);
            msgId ??= Guid.NewGuid().ToString("N");
            headers["Nats-Msg-Id"] = msgId;
            if (_msgIds.TryGetValue(msgId, out var dup)) return new(WriteStatus.Committed, dup, 0, null, msgId);

            var last = _subjects.TryGetValue(key, out var cur) ? cur.Seq : 0UL;
            if (expected is { } e && e != last)
                return new(WriteStatus.Rejected, 0, WriteResult.WrongLastSequence, $"wrong last sequence: {last}", msgId);

            var msg = new Msg(++_seq, _time.GetUtcNow(), op, payload, headers, ttl);
            if (cur is not null) _previous[key] = cur;
            _subjects[key] = msg;
            _msgIds[msgId] = msg.Seq;
            return new(WriteStatus.Committed, msg.Seq, 0, null, msgId);
        }
    }

    private void Expire(string key)
    {
        if (!_subjects.TryGetValue(key, out var m)) return;
        var now = _time.GetUtcNow();
        if (m.Op == L2Op.Put && m.Ttl is { } ttl && m.Created + ttl <= now)
        {
            var markerAt = m.Created + ttl;
            var marker = new Msg(++_seq, markerAt, L2Op.Marker, [],
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["Nats-Marker-Reason"] = "MaxAge" }, MarkerTtl);
            _previous[key] = m;
            _subjects[key] = marker;
            m = marker;
        }

        if (m.Op == L2Op.Marker && m.Created + MarkerTtl <= now) _subjects.Remove(key);
    }

    private void ThrowIfUnavailable()
    {
        if (Unavailable) throw new L2UnavailableException("fake outage");
    }

    private sealed record Msg(ulong Seq, DateTimeOffset Created, L2Op Op, byte[] Payload, Dictionary<string, string> Headers, TimeSpan? Ttl)
    {
        public L2Entry ToEntry(string key) => new(key, Seq, Created, Op, Payload, Headers);
    }
}

/// <summary>Deterministic jitter: always returns the same u in [0, 1).</summary>
internal sealed class FixedRandom(double u) : IRandomSource
{
    public double NextDouble() => u;
}
