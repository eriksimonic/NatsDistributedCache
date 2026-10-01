using System.Buffers;
using System.Collections.Concurrent;
using System.Text.Json;

namespace NatsDistributedCache.Internal;

/// <summary>Invalidation event kinds (design section 7).</summary>
internal enum EventOp
{
    Set,
    Del,
    Fail,
    Clear,
    Tag,
}

/// <summary>
/// One invalidation event: <c>{"op","key","rev","node","ts"}</c> on <c>{prefix}.notify.{internalKey}</c>. For
/// <c>tag</c> the key is the user-key prefix, for <c>clear</c> it is <c>*</c> on <c>{prefix}.notify._clear</c>.
/// </summary>
internal sealed record CacheEvent(EventOp Op, string Key, ulong Rev, string Node, long Ts)
{
    private static readonly JsonWriterOptions WriterOptions = new() { Indented = false };

    public byte[] Encode()
    {
        var buffer = new ArrayBufferWriter<byte>(128);
        using (var w = new Utf8JsonWriter(buffer, WriterOptions))
        {
            w.WriteStartObject();
            w.WriteString("op", OpName(Op));
            w.WriteString("key", Key);
            w.WriteNumber("rev", Rev);
            w.WriteString("node", Node);
            w.WriteNumber("ts", Ts);
            w.WriteEndObject();
        }

        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Null for anything that is not a well-formed event (unknown op, missing key), which is skipped.</summary>
    public static CacheEvent? Decode(ReadOnlySpan<byte> data)
    {
        try
        {
            var r = new Utf8JsonReader(data);
            string? op = null, key = null, node = null;
            ulong rev = 0;
            long ts = 0;
            if (!r.Read() || r.TokenType != JsonTokenType.StartObject) return null;
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                var name = r.GetString();
                if (!r.Read()) return null;
                switch (name)
                {
                    case "op": op = r.GetString(); break;
                    case "key": key = r.GetString(); break;
                    case "node": node = r.GetString(); break;
                    case "rev": rev = r.GetUInt64(); break;
                    case "ts": ts = r.GetInt64(); break;
                    default: r.Skip(); break;
                }
            }

            if (key is null || ParseOp(op) is not { } parsed) return null;
            return new CacheEvent(parsed, key, rev, node ?? "", ts);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
        {
            return null;
        }
    }

    private static string OpName(EventOp op) => op switch
    {
        EventOp.Set => "set",
        EventOp.Del => "del",
        EventOp.Fail => "fail",
        EventOp.Clear => "clear",
        _ => "tag",
    };

    private static EventOp? ParseOp(string? op) => op switch
    {
        "set" => EventOp.Set,
        "del" => EventOp.Del,
        "fail" => EventOp.Fail,
        "clear" => EventOp.Clear,
        "tag" => EventOp.Tag,
        _ => null,
    };
}

/// <summary>A delivered event message: its stream sequence and payload. Sequence 0 = the consumer is subscribed.</summary>
internal readonly record struct DeliveredEvent(ulong StreamSeq, ReadOnlyMemory<byte> Data)
{
    public static readonly DeliveredEvent Subscribed = new(0, ReadOnlyMemory<byte>.Empty);
}

/// <summary>What <c>StreamInfo</c> says about the notifications stream (design section 7, rule 7).</summary>
internal readonly record struct StreamPosition(DateTimeOffset Created, ulong FirstSeq, ulong LastSeq);

/// <summary>The notifications stream as the cache sees it. Implemented over NATS and faked in unit tests.</summary>
internal interface INotificationTransport
{
    /// <summary>Publishes an event; best effort, the caller logs failures (L1 TTL bounds staleness).</summary>
    ValueTask PublishAsync(string subject, byte[] data, string msgId, CancellationToken ct);

    /// <summary>
    /// An ordered consumer over every notify subject: from the next new message when <paramref name="startSeq"/> is
    /// null, otherwise from that stream sequence. Yields <see cref="DeliveredEvent.Subscribed"/> first, once the
    /// consumer exists. Ends (or throws) when the connection drops.
    /// </summary>
    IAsyncEnumerable<DeliveredEvent> ConsumeAsync(ulong? startSeq, CancellationToken ct);

    ValueTask<StreamPosition> PositionAsync(CancellationToken ct);

    /// <summary>Raised after the connection is re-established; the consumer then runs the gap check.</summary>
    event Action? Reconnected;
}

/// <summary>
/// Local waiter table (design section 5, waiter step 1): lock waiters on this node park on a per-key signal that
/// the notifications consumer pulses on the key's set, del or fail event. No NATS consumer per waiter.
/// </summary>
internal sealed class KeySignals
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _signals = new(StringComparer.Ordinal);

    public int Count => _signals.Count;

    /// <summary>A task that completes on the next pulse for <paramref name="key"/>. Take it before reading state.</summary>
    public Task Next(string key) =>
        _signals.GetOrAdd(key, static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

    public void Pulse(string key)
    {
        if (_signals.TryRemove(key, out var tcs)) tcs.TrySetResult(true);
    }

    public void PulsePrefix(string prefix)
    {
        foreach (var key in _signals.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal)) Pulse(key);
        }
    }

    public void PulseAll()
    {
        foreach (var key in _signals.Keys) Pulse(key);
    }

    /// <summary>Drops the signal for a key no one waits on any more, so the table does not grow.</summary>
    public void Forget(string key, Task signal)
    {
        if (_signals.TryGetValue(key, out var tcs) && tcs.Task == signal && !signal.IsCompleted)
            ((ICollection<KeyValuePair<string, TaskCompletionSource<bool>>>)_signals).Remove(new(key, tcs));
    }
}

internal static class NotifySubjects
{
    public static string ForKey(string prefix, string internalKey) => $"{prefix}.notify.{internalKey}";

    public static string ForTag(string prefix, string tagPrefix) => $"{prefix}.notify.{tagPrefix}";

    public static string Clear(string prefix) => $"{prefix}.notify._clear";
}
