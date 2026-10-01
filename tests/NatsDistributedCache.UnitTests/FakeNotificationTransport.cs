using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

/// <summary>
/// In-memory notifications stream with the behaviour design section 7 relies on: a global sequence, ordered delivery
/// to every subscribed consumer, start-by-sequence that silently skips to FirstSeq when asked for discarded messages
/// (spike-verified), discards, recreation, and connection drops followed by a reconnect callback.
/// </summary>
internal sealed class FakeNotificationTransport : INotificationTransport
{
    private readonly object _gate = new();
    private readonly List<(ulong Seq, string Subject, byte[] Data)> _messages = [];
    private readonly List<Channel<DeliveredEvent>> _subscribers = [];
    private ulong _seq;
    private ulong _first = 1;
    private int _skipDeliveries;

    public DateTimeOffset Created { get; private set; } = DateTimeOffset.UnixEpoch.AddDays(1);

    /// <summary>Publishes throw (NATS unreachable).</summary>
    public bool Unavailable { get; set; }

    public event Action? Reconnected;

    /// <summary>Called synchronously with the subject before each publish is stored (inspect state at publish time).</summary>
    public Action<string>? OnPublish { get; set; }

    public IReadOnlyList<CacheEvent> Published
    {
        get
        {
            lock (_gate) return _messages.Select(m => CacheEvent.Decode(m.Data)).Where(e => e is not null).Select(e => e!).ToList();
        }
    }

    public int Subscribers
    {
        get { lock (_gate) return _subscribers.Count; }
    }

    public ValueTask PublishAsync(string subject, byte[] data, string msgId, CancellationToken ct)
    {
        if (Unavailable) throw new L2UnavailableException("fake notifications outage");
        OnPublish?.Invoke(subject);
        Append(subject, data);
        return default;
    }

    /// <summary>Test helper: publishes raw bytes as if another node had.</summary>
    public void Append(string subject, byte[] data)
    {
        lock (_gate)
        {
            var seq = ++_seq;
            _messages.Add((seq, subject, data));
            if (_skipDeliveries > 0)
            {
                _skipDeliveries--; // stored, but the live consumers never see it: a gap
                return;
            }

            foreach (var c in _subscribers) c.Writer.TryWrite(new DeliveredEvent(seq, data));
        }
    }

    /// <summary>The next <paramref name="n"/> messages are stored but not delivered to the current consumers.</summary>
    public void SkipDeliveries(int n)
    {
        lock (_gate) _skipDeliveries = n;
    }

    /// <summary>MaxAge / MaxMsgs discard: messages up to <paramref name="seq"/> are gone.</summary>
    public void DiscardUpTo(ulong seq)
    {
        lock (_gate)
        {
            _messages.RemoveAll(m => m.Seq <= seq);
            _first = seq + 1;
        }
    }

    /// <summary>The stream is deleted and created again: sequences restart and consumers die.</summary>
    public void Recreate()
    {
        lock (_gate)
        {
            _messages.Clear();
            _seq = 0;
            _first = 1;
            Created = Created.AddHours(1);
            DropSubscribers();
        }
    }

    /// <summary>The connection drops: every consumer fails.</summary>
    public void Disconnect()
    {
        lock (_gate) DropSubscribers();
    }

    public void RaiseReconnected() => Reconnected?.Invoke();

    public async IAsyncEnumerable<DeliveredEvent> ConsumeAsync(ulong? startSeq, [EnumeratorCancellation] CancellationToken ct)
    {
        if (Unavailable) throw new L2UnavailableException("fake notifications outage");
        var channel = Channel.CreateUnbounded<DeliveredEvent>(new UnboundedChannelOptions { SingleReader = true });
        lock (_gate)
        {
            if (startSeq is { } start)
            {
                foreach (var m in _messages.Where(m => m.Seq >= Math.Max(start, _first))) channel.Writer.TryWrite(new DeliveredEvent(m.Seq, m.Data));
            }

            _subscribers.Add(channel);
        }

        try
        {
            yield return DeliveredEvent.Subscribed;
            await foreach (var e in channel.Reader.ReadAllAsync(ct)) yield return e;
        }
        finally
        {
            lock (_gate) _subscribers.Remove(channel);
        }
    }

    public ValueTask<StreamPosition> PositionAsync(CancellationToken ct)
    {
        if (Unavailable) throw new L2UnavailableException("fake notifications outage");
        lock (_gate) return new(new StreamPosition(Created, _first, _seq));
    }

    private void DropSubscribers()
    {
        foreach (var c in _subscribers) c.Writer.TryComplete(new InvalidOperationException("consumer deleted"));
        _subscribers.Clear();
    }
}
