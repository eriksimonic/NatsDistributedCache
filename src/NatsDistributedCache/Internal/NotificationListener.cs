using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace NatsDistributedCache.Internal;

/// <summary>The notifications stream over NATS JetStream (design section 7).</summary>
internal sealed class NatsNotificationTransport : INotificationTransport
{
    private const int MaxSends = 3;

    private readonly INatsJSContext _js;
    private readonly string _stream;
    private readonly string _filter;

    public NatsNotificationTransport(INatsJSContext js, StoreNames names)
    {
        _js = js;
        _stream = names.Notifications;
        _filter = names.NotifySubjects;
        js.Connection.ConnectionOpened += OnOpened;
    }

    public event Action? Reconnected;

    public async ValueTask PublishAsync(string subject, byte[] data, string msgId, CancellationToken ct)
    {
        var headers = new NatsHeaders { ["Nats-Msg-Id"] = msgId };
        for (var send = 1; ; send++)
        {
            try
            {
                var ack = await _js.PublishAsync(subject, data, headers: headers, cancellationToken: ct).ConfigureAwait(false);
                if (!ack.Duplicate) ack.EnsureSuccess();
                return;
            }
            catch (NatsJSDuplicateMessageException)
            {
                return; // an earlier send of this event was stored
            }
            catch (Exception ex) when (NatsL2Store.IsTransport(ex) && send < MaxSends)
            {
                // Lost reply or timeout: resend with the same msg id; a duplicate event is harmless anyway.
                await Task.Delay(100 * send, ct).ConfigureAwait(false);
            }
        }
    }

    public async IAsyncEnumerable<DeliveredEvent> ConsumeAsync(ulong? startSeq, [EnumeratorCancellation] CancellationToken ct)
    {
        var opts = new NatsJSOrderedConsumerOpts
        {
            FilterSubjects = [_filter],
            DeliverPolicy = startSeq is null ? ConsumerConfigDeliverPolicy.New : ConsumerConfigDeliverPolicy.ByStartSequence,
            OptStartSeq = startSeq ?? 0,
            MaxResetAttempts = int.MaxValue,
        };
        var consumer = await _js.CreateOrderedConsumerAsync(_stream, opts, ct).ConfigureAwait(false);
        yield return DeliveredEvent.Subscribed;
        await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct).ConfigureAwait(false))
        {
            if (msg.Metadata is not { } meta) continue;
            yield return new DeliveredEvent(meta.Sequence.Stream, msg.Data ?? []);
        }
    }

    public async ValueTask<StreamPosition> PositionAsync(CancellationToken ct)
    {
        var info = (await _js.GetStreamAsync(_stream, cancellationToken: ct).ConfigureAwait(false)).Info;
        return new StreamPosition(info.Created, info.State.FirstSeq, info.State.LastSeq);
    }

    private ValueTask OnOpened(object? sender, NatsEventArgs args)
    {
        Reconnected?.Invoke();
        return default;
    }
}

/// <summary>
/// One ordered consumer per node on <c>{prefix}.notify.&gt;</c> (design section 7, consumer rules). It starts at
/// "new" on first boot and tracks the last processed stream sequence. After a reconnect, and whenever the delivered
/// sequence jumps by more than 1, it reads StreamInfo: a recreated stream, <c>FirstSeq &gt; lastSeen + 1</c> or
/// <c>LastSeq &lt; lastSeen</c> means events were lost (MaxAge / MaxMsgs discard, or an ordered consumer that silently
/// skipped ahead), so it flushes L1 and resumes after the stream's current last sequence; otherwise it resumes from
/// <c>lastSeen + 1</c>.
/// </summary>
internal sealed class NotificationListener : IAsyncDisposable
{
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(2);

    private readonly INotificationTransport _transport;
    private readonly Action<CacheEvent> _handle;
    private readonly Action<string> _flush;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stop = new();
    private readonly TaskCompletionSource<bool> _live = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private CancellationTokenSource? _current;
    private Task? _loop;
    private ulong? _lastSeen;
    private DateTimeOffset? _created;
    private volatile bool _checkPending;

    public NotificationListener(INotificationTransport transport, Action<CacheEvent> handle, Action<string> flush, ILogger logger)
    {
        _transport = transport;
        _handle = handle;
        _flush = flush;
        _logger = logger;
        _transport.Reconnected += OnReconnected;
    }

    /// <summary>Completes once the first consumer is subscribed (events published after this are seen).</summary>
    public Task Live => _live.Task;

    public ulong? LastSeen => _lastSeen;

    public long Gaps { get; private set; }

    public void Start() => _loop ??= Task.Run(() => RunAsync(_stop.Token));

    public async ValueTask DisposeAsync()
    {
        _transport.Reconnected -= OnReconnected;
        _stop.Cancel();
        _current?.Cancel();
        if (_loop is { } loop)
        {
            try { await loop.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }

        _stop.Dispose();
    }

    private void OnReconnected()
    {
        if (_lastSeen is null) return; // nothing processed yet: nothing to resume
        _checkPending = true;
        try { _current?.Cancel(); } // restart the consumer through the gap check
        catch (ObjectDisposedException) { }
    }

    private async Task RunAsync(CancellationToken stop)
    {
        var backoff = TimeSpan.FromMilliseconds(100);
        while (!stop.IsCancellationRequested)
        {
            using var current = CancellationTokenSource.CreateLinkedTokenSource(stop);
            _current = current;
            try
            {
                _created ??= (await _transport.PositionAsync(current.Token).ConfigureAwait(false)).Created;
                if (_checkPending)
                {
                    _checkPending = false;
                    await CheckGapAsync(current.Token).ConfigureAwait(false);
                }

                var start = _lastSeen is { } seen ? seen + 1 : (ulong?)null;
                await foreach (var msg in _transport.ConsumeAsync(start, current.Token).ConfigureAwait(false))
                {
                    if (msg.StreamSeq == 0)
                    {
                        _live.TrySetResult(true); // subscribed
                        continue;
                    }

                    backoff = TimeSpan.FromMilliseconds(100);
                    if (_lastSeen is { } last)
                    {
                        if (msg.StreamSeq <= last) continue; // redelivery after a consumer reset
                        if (msg.StreamSeq > last + 1)
                        {
                            Gaps++;
                            _checkPending = true;
                            break; // rule 7: a jump means events may be missing; check, then resume or flush
                        }
                    }

                    Dispatch(msg);
                    _lastSeen = msg.StreamSeq;
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (OperationCanceledException)
            {
                // a reconnect cancelled the current consumer; loop runs the gap check
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Notifications consumer failed; retrying in {Backoff}", backoff);
                _checkPending = _lastSeen is not null;
                try { await Task.Delay(backoff, stop).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, MaxBackoff.Ticks));
            }
            finally
            {
                _current = null;
            }
        }
    }

    private async Task CheckGapAsync(CancellationToken ct)
    {
        if (_lastSeen is not { } last) return;
        var pos = await _transport.PositionAsync(ct).ConfigureAwait(false);
        string? reason = null;
        if (_created is { } created && pos.Created != created) reason = "the notifications stream was recreated";
        else if (pos.FirstSeq > last + 1) reason = $"events up to {pos.FirstSeq - 1} were discarded (last processed {last})";
        else if (pos.LastSeq < last) reason = $"the stream's last sequence {pos.LastSeq} is below the last processed {last}";

        if (reason is null) return; // nothing lost: resume from lastSeen + 1
        _logger.LogWarning("Invalidation events were lost ({Reason}); flushing L1 and resuming after the stream's last sequence", reason);
        _flush(reason);
        _created = pos.Created;
        // Everything up to LastSeq describes writes made before the flush, so nothing in the fresh L1 depends on it.
        // Resuming right after it (rather than from "new") leaves no window between the flush and the resubscribe.
        _lastSeen = pos.LastSeq;
    }

    private void Dispatch(DeliveredEvent msg)
    {
        var e = CacheEvent.Decode(msg.Data.Span);
        if (e is null)
        {
            _logger.LogDebug("Skipping malformed notification at stream sequence {Seq}", msg.StreamSeq);
            return;
        }

        try
        {
            _handle(e);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Handling notification {Op} {Key} failed", e.Op, e.Key);
        }
    }
}
