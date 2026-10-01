using System.Text;
using Microsoft.Extensions.Time.Testing;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

/// <summary>Cross-node invalidation, waiter wake-ups, outcomes, gap handling and outage recovery (design sections 7 and 8).</summary>
public sealed class InvalidationTests : IAsyncDisposable
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeL2Store _l2;
    private readonly FakeL2Store _locks;
    private readonly FakeNotificationTransport _notify = new();
    private readonly List<NatsCache> _caches = [];
    private readonly List<FactoryCompletion> _completions = [];

    public InvalidationTests()
    {
        _l2 = new FakeL2Store(_time);
        _locks = new FakeL2Store(_time);
    }

    private async Task<NatsCache> Node(string node, Action<NatsCacheOptions>? configure = null)
    {
        var o = new NatsCacheOptions
        {
            Prefix = "test",
            NodeId = node,
            OnFactoryCompleted = c => { lock (_completions) _completions.Add(c); },
        };
        configure?.Invoke(o);
        var cache = new NatsCache(o, _l2, _locks, _notify, null, null, _time, null, new FixedRandom(0.5));
        _caches.Add(cache);
        cache.Start();
        await DistributedLockTests.Eventually(() => cache.IsReady, $"{node} ready");
        await cache.NotificationsLive.WaitAsync(TimeSpan.FromSeconds(5));
        return cache;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _caches) await c.DisposeAsync();
    }

    private static Func<FactoryContext, CancellationToken, ValueTask<T>> Factory<T>(T value) => (_, _) => new ValueTask<T>(value);

    private static Task Eventually(Func<bool> condition, string what) => DistributedLockTests.Eventually(condition, what);

    /// <summary>Advances the clock a second at a time until <paramref name="done"/>; the recovery loop may arm its probe late.</summary>
    private async Task AdvanceUntil(Func<bool> done, string what)
    {
        for (var i = 0; i < 50 && !done(); i++)
        {
            _time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(20);
        }

        Assert.True(done(), what);
    }

    /// <summary>True when the node serves <paramref name="expected"/> from its own L1 (no L2 read).</summary>
    private bool ServesFromL1<T>(NatsCache cache, string key, T expected)
    {
        var reads = _l2.DirectReads + _l2.LeaderReads;
        var r = cache.TryGetAsync<T>(key).AsTask().GetAwaiter().GetResult();
        return r.Found && Equals(r.Value, expected) && _l2.DirectReads + _l2.LeaderReads == reads;
    }

    [Fact]
    public void Event_round_trips_and_malformed_payloads_are_rejected()
    {
        var e = new CacheEvent(EventOp.Tag, "orders.42", 1874, "api-3", 1_790_000_000_123);
        Assert.Equal("""{"op":"tag","key":"orders.42","rev":1874,"node":"api-3","ts":1790000000123}""", Encoding.UTF8.GetString(e.Encode()));
        Assert.Equal(e, CacheEvent.Decode(e.Encode()));
        Assert.Null(CacheEvent.Decode("""{"op":"nope","key":"k"}"""u8));
        Assert.Null(CacheEvent.Decode("""{"op":"set"}"""u8));
        Assert.Null(CacheEvent.Decode("not json"u8));
        Assert.Null(CacheEvent.Decode(""u8));                                 // empty: no first token
        Assert.Null(CacheEvent.Decode("""{"op":"set","key":"k","rev":"""u8));  // truncated: JsonException
        Assert.Null(CacheEvent.Decode("""{"op":"set","key":"k","rev":"abc"}"""u8)); // wrong type: InvalidOperationException
        Assert.Null(CacheEvent.Decode("""{"op":"set","key":"k","rev":-1}"""u8));    // out of range: FormatException
        Assert.Null(CacheEvent.Decode("""{"op":{"x":1},"key":"k"}"""u8));          // object where a string belongs
    }

    [Fact]
    public async Task Set_on_one_node_evicts_the_older_copy_on_another()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        Assert.Equal("v1", (await b.TryGetAsync<string>("orders.1")).Value);
        Assert.True(ServesFromL1(b, "orders.1", "v1"));

        await a.SetAsync("orders.1", "v2");

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "b evicted v1");
        Assert.Equal("v2", (await b.TryGetAsync<string>("orders.1")).Value);
        var set = Assert.Single(_notify.Published, p => p.Op == EventOp.Set && p.Rev == _l2.Peek("orders.1._s1")!.Revision);
        Assert.Equal("orders.1._s1", set.Key);
        Assert.Equal("a", set.Node);
    }

    [Fact]
    public async Task Own_and_late_events_never_evict_a_newer_copy()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v1");
        var rev1 = _l2.Peek("orders.1._s1")!.Revision;
        await a.SetAsync("orders.1", "v2");
        await Eventually(() => _notify.Published.Count == 2, "both events");
        await Task.Delay(50);
        Assert.True(ServesFromL1(a, "orders.1", "v2")); // its own rev-2 event did not evict rev 2

        _notify.Append("test.notify.orders.1._s1", new CacheEvent(EventOp.Set, "orders.1._s1", rev1, "x", 0).Encode()); // late duplicate
        await Task.Delay(50);
        Assert.True(ServesFromL1(a, "orders.1", "v2"));
    }

    [Fact]
    public async Task Delete_evicts_everywhere_and_the_high_water_revision_blocks_a_lagging_read()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        Assert.Equal("v1", (await b.TryGetAsync<string>("orders.1")).Value);

        await a.RemoveAsync("orders.1");
        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "b evicted");
        _l2.StaleDirectReads = true; // b's replica still has v1

        Assert.False((await b.TryGetAsync<string>("orders.1")).Found);
        Assert.Contains(_notify.Published, e => e.Op == EventOp.Del && e.Key == "orders.1._s1");
    }

    [Fact]
    public async Task Tag_delete_evicts_the_prefix_on_other_nodes()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.42.head", "h");
        await a.SetAsync("orders.43.head", "x");
        await b.TryGetAsync<string>("orders.42.head");
        await b.TryGetAsync<string>("orders.43.head");

        await a.RemoveByTagAsync("orders.42");

        await Eventually(() => !ServesFromL1(b, "orders.42.head", "h"), "prefix evicted");
        Assert.True(ServesFromL1(b, "orders.43.head", "x"));
        var tag = Assert.Single(_notify.Published, e => e.Op == EventOp.Tag);
        Assert.Equal("orders.42", tag.Key);
    }

    [Fact]
    public async Task Tag_delete_with_no_keys_still_publishes_the_bucket_sequence_as_floor()
    {
        var a = await Node("a");
        await a.SetAsync("other.1", "o");
        await a.RemoveByTagAsync("orders.42");
        var tag = Assert.Single(_notify.Published, e => e.Op == EventOp.Tag);
        Assert.Equal(await _l2.LastSequenceAsync(default), tag.Rev);
    }

    [Fact]
    public async Task Clear_flushes_l1_on_every_node()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await b.TryGetAsync<string>("orders.1");

        await a.ClearAsync();

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "b flushed");
        Assert.False(ServesFromL1(a, "orders.1", "v1"));
        Assert.Contains(_notify.Published, e => e.Op == EventOp.Clear && e.Key == "*");
    }

    [Fact]
    public async Task Waiter_is_woken_by_the_set_event_without_waiting_for_a_poll()
    {
        var a = await Node("a");
        var b = await Node("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) => { started.SetResult(); await gate.Task; return "from-a"; }).AsTask();
        await started.Task;
        var tb = b.GetOrCreateAsync("orders.1", Factory("from-b")).AsTask();
        await Eventually(() => _locks.PutsSnapshot().Count >= 2, "b lost the acquire and waits");
        await Task.Delay(100); // b parks on the key's signal

        var fakeNow = _time.GetUtcNow();
        gate.SetResult();
        Assert.Equal("from-a", await ta);
        Assert.Equal("from-a", await tb.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(fakeNow, _time.GetUtcNow()); // no poll timer fired: the event woke it
        Assert.Equal(EventOp.Set, _notify.Published[^1].Op);
    }

    [Fact]
    public async Task Release_happens_before_the_event()
    {
        var a = await Node("a");
        CacheEvent? seen = null;
        L2Entry? lockAtEvent = null;
        // Watch the stream: when the set event lands, the lock must already be released.
        _ = Task.Run(async () =>
        {
            await foreach (var m in _notify.ConsumeAsync(1, default))
            {
                if (m.StreamSeq == 0) continue;
                lockAtEvent = _locks.Peek("orders.1._s1");
                seen = CacheEvent.Decode(m.Data.Span);
                break;
            }
        });

        await a.GetOrCreateAsync("orders.1", Factory("v"));
        await Eventually(() => seen is not null, "event seen");
        Assert.Equal(EventOp.Set, seen!.Op);
        Assert.Equal(L2Op.Delete, lockAtEvent!.Op);
    }

    [Fact]
    public async Task Failed_factory_publishes_fail_and_the_waiter_takes_over_within_its_backoff()
    {
        var a = await Node("a");
        var b = await Node("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync<string>("orders.1", async (_, _) => { started.SetResult(); await gate.Task; throw new InvalidOperationException("origin down"); }).AsTask();
        await started.Task;
        FactoryContext? ctx = null;
        var tb = b.GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("from-b"); }).AsTask();
        await Eventually(() => _locks.PutsSnapshot().Count >= 2, "b waits");

        gate.SetResult();
        await Assert.ThrowsAsync<InvalidOperationException>(() => ta);
        Assert.Contains(_notify.Published, e => e.Op == EventOp.Fail && e.Key == "orders.1._s1");
        var start = _time.GetUtcNow();
        // Cover the 10–50 ms takeover backoff in 10 ms steps, each with ample real time, and never reach the poll.
        for (var i = 0; i < 6 && !tb.IsCompleted; i++)
        {
            _time.Advance(TimeSpan.FromMilliseconds(10));
            await Task.WhenAny(tb, Task.Delay(500));
        }

        Assert.Equal("from-b", await tb);
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason);
        Assert.True(_time.GetUtcNow() - start < PollInterval(), "woken by the fail event, not by a poll");
    }

    private static TimeSpan PollInterval() => NatsCache.PollInterval;

    [Fact]
    public async Task Factory_outcomes_are_reported_after_the_write_and_release()
    {
        var a = await Node("a");
        await a.GetOrCreateAsync("orders.1", Factory("v"));
        var cached = Assert.Single(_completions);
        Assert.Equal(FactoryOutcome.Cached, cached.Outcome);
        Assert.Equal(_l2.Peek("orders.1._s1")!.Revision, cached.Revision);
        Assert.StartsWith("a:", cached.LockToken);
        Assert.Equal(FactoryReason.Miss, cached.Reason);
        Assert.Null(cached.Error);

        await Assert.ThrowsAsync<InvalidOperationException>(() => a.GetOrCreateAsync<string>("orders.2", (_, _) => throw new InvalidOperationException("x")).AsTask());
        Assert.Equal(FactoryOutcome.Failed, _completions[^1].Outcome);
        Assert.IsType<InvalidOperationException>(_completions[^1].Error);

        var writer = await Node("w");
        _l2.BeforePut = async _ => await writer.SetAsync("orders.3", "newer");
        Assert.Equal("newer", await a.GetOrCreateAsync("orders.3", Factory("stale")));
        Assert.Equal(FactoryOutcome.FencedNewer, _completions.Last(c => c.Key == "orders.3").Outcome);

        Assert.Null(await a.GetOrCreateAsync<string?>("orders.4", Factory<string?>(null)));
        Assert.Equal(FactoryOutcome.Uncached, _completions[^1].Outcome);

        _l2.Unavailable = true;
        Assert.Equal("local", await a.GetOrCreateAsync("orders.5", Factory("local")));
        Assert.Equal(FactoryOutcome.LocalOnly, _completions[^1].Outcome);
        Assert.Equal(FactoryReason.Degraded, _completions[^1].Reason);
    }

    [Fact]
    public async Task Sequence_gap_without_loss_resumes_and_applies_the_missed_events()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await a.SetAsync("orders.2", "w1");
        await b.TryGetAsync<string>("orders.1");
        await b.TryGetAsync<string>("orders.2");
        await Task.Delay(50);

        _notify.SkipDeliveries(1); // the next event is stored but not delivered: an ordered consumer skipping ahead
        await a.SetAsync("orders.1", "v2");
        await a.SetAsync("orders.2", "w2"); // delivered: seq jumps by 2

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "missed event re-read after the gap");
        Assert.False(ServesFromL1(b, "orders.2", "w1"));
    }

    [Fact]
    public async Task Discarded_events_flush_l1()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await b.TryGetAsync<string>("orders.1");
        await b.SetAsync("orders.9", "own"); // in b's L1 too
        await Eventually(() => _notify.Published.Count == 2, "events");
        await Task.Delay(50);

        _notify.SkipDeliveries(1);
        await a.SetAsync("orders.1", "v2");
        _notify.DiscardUpTo(3); // MaxAge dropped the event b missed (seq 3)
        await a.SetAsync("orders.5", "x");

        await Eventually(() => !ServesFromL1(b, "orders.9", "own"), "b flushed its whole L1");
        Assert.Equal("v2", (await b.TryGetAsync<string>("orders.1")).Value);
    }

    [Fact]
    public async Task Reconnect_runs_the_gap_check_and_a_recreated_stream_flushes_l1()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await b.TryGetAsync<string>("orders.1");
        await Eventually(() => _notify.Published.Count == 1, "event");
        await Task.Delay(50);

        _notify.Recreate(); // e.g. the cluster was wiped; consumers die
        _notify.RaiseReconnected();

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "b flushed after the recreated stream");
        await Eventually(() => _notify.Subscribers >= 2, "consumers resubscribed");
        await a.SetAsync("orders.2", "w"); // the new stream's events are consumed again
        await b.TryGetAsync<string>("orders.2");
        await a.SetAsync("orders.2", "w2");
        await Eventually(() => !ServesFromL1(b, "orders.2", "w"), "events flow after the resubscribe");
    }

    [Fact]
    public async Task Outage_writes_are_replayed_as_fenced_deletes_on_recovery()
    {
        var a = await Node("a");
        var b = await Node("b");
        await b.SetAsync("orders.1", "pre-outage");
        await b.SetAsync("orders.2", "pre-outage");
        Assert.Equal(CacheHealth.Healthy, a.Health);
        await Eventually(() => a.LastEventSeen == 2, "a applied b's events"); // a late one would evict a's outage value

        _time.Advance(TimeSpan.FromSeconds(5));
        _l2.Unavailable = true;
        _notify.Unavailable = true;
        await a.SetAsync("orders.1", "outage-write"); // Open mode: L1 only, journaled
        await a.SetAsync("orders.2", "outage-write");
        Assert.Equal(CacheHealth.Degraded, a.Health);
        Assert.True(ServesFromL1(a, "orders.1", "outage-write"));

        _l2.Unavailable = false;
        _notify.Unavailable = false;
        await b.SetAsync("orders.2", "written-after-the-outage-began"); // newer than a's outage write: keep it
        await AdvanceUntil(() => a.Health == CacheHealth.Healthy, "a recovered"); // recovery probe
        Assert.Equal(L2Op.Delete, _l2.Peek("orders.1._s1")!.Op); // the pre-outage value no longer served anywhere
        Assert.Equal(L2Op.Put, _l2.Peek("orders.2._s1")!.Op);
        Assert.False(ServesFromL1(a, "orders.1", "outage-write")); // L1 flushed
        Assert.Contains(_notify.Published, e => e.Op == EventOp.Del && e.Key == "orders.1._s1" && e.Node == "a");
    }

    [Fact]
    public async Task Health_is_unavailable_in_closed_mode_and_degraded_in_open_mode_while_nats_is_down()
    {
        var open = await Node("open");
        var closed = await Node("closed", o => o.FailureMode = FailureMode.Closed);
        _l2.Unavailable = true;
        await open.TryGetAsync<string>("k");
        await Assert.ThrowsAsync<CacheUnavailableException>(() => closed.TryGetAsync<string>("k").AsTask());

        Assert.Equal(CacheHealth.Degraded, open.Health);
        Assert.Equal(CacheHealth.Unavailable, closed.Health);
        Assert.Contains("unavailable", open.HealthDescription);

        _l2.Unavailable = false;
        await AdvanceUntil(() => open.Health == CacheHealth.Healthy && closed.Health == CacheHealth.Healthy, "both recovered");
    }

    [Fact]
    public async Task Malformed_events_are_skipped_and_the_consumer_keeps_going()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await b.TryGetAsync<string>("orders.1");
        _notify.Append("test.notify.garbage", "{not json"u8.ToArray());
        await a.SetAsync("orders.1", "v2");
        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "later events still applied");
    }

    [Fact]
    public async Task Event_publish_failure_does_not_fail_the_write()
    {
        var a = await Node("a");
        _notify.Unavailable = true;
        await a.SetAsync("orders.1", "v1");
        Assert.Equal(L2Op.Put, _l2.Peek("orders.1._s1")!.Op);
        Assert.Empty(_notify.Published);
        _notify.Unavailable = false;
    }
}
