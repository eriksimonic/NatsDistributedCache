using Microsoft.Extensions.Time.Testing;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

/// <summary>Behaviour pinned by design review 8 (v1.4): listener position, recovery, journal, prefix floors, waiters.</summary>
public sealed class Review8Tests : IAsyncDisposable
{
    private const string Key = "orders.1._s1";

    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeL2Store _l2;
    private readonly FakeL2Store _locks;
    private readonly FakeNotificationTransport _notify = new();
    private readonly List<NatsCache> _caches = [];
    private readonly List<FactoryCompletion> _completions = [];

    public Review8Tests()
    {
        _l2 = new FakeL2Store(_time);
        _locks = new FakeL2Store(_time);
    }

    private NatsCache Create(string node, Action<NatsCacheOptions>? configure = null, Func<CancellationToken, Task>? provision = null,
        ICacheSerializer? serializer = null)
    {
        var o = new NatsCacheOptions
        {
            Prefix = "test",
            NodeId = node,
            OnFactoryCompleted = c => { lock (_completions) _completions.Add(c); },
        };
        configure?.Invoke(o);
        var cache = new NatsCache(o, _l2, _locks, _notify, provision, null, _time, serializer, new FixedRandom(0.5));
        _caches.Add(cache);
        return cache;
    }

    private async Task<NatsCache> Node(string node, Action<NatsCacheOptions>? configure = null, ICacheSerializer? serializer = null)
    {
        var cache = Create(node, configure, serializer: serializer);
        cache.Start();
        await Eventually(() => cache.IsReady, $"{node} ready");
        await cache.NotificationsLive.WaitAsync(TimeSpan.FromSeconds(5));
        return cache;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _caches) await c.DisposeAsync();
    }

    private static Func<FactoryContext, CancellationToken, ValueTask<T>> Factory<T>(T value, Action? seen = null) =>
        (_, _) => { seen?.Invoke(); return new ValueTask<T>(value); };

    private static Task Eventually(Func<bool> condition, string what) => DistributedLockTests.Eventually(condition, what);

    private async Task AdvanceUntil(Func<bool> done, string what)
    {
        for (var i = 0; i < 50 && !done(); i++)
        {
            _time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(20);
        }

        Assert.True(done(), what);
    }

    private bool ServesFromL1<T>(NatsCache cache, string key, T expected)
    {
        var reads = _l2.DirectReads + _l2.LeaderReads;
        var r = cache.TryGetAsync<T>(key).AsTask().GetAwaiter().GetResult();
        return r.Found && Equals(r.Value, expected) && _l2.DirectReads + _l2.LeaderReads == reads;
    }

    private void Outage(bool down)
    {
        _l2.Unavailable = down;
        _notify.Unavailable = down;
    }

    // ------------------------------------------------------------------ #1 position from the first subscribe

    [Fact]
    public async Task Node_that_never_processed_an_event_still_replays_events_missed_across_a_reconnect()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v1");
        var b = await Node("b"); // subscribes after that event: it has processed nothing
        Assert.Equal(1UL, b.LastEventSeen); // its position is the stream's last sequence at subscribe time
        Assert.Equal("v1", (await b.TryGetAsync<string>("orders.1")).Value);

        _notify.SkipDeliveries(1); // lost while b's connection was down
        await a.SetAsync("orders.1", "v2");
        await Task.Delay(50);
        Assert.True(ServesFromL1(b, "orders.1", "v1"));

        _notify.RaiseReconnected();

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "b replayed the missed event");
    }

    // ------------------------------------------------------------------ #2–#4 recovery

    [Fact]
    public async Task Replay_that_fails_with_any_error_keeps_the_journal_and_retries()
    {
        var a = await Node("a");
        var b = await Node("b");
        await b.SetAsync("orders.42.x", "pre-outage");
        _time.Advance(TimeSpan.FromSeconds(2));
        Outage(true);
        await a.RemoveByTagAsync("orders.42"); // journals the prefix
        Outage(false);
        _l2.NextListFails = new InvalidOperationException("not a NATS error"); // the first prefix replay fails

        await AdvanceUntil(() => a.Health == CacheHealth.Healthy, "a recovered on a later probe");

        Assert.Equal(L2Op.Delete, _l2.Peek("orders.42.x._s1")!.Op);
    }

    [Fact]
    public async Task Write_journaled_during_the_replay_is_replayed_too()
    {
        var a = await Node("a");
        var b = await Node("b");
        await b.SetAsync("orders.1", "pre-outage");
        await b.SetAsync("orders.2", "pre-outage");
        _time.Advance(TimeSpan.FromSeconds(2));
        Outage(true);
        await a.SetAsync("orders.1", "outage-write");
        Outage(false);
        // NATS flaps while the replay deletes orders.1: a's write of orders.2 in that moment is journaled.
        _l2.BeforeDelete = async _ =>
        {
            _l2.Unavailable = true;
            await a.SetAsync("orders.2", "written-during-the-replay");
            _l2.Unavailable = false;
        };

        await AdvanceUntil(() => a.Health == CacheHealth.Healthy && _l2.Peek("orders.2._s1")!.Op == L2Op.Delete, "both replayed");

        Assert.Equal(L2Op.Delete, _l2.Peek(Key)!.Op);
    }

    [Fact]
    public async Task Transient_outage_without_outage_writes_keeps_l1()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v");
        _l2.Unavailable = true;
        await a.TryGetAsync<string>("orders.9"); // a read fails: outage, but nothing written L1-only
        Assert.Equal(CacheHealth.Degraded, a.Health);
        _l2.Unavailable = false;

        await AdvanceUntil(() => a.Health == CacheHealth.Healthy, "recovered");

        Assert.True(ServesFromL1(a, "orders.1", "v")); // no flush, no miss storm after a blip
    }

    [Fact]
    public async Task Stores_not_provisioned_yet_are_not_an_outage()
    {
        var a = Create("a", provision: ct => Task.Delay(Timeout.Infinite, ct));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        Assert.False((await a.TryGetAsync<string>("orders.1", cts.Token)).Found); // Open mode: a miss

        Assert.Equal("stores not provisioned yet", a.HealthDescription);
        _time.Advance(TimeSpan.FromSeconds(2));
        await Task.Delay(50);
        Assert.Equal(0, _l2.LastSequenceCalls); // no recovery loop was started
    }

    [Fact]
    public async Task Journal_overflow_still_flushes_l1_and_replays_what_fits()
    {
        var a = await Node("a", o => o.OutageJournalCapacity = 1);
        var b = await Node("b");
        await b.SetAsync("orders.1", "pre-outage");
        await b.SetAsync("orders.2", "pre-outage");
        _time.Advance(TimeSpan.FromSeconds(2));
        Outage(true);
        await a.SetAsync("orders.1", "outage-write");
        await a.SetAsync("orders.2", "outage-write"); // over capacity: not journaled
        Outage(false);

        await AdvanceUntil(() => a.Health == CacheHealth.Healthy, "recovered");

        Assert.Equal(L2Op.Delete, _l2.Peek(Key)!.Op);
        Assert.Equal(L2Op.Put, _l2.Peek("orders.2._s1")!.Op);
        Assert.False(ServesFromL1(a, "orders.2", "outage-write")); // L1 flushed regardless
    }

    public static TheoryData<string> ClosedRemovals => ["remove", "tag"];

    [Theory]
    [MemberData(nameof(ClosedRemovals))]
    public async Task Closed_mode_removal_during_an_outage_evicts_locally_before_throwing(string op)
    {
        var a = await Node("a", o => o.FailureMode = FailureMode.Closed);
        await a.SetAsync("orders.1", "v");
        _l2.Unavailable = true;

        await Assert.ThrowsAsync<CacheUnavailableException>(() =>
            op == "remove" ? a.RemoveAsync("orders.1").AsTask() : a.RemoveByTagAsync("orders").AsTask());
        Assert.Equal(CacheHealth.Unavailable, a.Health);
        _l2.Unavailable = false;

        Assert.False(ServesFromL1(a, "orders.1", "v")); // the local copy went before the throw
    }

    // ------------------------------------------------------------------ #6 prefix floors survive a flush

    [Fact]
    public async Task Prefix_floor_survives_a_clear_so_a_lagging_replica_cannot_refill_a_tag_deleted_key()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.42.x", "v1");
        Assert.Equal("v1", (await b.TryGetAsync<string>("orders.42.x")).Value);
        await a.RemoveByTagAsync("orders.42");
        await a.ClearAsync();
        await Eventually(() => b.LastEventSeen == 3, "b applied set, tag and clear");

        _l2.StaleDirectReads = true; // b's replica still has v1

        Assert.False((await b.TryGetAsync<string>("orders.42.x")).Found);
    }

    // ------------------------------------------------------------------ #9, #10 owner and waiter

    private sealed class ThrowingSerializer : ICacheSerializer
    {
        public byte[] Serialize<T>(T value) => throw new InvalidOperationException("serializer broke");

        public T? Deserialize<T>(ReadOnlyMemory<byte> data) => throw new InvalidOperationException("serializer broke");
    }

    [Fact]
    public async Task Write_error_other_than_nats_is_reported_with_its_cause()
    {
        var a = await Node("a", serializer: new ThrowingSerializer());

        await Assert.ThrowsAsync<InvalidOperationException>(() => a.GetOrCreateAsync("orders.1", Factory("v")).AsTask());

        FactoryCompletion last;
        lock (_completions) last = _completions[^1];
        Assert.Equal(FactoryOutcome.Failed, last.Outcome);
        Assert.IsType<InvalidOperationException>(last.Error);
        Assert.Contains(_notify.Published, e => e.Op == EventOp.Fail && e.Key == Key);
    }

    [Fact]
    public async Task Waiter_rides_out_a_transient_error_instead_of_running_a_second_factory()
    {
        var a = await Node("a");
        var b = await Node("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) => { started.SetResult(); await gate.Task; return "from-a"; }).AsTask();
        await started.Task;
        var calls = 0;
        var tb = b.GetOrCreateAsync("orders.1", Factory("from-b", () => Interlocked.Increment(ref calls))).AsTask();
        await Eventually(() => _locks.PutsSnapshot().Count >= 2, "b waits");
        await Task.Delay(50);

        _l2.FailNextReads = 1;
        _time.Advance(NatsCache.PollInterval); // b polls and its read fails once
        await Eventually(() => _l2.FailNextReads == 0, "b's read failed");
        await Task.Delay(50);

        gate.SetResult();
        Assert.Equal("from-a", await ta);
        Assert.Equal("from-a", await tb.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(0, calls);
    }

    public static TheoryData<string> UncachedOutcomes => ["null", "bucket-full"];

    [Theory]
    [MemberData(nameof(UncachedOutcomes))]
    public async Task Uncached_owner_outcomes_publish_fail_so_the_waiter_takes_over(string outcome)
    {
        var a = await Node("a");
        var b = await Node("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync<string?>("orders.1", async (_, _) =>
        {
            started.SetResult();
            await gate.Task;
            return outcome == "null" ? null : "from-a";
        }).AsTask();
        await started.Task;
        FactoryContext? ctx = null;
        var tb = b.GetOrCreateAsync<string?>("orders.1", (c, _) => { ctx = c; return new ValueTask<string?>("from-b"); }).AsTask();
        await Eventually(() => _locks.PutsSnapshot().Count >= 2, "b waits");
        if (outcome == "bucket-full") _l2.RejectWritesWith = 10077;

        gate.SetResult();
        await ta;
        for (var i = 0; i < 100 && !tb.IsCompleted; i++) // the takeover backoff runs on the fake clock
        {
            _time.Advance(TimeSpan.FromMilliseconds(50));
            await Task.Delay(10);
        }

        Assert.Equal("from-b", await tb.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason);
        Assert.Contains(_notify.Published, e => e.Op == EventOp.Fail && e.Key == Key);
    }
}
