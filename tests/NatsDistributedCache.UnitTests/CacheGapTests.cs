using System.Text;
using Microsoft.Extensions.Time.Testing;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

/// <summary>
/// Behaviour the mutation run (Stryker) and the sabotage suite (tests/Sabotage) showed no test pinned down: fence
/// retries, high-water revisions, local evictions without events, waiter wake-ups, recovery and provisioning.
/// </summary>
public sealed class CacheGapTests : IAsyncDisposable
{
    private const string Key = "orders.1._s1";

    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeL2Store _l2;
    private readonly FakeL2Store _locks;
    private readonly FakeNotificationTransport _notify = new();
    private readonly List<NatsCache> _caches = [];
    private readonly List<FactoryCompletion> _completions = [];

    public CacheGapTests()
    {
        _l2 = new FakeL2Store(_time);
        _locks = new FakeL2Store(_time);
    }

    private NatsCache Create(string node, Action<NatsCacheOptions>? configure, bool events, Func<CancellationToken, Task>? provision = null)
    {
        var o = new NatsCacheOptions
        {
            Prefix = "test",
            NodeId = node,
            OnFactoryCompleted = c => { lock (_completions) _completions.Add(c); },
        };
        configure?.Invoke(o);
        var cache = new NatsCache(o, _l2, _locks, events ? _notify : null, provision, null, _time, null, new FixedRandom(0.5));
        _caches.Add(cache);
        return cache;
    }

    /// <summary>A started node; <paramref name="events"/> = false gives it no notifications consumer.</summary>
    private async Task<NatsCache> Node(string node, Action<NatsCacheOptions>? configure = null, bool events = true)
    {
        var cache = Create(node, configure, events);
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

    /// <summary>True when the node serves <paramref name="expected"/> from its own L1 (no L2 read).</summary>
    private bool ServesFromL1<T>(NatsCache cache, string key, T expected)
    {
        var reads = _l2.DirectReads + _l2.LeaderReads;
        var r = cache.TryGetAsync<T>(key).AsTask().GetAwaiter().GetResult();
        return r.Found && Equals(r.Value, expected) && _l2.DirectReads + _l2.LeaderReads == reads;
    }

    /// <summary>Advances the clock in steps until <paramref name="done"/>; a background loop may arm its delay late.</summary>
    private async Task AdvanceUntil(Func<bool> done, TimeSpan step, string what)
    {
        for (var i = 0; i < 50 && !done(); i++)
        {
            _time.Advance(step);
            await Task.Delay(20);
        }

        Assert.True(done(), what);
    }

    private FactoryCompletion LastCompletion()
    {
        lock (_completions) return _completions[^1];
    }

    // ------------------------------------------------------------------ fence retries (design section 5)

    [Fact]
    public async Task Fence_retry_after_the_entry_turned_into_a_marker_expects_the_marker_revision()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v1"); // x-cache-ttl 600 s, Nats-TTL 622 s
        _time.Advance(TimeSpan.FromSeconds(601)); // logically expired, still a PUT: the owner expects its revision
        var b = await Node("b");
        // Between the leader read and the write, the server TTL runs out and writes a MaxAge marker.
        _l2.BeforePut = _ => { _time.Advance(TimeSpan.FromSeconds(22)); return Task.CompletedTask; };

        Assert.Equal("v2", await b.GetOrCreateAsync("orders.1", Factory("v2")));

        var last = _l2.PutsSnapshot()[^1];
        Assert.Equal(WriteStatus.Committed, last.Result.Status);
        Assert.Equal(last.Result.Seq - 1, last.Expected); // the marker's revision, not "empty"
        Assert.Equal(FactoryOutcome.Cached, LastCompletion().Outcome);
    }

    [Fact]
    public async Task Fence_retry_after_a_logically_expired_put_expects_its_revision()
    {
        var a = await Node("a");
        var b = await Node("b");
        ulong shortRev = 0;
        // A concurrent writer stores a value that is already logically expired when b's write lands.
        _l2.BeforePut = async _ =>
        {
            await a.SetAsync("orders.1", "short", new CacheEntryOptions { L2Ttl = TimeSpan.FromSeconds(1) });
            shortRev = _l2.Peek(Key)!.Revision;
            _time.Advance(TimeSpan.FromSeconds(2));
        };

        Assert.Equal("v2", await b.GetOrCreateAsync("orders.1", Factory("v2")));

        var last = _l2.PutsSnapshot()[^1];
        Assert.Equal(WriteStatus.Committed, last.Result.Status);
        Assert.Equal(shortRev, last.Expected); // not a conflict: retried at its revision
        Assert.Equal(FactoryOutcome.Cached, LastCompletion().Outcome);
    }

    [Fact]
    public async Task Unknown_write_settled_by_our_msg_id_is_cached_and_publishes_set()
    {
        var a = await Node("a");
        _l2.NextPutLosesReply = true;

        Assert.Equal("v", await a.GetOrCreateAsync("orders.1", Factory("v")));

        Assert.Equal(FactoryOutcome.Cached, LastCompletion().Outcome); // ours, not "fenced by a newer value"
        var rev = _l2.Peek(Key)!.Revision;
        Assert.Contains(_notify.Published, e => e.Op == EventOp.Set && e.Key == Key && e.Rev == rev);
        Assert.True(ServesFromL1(a, "orders.1", "v")); // the settled write filled L1
    }

    [Theory]
    [InlineData(2, FactoryOutcome.Cached)]          // the third attempt commits
    [InlineData(3, FactoryOutcome.FencedExhausted)] // at most three attempts
    public async Task Fence_makes_at_most_three_attempts(int conflicts, FactoryOutcome outcome)
    {
        var a = await Node("a");
        var b = await Node("b");
        var seen = 0;
        Func<string, Task>? conflict = null;
        // Before each of b's writes, a stores a value that is already logically expired: a conflict, never a winner.
        conflict = async _ =>
        {
            await a.SetAsync("orders.1", $"short-{seen}", new CacheEntryOptions { L2Ttl = TimeSpan.FromSeconds(1) });
            _time.Advance(TimeSpan.FromSeconds(2));
            if (++seen < conflicts) _l2.BeforePut = conflict;
        };
        _l2.BeforePut = conflict;

        Assert.Equal("v2", await b.GetOrCreateAsync("orders.1", Factory("v2")));

        Assert.Equal(outcome, LastCompletion().Outcome);
    }

    [Fact]
    public async Task Lock_is_released_before_set_and_fail_events_are_published()
    {
        var a = await Node("a");
        var states = new List<(string Key, L2Op? Lock)>();
        _notify.OnPublish = subject =>
        {
            var ik = subject.Substring("test.notify.".Length);
            lock (states) states.Add((ik, _locks.Peek(ik)?.Op));
        };

        await a.GetOrCreateAsync("orders.1", Factory("v"));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            a.GetOrCreateAsync<string>("orders.2", (_, _) => throw new InvalidOperationException("boom")).AsTask());

        Assert.Equal([(Key, L2Op.Delete), ("orders.2._s1", L2Op.Delete)], states);
    }

    // ------------------------------------------------------------------ high-water revision (design section 7, I5)

    [Fact]
    public async Task Set_event_raises_the_floor_so_a_stale_direct_read_is_not_served()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await a.SetAsync("orders.1", "v2");
        await Eventually(() => b.LastEventSeen == 2, "b applied both set events");

        _l2.StaleDirectReads = true; // the replica b reads from still has v1
        Assert.Equal("v2", (await b.TryGetAsync<string>("orders.1")).Value);
    }

    [Fact]
    public async Task Serving_a_revision_raises_the_floor_so_a_lagging_replica_cannot_serve_an_older_one()
    {
        var a = await Node("a");
        var b = await Node("b", events: false); // only what b served can protect it
        await a.SetAsync("orders.1", "v1");
        await a.SetAsync("orders.1", "v2");
        Assert.Equal("v2", (await b.TryGetAsync<string>("orders.1")).Value);

        _time.Advance(TimeSpan.FromSeconds(31)); // L1 copy gone (30 s); floor kept (30 s × 1.1 + 1 s)
        var leaderReads = _l2.LeaderReads;
        Assert.Equal("v2", (await b.TryGetAsync<string>("orders.1")).Value);
        Assert.Equal(leaderReads, _l2.LeaderReads); // a Direct Get at the floor is served as is

        _time.Advance(TimeSpan.FromSeconds(31));
        _l2.StaleDirectReads = true;
        Assert.Equal("v2", (await b.TryGetAsync<string>("orders.1")).Value); // the stale v1 is re-read through the leader
    }

    [Fact]
    public async Task Entry_still_below_the_floor_after_the_leader_read_is_not_served()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        // A del at revision 99 reached b, but neither replica nor leader shows it yet (b saw it first).
        _notify.Append("test.notify." + Key, new CacheEvent(EventOp.Del, Key, 99, "x", 0).Encode());
        await Eventually(() => b.LastEventSeen == 2, "b applied the del");

        Assert.False((await b.TryGetAsync<string>("orders.1")).Found);
    }

    [Fact]
    public async Task Own_write_raises_the_writers_floor()
    {
        var a = await Node("a", events: false); // only its own writes can protect it
        await a.SetAsync("orders.1", "v1");
        await a.SetAsync("orders.1", "v2");
        _time.Advance(TimeSpan.FromSeconds(31)); // L1 copy gone, floor kept
        _l2.StaleDirectReads = true;

        Assert.Equal("v2", (await a.TryGetAsync<string>("orders.1")).Value);
    }

    // ------------------------------------------------------------------ local effects without events

    [Fact]
    public async Task Update_fills_l1_and_publishes_set()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v1");
        var r = await a.TryGetAsync<string>("orders.1");

        Assert.True(await a.UpdateAsync("orders.1", r.Revision, "v2"));

        Assert.True(ServesFromL1(a, "orders.1", "v2"));
        var rev = _l2.Peek(Key)!.Revision;
        Assert.Equal((EventOp.Set, Key, rev), (_notify.Published[^1].Op, _notify.Published[^1].Key, _notify.Published[^1].Rev));
    }

    [Fact]
    public async Task Set_and_update_use_the_options_passed_in()
    {
        var a = await Node("a");
        var o = new CacheEntryOptions { L2Ttl = TimeSpan.FromSeconds(120) };
        await a.SetAsync("orders.1", "v1", o);
        Assert.Equal(TimeSpan.FromSeconds(142), _l2.PutsSnapshot()[^1].NatsTtl); // 120 s + 12 s lease + 10 s factory timeout

        Assert.True(await a.UpdateAsync("orders.1", _l2.Peek(Key)!.Revision, "v2", o));
        Assert.Equal(TimeSpan.FromSeconds(142), _l2.PutsSnapshot()[^1].NatsTtl);
    }

    [Fact]
    public async Task Remove_and_clear_evict_the_local_l1_without_any_event()
    {
        var a = await Node("a", events: false);
        await a.SetAsync("orders.1", "v1");
        await a.SetAsync("orders.2", "v2");
        Assert.True(ServesFromL1(a, "orders.1", "v1"));

        await a.RemoveAsync("orders.1");
        Assert.False((await a.TryGetAsync<string>("orders.1")).Found);

        await a.ClearAsync();
        Assert.False(ServesFromL1(a, "orders.2", "v2"));
    }

    [Fact]
    public async Task Remove_by_tag_deletes_only_the_prefix_in_l2()
    {
        var a = await Node("a");
        await a.SetAsync("orders.42.head", "h");
        await a.SetAsync("orders.43.head", "other");

        await a.RemoveByTagAsync("orders.42");

        Assert.Equal(L2Op.Delete, _l2.Peek("orders.42.head._s1")!.Op);
        Assert.Equal(L2Op.Put, _l2.Peek("orders.43.head._s1")!.Op);
    }

    [Fact]
    public async Task Remove_deletes_the_schema_versions_set_per_type()
    {
        var a = await Node("a", o => o.ForType<int>(3));
        await a.SetAsync("orders.1", 7);
        Assert.Equal(L2Op.Put, _l2.Peek("orders.1._s3")!.Op);

        await a.RemoveAsync("orders.1");

        Assert.Equal(L2Op.Delete, _l2.Peek("orders.1._s3")!.Op);
    }

    [Fact]
    public async Task Events_go_to_the_key_tag_and_clear_subjects()
    {
        var a = await Node("a");
        var subjects = new List<string>();
        _notify.OnPublish = s => { lock (subjects) subjects.Add(s); };

        await a.SetAsync("orders.1", "v");
        await a.RemoveByTagAsync("orders");
        await a.ClearAsync();

        Assert.Equal(["test.notify.orders.1._s1", "test.notify.orders", "test.notify._clear"], subjects);
    }

    public static TheoryData<string> OutageWrites => ["remove", "tag", "update"];

    [Theory]
    [MemberData(nameof(OutageWrites))]
    public async Task Writes_during_an_outage_evict_locally_and_mark_the_node_degraded(string op)
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v");
        _l2.Unavailable = true;
        _notify.Unavailable = true;

        switch (op)
        {
            case "remove": await a.RemoveAsync("orders.1"); break;
            case "tag": await a.RemoveByTagAsync("orders"); break;
            default: await Assert.ThrowsAsync<CacheUnavailableException>(() => a.UpdateAsync("orders.1", 1, "w").AsTask()); break;
        }

        Assert.Equal(CacheHealth.Degraded, a.Health);
        if (op != "update") Assert.False(ServesFromL1(a, "orders.1", "v")); // the local copy is gone at once
    }

    [Fact]
    public async Task Write_within_the_skew_margin_of_the_outage_is_kept_on_recovery()
    {
        var a = await Node("a");
        var b = await Node("b");
        await b.SetAsync("orders.1", "pre-outage");
        await Eventually(() => a.LastEventSeen == 1, "a applied b's event");
        _time.Advance(TimeSpan.FromSeconds(1)); // a's outage write is exactly 1 s (the skew margin) later
        _l2.Unavailable = true;
        _notify.Unavailable = true;
        await a.SetAsync("orders.1", "outage-write");

        _l2.Unavailable = false;
        _notify.Unavailable = false;
        await AdvanceUntil(() => a.Health == CacheHealth.Healthy, TimeSpan.FromSeconds(1), "recovered");

        Assert.Equal(L2Op.Put, _l2.Peek(Key)!.Op); // could be concurrent with the outage write: not deleted
    }

    // ------------------------------------------------------------------ waiters (design section 5)

    public static TheoryData<string> WakingEvents => ["del", "tag", "clear"];

    [Theory]
    [MemberData(nameof(WakingEvents))]
    public async Task Parked_waiter_does_not_poll_and_is_woken_by_del_tag_and_clear_events(string op)
    {
        var a = await Node("a");
        var b = await Node("b");
        var c = await Node("c");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) => { started.SetResult(); await gate.Task; return "from-a"; }).AsTask();
        await started.Task;
        var tb = b.GetOrCreateAsync("orders.1", Factory("from-b")).AsTask();
        await Eventually(() => _locks.PutsSnapshot().Count >= 2, "b lost the acquire and waits");
        await Task.Delay(100);

        var reads = _locks.LeaderReads;
        await Task.Delay(150);
        Assert.Equal(reads, _locks.LeaderReads); // parked: no reads while no poll is due and no event came

        switch (op)
        {
            case "del": await c.RemoveAsync("orders.1"); break;
            case "tag": await c.RemoveByTagAsync("orders"); break;
            default: await c.ClearAsync(); break;
        }

        await Eventually(() => _locks.LeaderReads > reads, $"b woken by the {op} event");
        gate.SetResult();
        await ta;
        // After a del, a's write is fenced and b takes over once its 10–50 ms takeover backoff (fake clock) passes.
        for (var i = 0; i < 100 && !tb.IsCompleted; i++)
        {
            _time.Advance(TimeSpan.FromMilliseconds(50));
            await Task.Delay(10);
        }

        await tb.WaitAsync(TimeSpan.FromSeconds(5));
        await Eventually(() => b.PendingSignals == 0, "the waiter dropped its signal");
    }

    [Fact]
    public async Task Different_keys_never_share_a_load()
    {
        var a = await Node("a");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var one = a.GetOrCreateAsync("orders.1", async (_, _) => { started.SetResult(); await gate.Task; return "one"; }).AsTask();
        await started.Task;

        Assert.Equal("two", await a.GetOrCreateAsync("orders.2", Factory("two")).AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        gate.SetResult();
        Assert.Equal("one", await one);
    }

    // ------------------------------------------------------------------ early refresh (design section 6)

    [Fact]
    public async Task Read_from_l2_inside_the_refresh_window_starts_a_refresh()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v1");
        _time.Advance(TimeSpan.FromSeconds(545)); // last 10 % of 600 s
        var b = await Node("b");
        var calls = 0;

        Assert.Equal("v1", await b.GetOrCreateAsync("orders.1", Factory("v2", () => Interlocked.Increment(ref calls))));

        await Eventually(() => Volatile.Read(ref calls) == 1, "background refresh ran");
    }

    [Fact]
    public async Task One_l1_fill_starts_at_most_one_refresh_even_when_the_refresh_is_a_no_op()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v1");
        _time.Advance(TimeSpan.FromSeconds(545));
        // Another node holds the lock, so every refresh attempt is a no-op that leaves the old L1 item in place.
        var token = LockToken.New("other", _time.GetUtcNow(), TimeSpan.FromSeconds(12), TimeSpan.FromSeconds(10));
        await _locks.PutAsync(Key, Encoding.UTF8.GetBytes(token.ToString()), new Dictionary<string, string>(), TimeSpan.FromSeconds(12), 0, default);
        var before = _locks.PutsSnapshot().Count;

        for (var i = 0; i < 5; i++)
        {
            Assert.Equal("v1", await a.GetOrCreateAsync("orders.1", Factory("v2")));
            await Task.Delay(30); // let each background attempt finish
        }

        Assert.Equal(1, _locks.PutsSnapshot().Count - before); // one acquire attempt, not one per read
    }

    [Fact]
    public async Task Early_refreshes_of_different_keys_run_independently()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "v1");
        await a.SetAsync("orders.2", "v1");
        _time.Advance(TimeSpan.FromSeconds(545));
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        Assert.Equal("v1", await a.GetOrCreateAsync("orders.1", async (c, _) =>
        {
            if (c.Reason == FactoryReason.EarlyRefresh) { started.TrySetResult(); await gate.Task; }
            return "r1";
        }));
        await started.Task; // orders.1's refresh is still running

        var calls = 0;
        Assert.Equal("v1", await a.GetOrCreateAsync("orders.2", Factory("r2", () => Interlocked.Increment(ref calls))));

        await Eventually(() => Volatile.Read(ref calls) == 1, "orders.2 refreshed on its own");
        gate.SetResult();
    }

    // ------------------------------------------------------------------ outage and recovery (design section 8)

    [Fact]
    public async Task Recovery_flushes_values_computed_locally_during_the_outage()
    {
        var a = await Node("a");
        _l2.Unavailable = true;
        _notify.Unavailable = true;
        Assert.Equal("local", await a.GetOrCreateAsync("orders.9", Factory("local"))); // Open mode: L1 only, not journaled
        Assert.True(ServesFromL1(a, "orders.9", "local"));

        _l2.Unavailable = false;
        _notify.Unavailable = false;
        await AdvanceUntil(() => a.Health == CacheHealth.Healthy, TimeSpan.FromSeconds(1), "recovered");

        Assert.False(ServesFromL1(a, "orders.9", "local"));
    }

    [Fact]
    public async Task Outage_write_replaces_a_cached_copy_so_the_node_reads_its_own_write()
    {
        var a = await Node("a");
        await a.SetAsync("orders.1", "pre-outage");
        Assert.True(ServesFromL1(a, "orders.1", "pre-outage"));
        await Eventually(() => a.LastEventSeen == 1, "a applied its own set event"); // a late one evicts a revision-0 value
        _l2.Unavailable = true;
        _notify.Unavailable = true;

        await a.SetAsync("orders.1", "outage-write"); // Open mode: L1 only, revision 0

        Assert.Equal("outage-write", (await a.TryGetAsync<string>("orders.1")).Value);
    }

    [Fact]
    public async Task Recovery_loop_probes_once_per_second_and_stops_after_recovering()
    {
        var a = await Node("a");
        _l2.Unavailable = true;
        await a.TryGetAsync<string>("orders.1"); // enters the outage
        await Task.Delay(100);
        Assert.Equal(0, _l2.LastSequenceCalls); // the first probe waits a second

        await AdvanceUntil(() => _l2.LastSequenceCalls >= 1, TimeSpan.FromSeconds(1), "first probe");
        await Task.Delay(100);
        Assert.Equal(1, _l2.LastSequenceCalls); // no tight retry loop against a down NATS

        _l2.Unavailable = false;
        await AdvanceUntil(() => a.Health == CacheHealth.Healthy, TimeSpan.FromSeconds(1), "recovered");
        await a.SetAsync("orders.1", "v");
        await Task.Delay(50);
        var probes = _l2.LastSequenceCalls;
        for (var i = 0; i < 3; i++)
        {
            _time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(30);
        }

        Assert.Equal(probes, _l2.LastSequenceCalls); // the loop ended: no more probes against a healthy NATS
        Assert.True(ServesFromL1(a, "orders.1", "v"));
    }

    // ------------------------------------------------------------------ provisioning, validation, shutdown

    [Fact]
    public async Task Unusable_stores_stop_the_provisioning_loop()
    {
        var calls = 0;
        var a = Create("a", null, events: true, provision: _ =>
        {
            Interlocked.Increment(ref calls);
            throw new CacheProvisioningException("history is 5");
        });
        a.Start();
        await Eventually(() => a.ProvisioningError is not null, "fatal error recorded");
        await Task.Delay(100);

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    public static TheoryData<string> Operations => ["tryget", "set", "update", "remove", "tag", "clear"];

    private static Task Run(NatsCache c, string op, CancellationToken ct) => op switch
    {
        "tryget" => c.TryGetAsync<string>("orders.1", ct).AsTask(),
        "set" => c.SetAsync("orders.1", "v", null, ct).AsTask(),
        "update" => c.UpdateAsync("orders.1", 0, "v", null, ct).AsTask(),
        "remove" => c.RemoveAsync("orders.1", ct).AsTask(),
        "tag" => c.RemoveByTagAsync("orders", ct).AsTask(),
        _ => c.ClearAsync(ct).AsTask(),
    };

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Operations_before_provisioning_never_touch_l2(string op)
    {
        var c = Create("a", o => o.FailureMode = FailureMode.Closed, events: true, provision: ct => Task.Delay(Timeout.Infinite, ct));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAsync<CacheUnavailableException>(() => Run(c, op, cts.Token));

        Assert.Empty(_l2.PutsSnapshot());
        Assert.Equal(0, _l2.DirectReads + _l2.LeaderReads);
        Assert.Empty(_notify.Published);
    }

    public static TheoryData<string> KeyedOperations => ["tryget", "set", "update", "remove", "tag"];

    [Theory]
    [MemberData(nameof(KeyedOperations))]
    public async Task Operations_validate_the_key(string op)
    {
        var c = await Node("a");
        var bad = op == "tag"
            ? (Func<Task>)(() => c.RemoveByTagAsync("bad prefix").AsTask())
            : () => op switch
            {
                "tryget" => c.TryGetAsync<string>("bad key").AsTask(),
                "set" => c.SetAsync("bad key", "v").AsTask(),
                "update" => c.UpdateAsync("bad key", 0, "v").AsTask(),
                _ => c.RemoveAsync("bad key").AsTask(),
            };

        await Assert.ThrowsAsync<ArgumentException>(bad);
        Assert.Empty(_l2.PutsSnapshot());
    }

    [Fact]
    public void Constructor_validates_the_options()
    {
        Assert.Throws<ArgumentException>(() => Create("a", o => o.Prefix = "bad prefix", events: false));
    }

    [Fact]
    public async Task Dispose_stops_the_notifications_consumer()
    {
        var a = await Node("a");
        Assert.Equal(1, _notify.Subscribers);

        await a.DisposeAsync();

        await Eventually(() => _notify.Subscribers == 0, "consumer stopped");
    }

    // ------------------------------------------------------------------ notifications consumer (design section 7, rule 7)

    [Fact]
    public async Task Recreated_stream_is_detected_even_when_it_already_holds_more_events()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await b.TryGetAsync<string>("orders.1");
        await Eventually(() => b.LastEventSeen == 1, "b applied the event");

        // The stream is recreated and refilled past b's position before b resubscribes, so the sequences look
        // continuous; only the creation time shows that the new stream's first event (orders.1) was never seen.
        _notify.Recreate();
        _notify.Append("test.notify." + Key, new CacheEvent(EventOp.Set, Key, 99, "x", 0).Encode());
        _notify.Append("test.notify.other._s1", new CacheEvent(EventOp.Set, "other._s1", 100, "x", 0).Encode());
        _notify.Append("test.notify.other._s1", new CacheEvent(EventOp.Set, "other._s1", 101, "x", 0).Encode());

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "b flushed L1");
    }

    [Fact]
    public async Task Stream_recreated_under_a_live_consumer_is_found_by_the_position_check()
    {
        var fast = (Action<NatsCacheOptions>)(o => o.NotificationsCheckInterval = TimeSpan.FromMilliseconds(100));
        var a = await Node("a", fast);
        var b = await Node("b", fast);
        await a.SetAsync("orders.1", "v1");
        await b.TryGetAsync<string>("orders.1");
        await Eventually(() => b.LastEventSeen == 1, "b applied the event");

        _notify.Recreate(dropConsumers: false); // the consumer neither fails nor sees a jump

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "b flushed L1");
    }

    [Fact]
    public async Task Reconnect_alone_triggers_the_gap_check_and_replays_missed_events()
    {
        var a = await Node("a");
        var b = await Node("b");
        await a.SetAsync("orders.1", "v1");
        await b.TryGetAsync<string>("orders.1");
        await Eventually(() => b.LastEventSeen == 1, "b applied the event");

        _notify.SkipDeliveries(1); // stored, never delivered live (lost while the connection was down)
        await a.SetAsync("orders.1", "v2");
        await Task.Delay(50);
        Assert.True(ServesFromL1(b, "orders.1", "v1"));

        _notify.RaiseReconnected();

        await Eventually(() => !ServesFromL1(b, "orders.1", "v1"), "the missed set event was replayed");
    }
}
