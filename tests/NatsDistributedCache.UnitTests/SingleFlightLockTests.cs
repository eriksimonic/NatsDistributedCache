using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

/// <summary>Distributed single-flight through the cache (design sections 5 and 6), with nodes sharing one fake NATS.</summary>
public sealed class SingleFlightLockTests : IAsyncDisposable
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeL2Store _l2;
    private readonly FakeL2Store _locks;
    private readonly FakeNotificationTransport _notify = new();
    private readonly List<NatsCache> _caches = [];

    public SingleFlightLockTests()
    {
        _l2 = new FakeL2Store(_time);
        _locks = new FakeL2Store(_time);
    }

    private NatsCache NewCache(string node, Action<NatsCacheOptions>? configure = null)
    {
        var o = new NatsCacheOptions { Prefix = "test", NodeId = node };
        configure?.Invoke(o);
        var cache = new NatsCache(o, _l2, _locks, _notify, null, null, _time, null, new FixedRandom(0.5));
        _caches.Add(cache);
        return cache;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _caches) await c.DisposeAsync();
    }

    /// <summary>Advances fake time in small steps until the task completes; returns the fake time that passed.</summary>
    private async Task<(T Value, TimeSpan Elapsed)> Drive<T>(Task<T> task, TimeSpan? step = null, TimeSpan? max = null)
    {
        var start = _time.GetUtcNow();
        var limit = max ?? TimeSpan.FromSeconds(60);
        while (!task.IsCompleted && _time.GetUtcNow() - start < limit)
        {
            await Task.Delay(2);
            _time.Advance(step ?? TimeSpan.FromMilliseconds(50));
        }

        return (await task, _time.GetUtcNow() - start);
    }

    private async Task DriveFor(TimeSpan span)
    {
        var end = _time.GetUtcNow() + span;
        while (_time.GetUtcNow() < end)
        {
            await Task.Delay(2);
            _time.Advance(TimeSpan.FromMilliseconds(50));
        }
    }

    private async Task<LockLease> HoldLock(string key, TimeSpan lease, TimeSpan? factoryTimeout = null)
    {
        var r = await new DistributedLock(_locks, "crashed", _time, NullLogger.Instance).TryAcquireAsync(key, lease, factoryTimeout ?? TimeSpan.FromSeconds(10), default);
        return r.Lease!;
    }

    /// <summary>
    /// A misbehaving owner that keeps its token alive forever (renewing past its own FactoryTimeout + LeaseTtl):
    /// the only case in which a waiter gives up on a live lock.
    /// </summary>
    private async Task<(T Value, TimeSpan Elapsed)> DriveWithRogueOwner<T>(Task<T> task, LockLease rogue)
    {
        var start = _time.GetUtcNow();
        var revision = rogue.Revision;
        var lastRenew = start;
        while (!task.IsCompleted && _time.GetUtcNow() - start < TimeSpan.FromSeconds(60))
        {
            await Task.Delay(2);
            _time.Advance(TimeSpan.FromMilliseconds(50));
            if (_time.GetUtcNow() - lastRenew < TimeSpan.FromSeconds(1)) continue;
            var r = await _locks.PutAsync(rogue.Key, rogue.TokenBytes, new Dictionary<string, string>(), rogue.Token.Lease, revision, default);
            if (r.Status == WriteStatus.Committed) revision = r.Seq;
            lastRenew = _time.GetUtcNow();
        }

        return (await task, _time.GetUtcNow() - start);
    }

    [Fact]
    public async Task Waiter_on_another_node_gets_the_owners_value_without_running_its_factory()
    {
        var a = NewCache("a");
        var b = NewCache("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var calls = 0;

        var ta = a.GetOrCreateAsync("orders.1", async (_, _) =>
        {
            Interlocked.Increment(ref calls);
            started.SetResult();
            await gate.Task;
            return "from-a";
        }).AsTask();
        await started.Task;

        var tb = b.GetOrCreateAsync("orders.1", (_, _) =>
        {
            Interlocked.Increment(ref calls);
            return new ValueTask<string>("from-b");
        }).AsTask();
        await DriveFor(TimeSpan.FromSeconds(2));
        Assert.False(tb.IsCompleted); // waiting on a's lock

        gate.SetResult();
        Assert.Equal("from-a", await ta);
        Assert.Equal("from-a", (await Drive(tb)).Value);
        Assert.Equal(1, calls);
        Assert.Single(_l2.Puts);
    }

    [Fact]
    public async Task Five_nodes_racing_on_one_key_run_the_factory_once()
    {
        var gate = new TaskCompletionSource();
        var calls = 0;
        var tasks = Enumerable.Range(0, 5).Select(i => NewCache($"n{i}")).SelectMany(c => Enumerable.Range(0, 20).Select(_ =>
            c.GetOrCreateAsync("orders.1", async (_, _) =>
            {
                Interlocked.Increment(ref calls);
                await gate.Task;
                return 7;
            }).AsTask())).ToList();

        await DriveFor(TimeSpan.FromSeconds(1));
        gate.SetResult();
        var all = Task.WhenAll(tasks);
        await Drive(all);
        Assert.All(await all, v => Assert.Equal(7, v));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Crashed_owner_is_taken_over_after_its_lease()
    {
        await HoldLock("orders.1._s1", TimeSpan.FromSeconds(12));
        FactoryContext? ctx = null;
        var t = NewCache("b").GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("v"); }).AsTask();

        var (value, elapsed) = await Drive(t);

        Assert.Equal("v", value);
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason);
        Assert.Equal(2, ctx.Attempt);
        Assert.StartsWith("b:", ctx.LockToken);
        Assert.InRange(elapsed, TimeSpan.FromSeconds(12), TimeSpan.FromSeconds(13));
        Assert.Equal(WriteStatus.Committed, Assert.Single(_l2.Puts).Result.Status);
    }

    [Fact]
    public async Task Failed_factory_releases_the_lock_so_a_waiter_takes_over_at_once()
    {
        var a = NewCache("a");
        var b = NewCache("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync<string>("orders.1", async (_, _) =>
        {
            started.SetResult();
            await gate.Task;
            throw new InvalidOperationException("origin down");
        }).AsTask();
        await started.Task;

        FactoryContext? ctx = null;
        var tb = b.GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("from-b"); }).AsTask();
        await DriveFor(TimeSpan.FromSeconds(1));
        gate.SetResult();
        await Assert.ThrowsAsync<InvalidOperationException>(() => ta);
        Assert.Equal(L2Op.Delete, _locks.Peek("orders.1._s1")!.Op);

        var (value, elapsed) = await Drive(tb);
        Assert.Equal("from-b", value);
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason);
        Assert.True(elapsed < TimeSpan.FromSeconds(1), $"took {elapsed}"); // not a lease wait
    }

    [Fact]
    public async Task Waiter_keeps_waiting_past_the_timeout_while_the_owner_renews()
    {
        var a = NewCache("a");
        var b = NewCache("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var calls = 0;
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) =>
        {
            Interlocked.Increment(ref calls);
            started.SetResult();
            await gate.Task;
            return "slow";
        }, CacheEntryOptions.Default with { FactoryTimeout = TimeSpan.FromMinutes(5), LeaseTtl = TimeSpan.FromSeconds(6) }).AsTask();
        await started.Task;
        var tb = b.GetOrCreateAsync("orders.1", (_, _) => { Interlocked.Increment(ref calls); return new ValueTask<string>("b"); }).AsTask();

        await DriveFor(TimeSpan.FromSeconds(30)); // past b's 15 s LockWaitTimeout, renewals every LeaseTtl / 3
        Assert.False(tb.IsCompleted);
        Assert.True(_locks.PutsSnapshot().Count(p => p.Key == "orders.1._s1" && p.Result.Status == WriteStatus.Committed) > 2);

        gate.SetResult();
        Assert.Equal("slow", await ta);
        Assert.Equal("slow", (await Drive(tb)).Value);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Early_refresh_serves_the_current_value_and_refreshes_once_in_the_background()
    {
        var o = CacheEntryOptions.Default with { L2Ttl = TimeSpan.FromSeconds(100), L1Ttl = TimeSpan.FromSeconds(100), JitterRatio = 0 };
        var a = NewCache("a", c => c.DefaultEntryOptions = o);
        var reasons = new List<FactoryReason>();
        var version = 0;
        var refreshGate = new TaskCompletionSource();
        Func<FactoryContext, CancellationToken, ValueTask<int>> factory = async (c, _) =>
        {
            lock (reasons) reasons.Add(c.Reason);
            if (c.Reason == FactoryReason.EarlyRefresh) await refreshGate.Task; // finish only after the reads below
            return Interlocked.Increment(ref version);
        };

        Assert.Equal(1, await a.GetOrCreateAsync("orders.1", factory));
        _time.Advance(TimeSpan.FromSeconds(85));
        Assert.Equal(1, await a.GetOrCreateAsync("orders.1", factory)); // 15 % left: no refresh yet
        _time.Advance(TimeSpan.FromSeconds(6)); // 9 % left

        var hits = await Task.WhenAll(Enumerable.Range(0, 50).Select(_ => a.GetOrCreateAsync("orders.1", factory).AsTask()));
        Assert.All(hits, v => Assert.Equal(1, v)); // callers keep the current value
        refreshGate.SetResult();
        await DistributedLockTests.Eventually(() => _l2.Puts.Count == 2, "refresh written");
        await DistributedLockTests.Eventually(() => _locks.Peek("orders.1._s1")?.Op == L2Op.Delete, "refresh lock released");

        Assert.Equal([FactoryReason.Miss, FactoryReason.EarlyRefresh], reasons);
        Assert.Equal(_l2.Puts[0].Result.Seq, _l2.Puts[1].Expected); // fenced on the value it replaces
        Assert.Equal(2, await a.GetOrCreateAsync("orders.1", factory));
        Assert.Equal(2, await NewCache("b", c => c.DefaultEntryOptions = o).GetOrCreateAsync("orders.1", factory));
    }

    [Fact]
    public async Task Lock_store_outage_degrades_like_any_other_l2_outage()
    {
        _locks.Unavailable = true;
        FactoryContext? ctx = null;
        Assert.Equal("local", await NewCache("a").GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("local"); }));
        Assert.Equal(FactoryReason.Degraded, ctx!.Reason);
    }

    [Fact]
    public async Task Live_token_keeps_the_waiter_past_lock_wait_timeout_and_the_lock_hands_over_on_expiry()
    {
        // A hung owner with a 20 s lease that never renews: no revision change at the 15 s LockWaitTimeout, but its
        // token is live, so the waiter keeps waiting and takes over through the lock instead of running without it.
        await HoldLock("orders.1._s1", TimeSpan.FromSeconds(20));
        FactoryContext? ctx = null;
        var t = NewCache("b").GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("v"); }).AsTask();

        var (value, elapsed) = await Drive(t);

        Assert.Equal("v", value);
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason);
        Assert.StartsWith("b:", ctx.LockToken);
        Assert.InRange(elapsed, TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(21));
    }

    [Fact]
    public async Task Owner_whose_factory_ignores_its_timeout_hands_over_through_the_lock_and_the_fence_keeps_one_value()
    {
        var a = NewCache("a");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var calls = 0;
        // FactoryTimeout 10 s, LeaseTtl 20 s: a renews until 30 s, so its lock expires by 50 s at the latest.
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) =>
        {
            Interlocked.Increment(ref calls);
            started.SetResult();
            await gate.Task; // ignores the cancellation
            return "late";
        }, CacheEntryOptions.Default with { LeaseTtl = TimeSpan.FromSeconds(20) }).AsTask();
        await started.Task;

        FactoryContext? ctx = null;
        var tb = NewCache("b").GetOrCreateAsync("orders.1", (c, _) => { Interlocked.Increment(ref calls); ctx = c; return new ValueTask<string>("b"); }).AsTask();
        var (value, elapsed) = await Drive(tb, max: TimeSpan.FromSeconds(90));

        Assert.Equal("b", value);
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason); // never a lock-less run
        Assert.InRange(elapsed, TimeSpan.FromSeconds(40), TimeSpan.FromSeconds(51));

        gate.SetResult();
        Assert.Equal("b", await ta); // a's late write is fenced: it returns the newer value
        Assert.Equal(2, calls);
        Assert.Single(_l2.PutsSnapshot(), p => p.Result.Status == WriteStatus.Committed);
    }

    [Fact]
    public async Task Lost_lease_double_execution_is_resolved_by_the_fence()
    {
        var a = NewCache("a");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) =>
        {
            started.SetResult();
            await gate.Task;
            return "stale";
        }).AsTask();
        await started.Task;

        _locks.Unavailable = true; // a partition: a's renewals fail and its 12 s lease runs out
        await DriveFor(TimeSpan.FromSeconds(13));
        _locks.Unavailable = false;

        var vb = (await Drive(NewCache("b").GetOrCreateAsync("orders.1", (_, _) => new ValueTask<string>("fresh")).AsTask())).Value;
        Assert.Equal("fresh", vb);

        gate.SetResult();
        Assert.Equal("fresh", await ta); // fenced-newer: no node ever serves the stale value
        Assert.Equal("fresh", (await NewCache("c").TryGetAsync<string>("orders.1")).Value);
    }

    [Fact]
    public async Task Rogue_owner_past_its_bound_makes_the_waiter_apply_failure_mode_open()
    {
        // The owner's token says FactoryTimeout 1 s, LeaseTtl 6 s: waiters allow 1 + 2 × 6 s after first seeing it.
        var rogue = await HoldLock("orders.1._s1", TimeSpan.FromSeconds(6), TimeSpan.FromSeconds(1));
        FactoryContext? ctx = null;
        var t = NewCache("b").GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("v"); }).AsTask();

        var (value, elapsed) = await DriveWithRogueOwner(t, rogue);

        Assert.Equal("v", value);
        Assert.Equal(FactoryReason.Degraded, ctx!.Reason);
        Assert.Null(ctx.LockToken);
        Assert.InRange(elapsed, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(16)); // LockWaitTimeout, past the 13 s bound
        Assert.Equal(0UL, Assert.Single(_l2.PutsSnapshot()).Expected); // still a fenced write
    }

    [Fact]
    public async Task Rogue_owner_past_its_bound_in_closed_mode_throws_a_lock_timeout()
    {
        var rogue = await HoldLock("orders.1._s1", TimeSpan.FromSeconds(6), TimeSpan.FromSeconds(1));
        var t = NewCache("b", o => o.FailureMode = FailureMode.Closed).GetOrCreateAsync("orders.1", (_, _) => new ValueTask<string>("v")).AsTask();
        var ex = await Assert.ThrowsAsync<CacheUnavailableException>(() => DriveWithRogueOwner(t, rogue));
        Assert.Contains("distributed lock", ex.Message);
        Assert.Empty(_l2.PutsSnapshot());
    }

    [Fact]
    public async Task Waiter_follows_an_owner_change_and_counts_it_in_the_attempt()
    {
        var first = await HoldLock("orders.1._s1", TimeSpan.FromSeconds(12));
        FactoryContext? ctx = null;
        var t = NewCache("b").GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("v"); }).AsTask();
        await DriveFor(TimeSpan.FromSeconds(5));

        // Another node takes the lock straight from the first owner (no free window), then crashes too.
        var second = LockToken.New("second", _time.GetUtcNow(), TimeSpan.FromSeconds(12), TimeSpan.FromSeconds(10));
        var swap = await _locks.PutAsync("orders.1._s1", System.Text.Encoding.UTF8.GetBytes(second.ToString()),
            new Dictionary<string, string>(), TimeSpan.FromSeconds(12), first.Revision, default);
        Assert.Equal(WriteStatus.Committed, swap.Status);

        var (_, elapsed) = await Drive(t);
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason);
        Assert.Equal(3, ctx.Attempt);
        Assert.InRange(elapsed, TimeSpan.FromSeconds(12), TimeSpan.FromSeconds(13)); // 17 s in total
    }

    [Fact]
    public async Task Stale_direct_reads_of_the_lock_key_do_not_delay_a_takeover()
    {
        var a = NewCache("a");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync<string>("orders.1", async (_, _) =>
        {
            started.SetResult();
            await gate.Task;
            throw new InvalidOperationException("boom");
        }).AsTask();
        await started.Task;
        _locks.StaleDirectReads = true; // a lagging replica keeps showing a's token after the release
        var tb = NewCache("b").GetOrCreateAsync("orders.1", (_, _) => new ValueTask<string>("b")).AsTask();
        await DriveFor(TimeSpan.FromSeconds(1));
        gate.SetResult();
        await Assert.ThrowsAsync<InvalidOperationException>(() => ta);

        var (value, elapsed) = await Drive(tb);
        Assert.Equal("b", value);
        Assert.True(elapsed < TimeSpan.FromSeconds(1), $"took {elapsed}");
    }

    [Fact]
    public async Task Lock_is_released_when_the_double_check_finds_a_value()
    {
        var writer = NewCache("writer");
        await writer.RemoveAsync("orders.1");
        await writer.SetAsync("orders.1", "set-meanwhile");
        _l2.StaleDirectReads = true; // b's Direct Get still sees the tombstone, the leader sees the value

        var calls = 0;
        var v = await NewCache("b").GetOrCreateAsync("orders.1", (_, _) => { calls++; return new ValueTask<string>("b"); });

        Assert.Equal("set-meanwhile", v);
        Assert.Equal(0, calls);
        Assert.Equal(L2Op.Delete, _locks.Peek("orders.1._s1")!.Op);
    }

    [Fact]
    public async Task Lock_is_released_when_the_factory_times_out()
    {
        var o = CacheEntryOptions.Default with { FactoryTimeout = TimeSpan.FromMilliseconds(100) };
        var t = NewCache("a").GetOrCreateAsync("orders.1", async (_, ct) =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return "never";
        }, o).AsTask();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => t);
        Assert.Equal(L2Op.Delete, _locks.Peek("orders.1._s1")!.Op);
    }

    [Fact]
    public async Task Outage_during_the_fenced_write_abandons_the_lease_instead_of_waiting_for_a_release()
    {
        var v = await NewCache("a").GetOrCreateAsync("orders.1", (_, _) =>
        {
            _l2.Unavailable = true; // NATS goes away while the factory runs
            return new ValueTask<string>("local");
        });

        Assert.Equal("local", v);
        Assert.Equal(L2Op.Put, _locks.Peek("orders.1._s1")!.Op); // left to expire with its lease
    }

    [Fact]
    public async Task Disposing_a_waiting_node_cancels_the_wait()
    {
        await HoldLock("orders.1._s1", TimeSpan.FromSeconds(12));
        var b = NewCache("b");
        var t = b.GetOrCreateAsync("orders.1", (_, _) => new ValueTask<string>("v")).AsTask();
        await DriveFor(TimeSpan.FromSeconds(1));
        await b.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => t);
    }

    [Fact]
    public async Task Disposing_an_owner_waits_for_its_factory_to_write_and_release()
    {
        var a = NewCache("a");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) =>
        {
            started.SetResult();
            await gate.Task;
            return "done";
        }).AsTask();
        await started.Task;

        var dispose = a.DisposeAsync().AsTask();
        await Task.Delay(100);
        Assert.False(dispose.IsCompleted); // the lock is not dropped under a running factory
        gate.SetResult();
        await dispose;

        Assert.Equal("done", await ta);
        Assert.Equal(L2Op.Delete, _locks.Peek("orders.1._s1")!.Op);
        Assert.Equal(L2Op.Put, _l2.Peek("orders.1._s1")!.Op);
    }

    [Fact]
    public async Task Early_refresh_that_loses_the_lock_is_a_no_op()
    {
        var o = CacheEntryOptions.Default with { L2Ttl = TimeSpan.FromSeconds(100), L1Ttl = TimeSpan.FromSeconds(100), JitterRatio = 0 };
        var a = NewCache("a", c => c.DefaultEntryOptions = o);
        var calls = 0;
        Func<FactoryContext, CancellationToken, ValueTask<int>> factory = (_, _) => new ValueTask<int>(Interlocked.Increment(ref calls));
        await a.GetOrCreateAsync("orders.1", factory);
        _time.Advance(TimeSpan.FromSeconds(91));
        await HoldLock("orders.1._s1", TimeSpan.FromSeconds(60)); // another node is refreshing
        var before = _locks.PutsSnapshot().Count;

        Assert.Equal(1, await a.GetOrCreateAsync("orders.1", factory));
        // The refresh's acquire is rejected (held); once that publish is seen the refresh can only return.
        await DistributedLockTests.Eventually(() => _locks.PutsSnapshot().Count == before + 1, "refresh attempted the lock");

        Assert.Equal(1, calls);
        Assert.Single(_l2.PutsSnapshot());
        Assert.Equal(1, await a.GetOrCreateAsync("orders.1", factory)); // still served, no waiting, no second attempt
        Assert.Equal(before + 1, _locks.PutsSnapshot().Count);
    }

    [Fact]
    public async Task Early_refresh_ratio_zero_turns_it_off_per_call()
    {
        var o = CacheEntryOptions.Default with { L2Ttl = TimeSpan.FromSeconds(100), L1Ttl = TimeSpan.FromSeconds(100), JitterRatio = 0 };
        var a = NewCache("a", c => c.DefaultEntryOptions = o);
        var reasons = new List<FactoryReason>();
        Func<FactoryContext, CancellationToken, ValueTask<int>> factory = (c, _) => { lock (reasons) reasons.Add(c.Reason); return new ValueTask<int>(reasons.Count); };
        await a.GetOrCreateAsync("orders.1", factory);
        _time.Advance(TimeSpan.FromSeconds(95));

        await a.GetOrCreateAsync("orders.1", factory, o with { EarlyRefreshRatio = 0 }); // must not claim the refresh
        await a.GetOrCreateAsync("orders.1", factory); // so this call still starts it
        await DistributedLockTests.Eventually(() => _l2.PutsSnapshot().Count == 2, "refreshed by the second call");
        Assert.Equal([FactoryReason.Miss, FactoryReason.EarlyRefresh], reasons);
    }
}
