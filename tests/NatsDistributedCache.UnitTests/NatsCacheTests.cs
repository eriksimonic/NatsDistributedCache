using Microsoft.Extensions.Time.Testing;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

public sealed class NatsCacheTests : IAsyncDisposable
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 9, 30, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeL2Store _l2;
    private readonly FakeL2Store _locks;
    private readonly FakeNotificationTransport _notify = new();
    private readonly List<NatsCache> _caches = [];

    public NatsCacheTests()
    {
        _l2 = new FakeL2Store(_time);
        _locks = new FakeL2Store(_time);
    }

    private NatsCache NewCache(Action<NatsCacheOptions>? configure = null, Func<CancellationToken, Task>? provision = null, string node = "node-1")
    {
        var o = new NatsCacheOptions { Prefix = "test", NodeId = node, CompressionThresholdBytes = 1024 };
        configure?.Invoke(o);
        var cache = new NatsCache(o, _l2, _locks, _notify, provision, null, _time, null, new FixedRandom(0.5));
        _caches.Add(cache);
        return cache;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _caches) await c.DisposeAsync();
    }

    private static Func<FactoryContext, CancellationToken, ValueTask<T>> Factory<T>(T value, Action<FactoryContext>? seen = null) =>
        (ctx, _) => { seen?.Invoke(ctx); return new ValueTask<T>(value); };

    [Fact]
    public async Task Miss_runs_the_factory_once_writes_l2_expecting_empty_and_fills_l1()
    {
        var cache = NewCache();
        FactoryContext? ctx = null;
        var v = await cache.GetOrCreateAsync("orders.1", Factory("a", c => ctx = c));

        Assert.Equal("a", v);
        Assert.Equal(FactoryReason.Miss, ctx!.Reason);
        Assert.Equal(1, ctx.Attempt);
        Assert.StartsWith("node-1:", ctx.LockToken); // the factory runs under the distributed lock
        Assert.Equal(L2Op.Delete, _locks.Peek("orders.1._s1")!.Op); // released after the write
        var put = Assert.Single(_l2.Puts);
        Assert.Equal("orders.1._s1", put.Key);
        Assert.Equal(0UL, put.Expected);
        Assert.Equal(WriteStatus.Committed, put.Result.Status);
        Assert.Equal("600000", put.Headers[NatsCache.HeaderTtl]);
        Assert.Equal("node-1", put.Headers[NatsCache.HeaderNode]);
        Assert.Equal("1", put.Headers[NatsCache.HeaderSchema]);
        Assert.Equal(TimeSpan.FromSeconds(622), put.NatsTtl);

        var reads = _l2.DirectReads + _l2.LeaderReads;
        Assert.Equal("a", await cache.GetOrCreateAsync("orders.1", Factory("b")));
        Assert.Equal(reads, _l2.DirectReads + _l2.LeaderReads); // served from L1
    }

    [Fact]
    public async Task Concurrent_callers_on_one_node_run_the_factory_once()
    {
        var cache = NewCache();
        var calls = 0;
        var gate = new TaskCompletionSource();
        var tasks = Enumerable.Range(0, 200).Select(_ => cache.GetOrCreateAsync("orders.1", async (_, _) =>
        {
            Interlocked.Increment(ref calls);
            await gate.Task;
            return 42;
        }).AsTask()).ToList();
        await Task.Delay(100);
        gate.SetResult();
        Assert.All(await Task.WhenAll(tasks), v => Assert.Equal(42, v));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Another_node_reads_the_value_from_l2_without_running_its_factory()
    {
        await NewCache(node: "a").GetOrCreateAsync("orders.1", Factory("from-a"));
        var calls = 0;
        var v = await NewCache(node: "b").GetOrCreateAsync("orders.1", Factory("from-b", _ => calls++));
        Assert.Equal("from-a", v);
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task Logically_expired_value_is_a_miss_and_the_write_fences_on_its_revision()
    {
        var a = NewCache(node: "a");
        await a.GetOrCreateAsync("orders.1", Factory("v1"));
        var firstRev = _l2.Puts[0].Result.Seq;

        _time.Advance(TimeSpan.FromSeconds(601)); // past x-cache-ttl (600 s), inside the 22 s grace
        Assert.Equal(L2Op.Put, _l2.Peek("orders.1._s1")!.Op);
        var v = await NewCache(node: "b").GetOrCreateAsync("orders.1", Factory("v2"));

        Assert.Equal("v2", v);
        Assert.Equal(firstRev, _l2.Puts[1].Expected);
        Assert.Equal(WriteStatus.Committed, _l2.Puts[1].Result.Status);
    }

    [Fact]
    public async Task Ttl_marker_counts_as_free_and_the_write_expects_the_marker_revision()
    {
        await NewCache(node: "a").GetOrCreateAsync("orders.1", Factory("v1"));
        _time.Advance(TimeSpan.FromSeconds(623)); // past Nats-TTL: the server wrote a MaxAge marker
        var marker = _l2.Peek("orders.1._s1")!;
        Assert.Equal(L2Op.Marker, marker.Op);

        Assert.Equal("v2", await NewCache(node: "b").GetOrCreateAsync("orders.1", Factory("v2")));
        Assert.Equal(marker.Revision, _l2.Puts[1].Expected);
        Assert.Equal(WriteStatus.Committed, _l2.Puts[1].Result.Status);
    }

    [Fact]
    public async Task Fenced_by_a_concurrent_set_returns_the_newer_value()
    {
        var writer = NewCache(node: "writer");
        var reader = NewCache(node: "reader");
        _l2.BeforePut = async _ => await writer.SetAsync("orders.1", "newer");

        var v = await reader.GetOrCreateAsync("orders.1", Factory("stale"));

        Assert.Equal("newer", v);
        Assert.Contains(_l2.Puts, p => p.Result.IsWrongLastSequence);
    }

    [Fact]
    public async Task Fenced_by_a_concurrent_delete_returns_the_factory_result_uncached()
    {
        await NewCache(node: "a").SetAsync("orders.1", "old");
        _time.Advance(TimeSpan.FromSeconds(601)); // expired, so node b loads
        var remover = NewCache(node: "remover");
        var b = NewCache(node: "b");
        _l2.BeforePut = async _ => await remover.RemoveAsync("orders.1");

        Assert.Equal("fresh", await b.GetOrCreateAsync("orders.1", Factory("fresh")));
        Assert.Equal(L2Op.Delete, _l2.Peek("orders.1._s1")!.Op); // not resurrected
        var calls = 0;
        await b.GetOrCreateAsync("orders.1", Factory("again", _ => calls++));
        Assert.Equal(1, calls); // the uncached result was not put in L1 either
    }

    [Fact]
    public async Task Unknown_outcome_is_settled_by_the_msg_id_at_the_head_of_the_subject()
    {
        var cache = NewCache();
        _l2.NextPutLosesReply = true;
        Assert.Equal("v", await cache.GetOrCreateAsync("orders.1", Factory("v")));
        Assert.Single(_l2.Puts); // no second write
        var calls = 0;
        await cache.GetOrCreateAsync("orders.1", Factory("x", _ => calls++));
        Assert.Equal(0, calls); // L1 was filled
    }

    [Fact]
    public async Task Set_then_try_get_and_update_with_compare_and_swap()
    {
        var a = NewCache(node: "a");
        await a.SetAsync("orders.1", 10);
        var b = NewCache(node: "b");
        var r = await b.TryGetAsync<int>("orders.1");
        Assert.True(r.Found);
        Assert.Equal(10, r.Value);

        Assert.True(await b.UpdateAsync("orders.1", r.Revision, 11));
        Assert.False(await a.UpdateAsync("orders.1", r.Revision, 12)); // lost the race
        Assert.Equal(11, (await NewCache(node: "c").TryGetAsync<int>("orders.1")).Value);
        Assert.True(await a.UpdateAsync("orders.new", 0, 1)); // 0 = expect absent
    }

    [Fact]
    public async Task Remove_deletes_every_known_schema_version_and_blocks_stale_refills()
    {
        var v1 = NewCache(o => o.KnownSchemaVersions = [1, 2]);
        var v2 = NewCache(o => { o.SchemaVersion = 2; o.KnownSchemaVersions = [1, 2]; });
        await v1.SetAsync("orders.1", "one");
        await v2.SetAsync("orders.1", "two");

        await v1.RemoveAsync("orders.1");

        Assert.Equal(L2Op.Delete, _l2.Peek("orders.1._s1")!.Op);
        Assert.Equal(L2Op.Delete, _l2.Peek("orders.1._s2")!.Op);
        _l2.StaleDirectReads = true; // a lagging replica still returns the deleted value
        Assert.False((await v1.TryGetAsync<string>("orders.1")).Found);
    }

    [Fact]
    public async Task Remove_by_tag_deletes_keys_under_the_prefix_only()
    {
        var cache = NewCache();
        await cache.SetAsync("orders.42.lines", "l");
        await cache.SetAsync("orders.42.head", "h");
        await cache.SetAsync("orders.43.head", "other");

        await cache.RemoveByTagAsync("orders.42");

        Assert.False((await cache.TryGetAsync<string>("orders.42.lines")).Found);
        Assert.False((await cache.TryGetAsync<string>("orders.42.head")).Found);
        Assert.True((await cache.TryGetAsync<string>("orders.43.head")).Found);
    }

    [Fact]
    public async Task Null_is_not_cached_unless_negative_caching_is_on()
    {
        var cache = NewCache();
        var calls = 0;
        await cache.GetOrCreateAsync<string?>("orders.1", Factory<string?>(null, _ => calls++));
        await cache.GetOrCreateAsync<string?>("orders.1", Factory<string?>(null, _ => calls++));
        Assert.Equal(2, calls);
        Assert.Empty(_l2.Puts);

        var o = CacheEntryOptions.Default with { CacheNullFor = TimeSpan.FromSeconds(5) };
        await cache.GetOrCreateAsync<string?>("orders.2", Factory<string?>(null, _ => calls++), o);
        await cache.GetOrCreateAsync<string?>("orders.2", Factory<string?>(null, _ => calls++), o);
        Assert.Equal(3, calls);
        Assert.Equal("5000", Assert.Single(_l2.Puts).Headers[NatsCache.HeaderTtl]);
    }

    [Fact]
    public async Task Per_type_schema_versions_use_separate_internal_keys()
    {
        var cache = NewCache(o => o.ForType<int>(3));
        await cache.SetAsync("orders.1", 5);
        await cache.SetAsync("orders.1", "text");
        Assert.Contains(_l2.Puts, p => p.Key == "orders.1._s3");
        Assert.Contains(_l2.Puts, p => p.Key == "orders.1._s1");
        Assert.Equal(5, (await cache.TryGetAsync<int>("orders.1")).Value);
        Assert.Equal("text", (await cache.TryGetAsync<string>("orders.1")).Value);
    }

    [Fact]
    public async Task Open_mode_runs_the_factory_locally_when_l2_is_down()
    {
        var cache = NewCache();
        _l2.Unavailable = true;
        FactoryContext? ctx = null;
        Assert.Equal("local", await cache.GetOrCreateAsync("orders.1", Factory("local", c => ctx = c)));
        Assert.Equal(FactoryReason.Degraded, ctx!.Reason);
        var calls = 0;
        Assert.Equal("local", await cache.GetOrCreateAsync("orders.1", Factory("x", _ => calls++)));
        Assert.Equal(0, calls); // L1-only fill
    }

    [Fact]
    public async Task Closed_mode_throws_when_l2_is_down()
    {
        var cache = NewCache(o => o.FailureMode = FailureMode.Closed);
        _l2.Unavailable = true;
        await Assert.ThrowsAsync<CacheUnavailableException>(() => cache.GetOrCreateAsync("orders.1", Factory("v")).AsTask());
        await Assert.ThrowsAsync<CacheUnavailableException>(() => cache.SetAsync("orders.1", "v").AsTask());
        var open = CacheEntryOptions.Default with { FailureMode = FailureMode.Open };
        Assert.Equal("v", await cache.GetOrCreateAsync("orders.2", Factory("v"), open)); // per-call override
    }

    [Fact]
    public async Task Unusable_stores_leave_the_cache_degraded_without_crashing()
    {
        var cache = NewCache(provision: _ => throw new CacheProvisioningException("memory storage"));
        Assert.Equal("v", await cache.GetOrCreateAsync("orders.1", Factory("v")));
        Assert.False(cache.IsReady);
        Assert.IsType<CacheProvisioningException>(cache.ProvisioningError);
        Assert.Empty(_l2.Puts);
    }

    [Fact]
    public async Task Absolute_expiration_too_close_returns_the_value_uncached()
    {
        var cache = NewCache();
        var o = CacheEntryOptions.Default with { AbsoluteExpiration = _time.GetUtcNow().AddSeconds(30) };
        Assert.Equal("jwt", await cache.GetOrCreateAsync("tokens.1", Factory("jwt"), o));
        Assert.Empty(_l2.Puts);
    }

    [Fact]
    public async Task Large_payloads_are_compressed_and_read_back()
    {
        var big = string.Concat(Enumerable.Repeat("order-line;", 1000));
        await NewCache(node: "a").SetAsync("orders.1", big);
        Assert.Equal(PayloadCodec.Brotli, _l2.Puts[0].Headers[NatsCache.HeaderEncoding]);
        Assert.Equal(big, (await NewCache(node: "b").TryGetAsync<string>("orders.1")).Value);
    }

    [Fact]
    public async Task Values_above_the_large_value_threshold_are_returned_uncached()
    {
        var cache = NewCache(o => { o.CompressionThresholdBytes = 0; o.LargeValueThresholdBytes = 100; });
        var v = new string('x', 200);
        Assert.Equal(v, await cache.GetOrCreateAsync("orders.1", Factory(v)));
        Assert.Empty(_l2.Puts);
    }

    [Theory]
    [InlineData("user:42")]
    [InlineData("orders._s1")]
    public async Task Invalid_keys_throw(string key)
    {
        var cache = NewCache();
        await Assert.ThrowsAsync<ArgumentException>(() => cache.GetOrCreateAsync(key, Factory("v")).AsTask());
    }

    [Fact]
    public async Task Unreadable_l2_entries_are_treated_as_misses()
    {
        await NewCache(node: "a").SetAsync("orders.1", "not-a-number");
        var calls = 0;
        Assert.Equal(7, await NewCache(node: "b").GetOrCreateAsync<int>("orders.1", Factory(7, _ => calls++)));
        Assert.Equal(1, calls);
    }
}
