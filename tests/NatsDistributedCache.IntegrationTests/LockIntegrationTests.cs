using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.IntegrationTests;

/// <summary>Distributed single-flight against real NATS (milestone 3): one connection per simulated node.</summary>
public sealed class LockIntegrationTests(NatsFixture nats) : IAsyncLifetime
{
    private readonly List<IAsyncDisposable> _disposables = [];
    private string _prefix = null!;

    public ValueTask InitializeAsync()
    {
        _prefix = NatsFixture.NewPrefix();
        return default;
    }

    public async ValueTask DisposeAsync()
    {
        for (var i = _disposables.Count - 1; i >= 0; i--) await _disposables[i].DisposeAsync();
    }

    private async Task<NatsConnection> ConnectAsync()
    {
        var nc = await nats.ConnectAsync();
        _disposables.Add(nc);
        return nc;
    }

    private async Task<NatsCache> NodeAsync(string node, Action<NatsCacheOptions>? configure = null)
    {
        var o = new NatsCacheOptions { Prefix = _prefix, NodeId = node, Replicas = 1 };
        configure?.Invoke(o);
        var cache = new NatsCache(o, await ConnectAsync());
        _disposables.Add(cache);
        cache.Start();
        for (var i = 0; i < 100 && !cache.IsReady && cache.ProvisioningError is null; i++) await Task.Delay(100);
        Assert.True(cache.IsReady);
        return cache;
    }

    private async Task<DistributedLock> LockAsync(string node) =>
        new(new NatsL2Store(new NatsJSContext(await ConnectAsync()), $"{_prefix}_locks"), node, TimeProvider.System, NullLogger.Instance);

    [Fact]
    public async Task Five_nodes_racing_on_one_key_run_the_factory_exactly_once()
    {
        var nodes = new List<NatsCache>();
        for (var i = 0; i < 5; i++) nodes.Add(await NodeAsync($"n{i}"));
        var calls = 0;
        var start = new TaskCompletionSource();

        var tasks = nodes.SelectMany(n => Enumerable.Range(0, 20).Select(async _ =>
        {
            await start.Task;
            return await n.GetOrCreateAsync("orders.1", async (ctx, ct) =>
            {
                Interlocked.Increment(ref calls);
                await Task.Delay(500, ct);
                return ctx.LockToken!;
            });
        })).ToList();
        start.SetResult();
        var results = await Task.WhenAll(tasks);

        Assert.Equal(1, calls);
        Assert.Single(results.Distinct());
        Assert.StartsWith("n", results[0]);
    }

    [Fact]
    public async Task Many_keys_across_nodes_run_each_factory_once()
    {
        var nodes = new List<NatsCache>();
        for (var i = 0; i < 3; i++) nodes.Add(await NodeAsync($"n{i}"));
        var calls = new int[50];

        await Task.WhenAll(nodes.SelectMany(n => Enumerable.Range(0, calls.Length).Select(k => n.GetOrCreateAsync($"items.{k}", async (_, ct) =>
        {
            Interlocked.Increment(ref calls[k]);
            await Task.Delay(100, ct);
            return k;
        }).AsTask())));

        Assert.All(calls, c => Assert.Equal(1, c));
    }

    [Fact]
    public async Task Crashed_owner_is_taken_over_after_its_lease_expires_on_the_server()
    {
        var node = await NodeAsync("b");
        var crashed = await (await LockAsync("crashed")).TryAcquireAsync("orders.1._s1", TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), default);
        Assert.NotNull(crashed.Lease); // never renewed, never released

        FactoryContext? ctx = null;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var v = await node.GetOrCreateAsync("orders.1", (c, _) => { ctx = c; return new ValueTask<string>("v"); });

        Assert.Equal("v", v);
        Assert.Equal(FactoryReason.Takeover, ctx!.Reason);
        Assert.InRange(sw.Elapsed, TimeSpan.FromSeconds(1.5), TimeSpan.FromSeconds(6)); // server TTL, then a 250 ms poll
    }

    [Fact]
    public async Task Renewal_keeps_a_short_lease_alive_past_its_ttl()
    {
        // A 2 s lease is below the 6 s MinLeaseTtl floor; allowed here to keep the test short.
        var a = await NodeAsync("a", c => c.MinLeaseTtl = TimeSpan.FromSeconds(1));
        var b = await NodeAsync("b", c => c.MinLeaseTtl = TimeSpan.FromSeconds(1));
        var o = CacheEntryOptions.Default with { LeaseTtl = TimeSpan.FromSeconds(2) }; // renewed every ~667 ms
        var started = new TaskCompletionSource();
        var calls = 0;

        var ta = a.GetOrCreateAsync("orders.1", async (_, ct) =>
        {
            Interlocked.Increment(ref calls);
            started.SetResult();
            await Task.Delay(5000, ct); // 2.5 leases
            return "slow";
        }, o).AsTask();
        await started.Task;
        var vb = await b.GetOrCreateAsync("orders.1", (_, _) => { Interlocked.Increment(ref calls); return new ValueTask<string>("b"); }, o);

        Assert.Equal("slow", vb);
        Assert.Equal("slow", await ta);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Lock_primitives_behave_on_nats_like_the_spike_measured()
    {
        await NodeAsync("provisioner");
        var la = await LockAsync("a");
        var lb = await LockAsync("b");
        var a = (await la.TryAcquireAsync("k", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), default)).Lease!;
        var held = await lb.TryAcquireAsync("k", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), default);
        Assert.Null(held.Lease);
        Assert.Equal(a.Token, LockToken.Parse(held.Holder!.Payload));

        await a.DisposeAsync();
        var head = await la.ReadAsync("k", leader: true, default);
        Assert.Equal(L2Op.Delete, head!.Op);

        // Free after the release: the second acquire expects the tombstone's sequence and wins.
        var b = (await lb.TryAcquireAsync("k", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), default)).Lease!;
        Assert.True(b.Revision > head.Revision);

        // A stale release (a's old revision) is rejected and leaves b's lock in place.
        Assert.False(await la.ReleaseAsync(a, a.Revision));
        Assert.Equal(b.Revision, (await la.ReadAsync("k", leader: true, default))!.Revision);
        await b.DisposeAsync();
    }

    [Fact]
    public async Task Ttl_marker_after_expiry_counts_as_free()
    {
        await NodeAsync("provisioner");
        var la = await LockAsync("a");
        Assert.NotNull((await la.TryAcquireAsync("k", TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), default)).Lease);
        L2Entry? head = null;
        for (var i = 0; i < 50 && head?.Op != L2Op.Marker; i++)
        {
            await Task.Delay(100);
            head = await la.ReadAsync("k", leader: true, default);
        }

        Assert.Equal(L2Op.Marker, head!.Op);
        var b = (await (await LockAsync("b")).TryAcquireAsync("k", TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), default)).Lease;
        Assert.NotNull(b);
        Assert.True(b!.Revision > head.Revision);
        await b.DisposeAsync();
    }

    [Fact]
    public async Task Reading_a_missing_bucket_is_an_outage_not_a_raw_api_error()
    {
        var store = new NatsL2Store(new NatsJSContext(await ConnectAsync()), $"{_prefix}_locks"); // never provisioned
        await Assert.ThrowsAsync<L2UnavailableException>(() => store.ReadAsync("k", leader: true, default).AsTask());
    }

    [Fact]
    public async Task Lost_lease_double_execution_is_resolved_by_the_fence_on_nats()
    {
        var a = await NodeAsync("a");
        var b = await NodeAsync("b");
        var key = "orders.1._s1";
        // Simulate a's lease running out mid-factory: a holds the lock, then the lock is taken from it.
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) =>
        {
            started.SetResult();
            await gate.Task;
            return "stale";
        }).AsTask();
        await started.Task;

        var locks = new NatsL2Store(new NatsJSContext(await ConnectAsync()), $"{_prefix}_locks");
        Assert.Equal(WriteStatus.Committed, (await locks.DeleteAsync(key, expected: null, default)).Status); // a's lease is gone
        Assert.Equal("fresh", await b.GetOrCreateAsync("orders.1", (_, _) => new ValueTask<string>("fresh")));

        gate.SetResult();
        Assert.Equal("fresh", await ta); // fenced-newer
        Assert.Equal("fresh", (await (await NodeAsync("c")).TryGetAsync<string>("orders.1")).Value);
    }
}
