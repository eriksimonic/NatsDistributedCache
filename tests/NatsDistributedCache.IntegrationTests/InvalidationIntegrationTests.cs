using System.Diagnostics;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.IntegrationTests;

/// <summary>
/// Invalidation against real NATS (milestone 4, design section 7): events across nodes, waiter wake-ups, reconnect
/// resume and a recreated notifications stream. Each node has its own connection. L1Ttl is 30 s, so every
/// convergence asserted within <see cref="Bound"/> comes from an event, not from L1 expiry.
/// </summary>
public sealed class InvalidationIntegrationTests(NatsFixture nats) : IAsyncLifetime
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

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

    private async Task<(NatsCache Cache, NatsConnection Connection)> NodeAsync(string node)
    {
        var nc = await ConnectAsync();
        var cache = new NatsCache(new NatsCacheOptions { Prefix = _prefix, NodeId = node, Replicas = 1 }, nc);
        _disposables.Add(cache);
        cache.Start();
        for (var i = 0; i < 100 && !cache.IsReady && cache.ProvisioningError is null; i++) await Task.Delay(100);
        Assert.True(cache.IsReady, cache.ProvisioningError?.Message);
        await cache.NotificationsLive.WaitAsync(Bound);
        return (cache, nc);
    }

    private static async Task Eventually(Func<Task<bool>> condition, string what, TimeSpan? bound = null)
    {
        var limit = bound ?? Bound;
        var sw = Stopwatch.StartNew();
        var ok = await condition();
        while (!ok && sw.Elapsed < limit)
        {
            await Task.Delay(20);
            ok = await condition();
        }

        Assert.True(ok, $"{what} within {limit.TotalSeconds} s");
    }

    private static async Task<string?> Read(NatsCache cache, string key) => (await cache.TryGetAsync<string>(key)).Value;

    [Fact]
    public async Task Set_del_tag_and_clear_events_reach_the_other_node()
    {
        var (a, _) = await NodeAsync("a");
        var (b, _) = await NodeAsync("b");
        await a.SetAsync("orders.1", "v1");
        await a.SetAsync("orders.2", "v1");
        await a.SetAsync("users.1", "v1");
        Assert.Equal("v1", await Read(b, "orders.1")); // b now holds L1 copies
        Assert.Equal("v1", await Read(b, "orders.2"));
        Assert.Equal("v1", await Read(b, "users.1"));

        await a.SetAsync("orders.1", "v2");
        await Eventually(async () => await Read(b, "orders.1") == "v2", "set evicted b's copy");

        await a.RemoveAsync("orders.1");
        await Eventually(async () => !(await b.TryGetAsync<string>("orders.1")).Found, "del evicted b's copy");

        await a.RemoveByTagAsync("orders");
        await Eventually(async () => !(await b.TryGetAsync<string>("orders.2")).Found, "tag evicted b's copy");

        // clear: change L2 behind b's back (a raw write, no event), then clear; b must re-read L2.
        var l2 = new NatsL2Store(new NatsJSContext(await ConnectAsync()), $"{_prefix}_cache");
        var head = await l2.ReadAsync("users.1._s1", leader: true, default);
        Assert.NotNull(head);
        var put = await l2.PutAsync("users.1._s1", "\"raw\""u8.ToArray(), head.Headers.Where(h => h.Key.StartsWith("x-cache", StringComparison.Ordinal)).ToDictionary(),
            TimeSpan.FromMinutes(11), head.Revision, default);
        Assert.Equal(WriteStatus.Committed, put.Status);
        Assert.Equal("v1", await Read(b, "users.1")); // still b's L1 copy

        await a.ClearAsync();
        await Eventually(async () => await Read(b, "users.1") == "raw", "clear flushed b's L1");
    }

    [Fact]
    public async Task Waiter_on_another_node_is_woken_by_the_set_event_before_its_poll()
    {
        var (a, _) = await NodeAsync("a");
        var (b, _) = await NodeAsync("b");
        var started = new TaskCompletionSource();
        var gate = new TaskCompletionSource();
        var ta = a.GetOrCreateAsync("orders.1", async (_, _) => { started.SetResult(); await gate.Task; return "from-a"; }).AsTask();
        await started.Task;
        var tb = b.GetOrCreateAsync("orders.1", (_, _) => new ValueTask<string>("from-b")).AsTask();
        await Task.Delay(1_000); // b lost the acquire and parks on the key's signal

        var sw = Stopwatch.StartNew();
        gate.SetResult();
        Assert.Equal("from-a", await ta);
        var first = sw.Elapsed;
        Assert.Equal("from-a", await tb);
        var waited = sw.Elapsed - first;

        // The 250 ms poll is the fallback; the set event wakes the waiter well before it in the common case.
        Assert.True(waited < NatsCache.PollInterval, $"b got the value {waited.TotalMilliseconds:F0} ms after a: woken by a poll, not the event");
    }

    [Fact]
    public async Task Events_published_while_a_node_reconnects_are_applied_after_it()
    {
        var (a, _) = await NodeAsync("a");
        var (b, bConnection) = await NodeAsync("b");
        await a.SetAsync("orders.1", "v1");
        Assert.Equal("v1", await Read(b, "orders.1"));

        var reconnect = bConnection.ReconnectAsync();
        await a.SetAsync("orders.1", "v2"); // published while b's connection is down or just back
        await reconnect;

        await Eventually(async () => await Read(b, "orders.1") == "v2", "b applied the event after reconnecting");
    }

    [Fact]
    public async Task Recreated_notifications_stream_flushes_l1()
    {
        var (a, _) = await NodeAsync("a");
        var (b, _) = await NodeAsync("b");
        await a.SetAsync("users.1", "v1");
        Assert.Equal("v1", await Read(b, "users.1"));
        await Eventually(() => Task.FromResult(b.LastEventSeen == 1), "b applied the first event");

        // Change L2 behind b's back, then delete and recreate the notifications stream (e.g. a wiped cluster).
        var js = new NatsJSContext(await ConnectAsync());
        var l2 = new NatsL2Store(js, $"{_prefix}_cache");
        var head = (await l2.ReadAsync("users.1._s1", leader: true, default))!;
        await l2.PutAsync("users.1._s1", "\"raw\""u8.ToArray(), head.Headers.Where(h => h.Key.StartsWith("x-cache", StringComparison.Ordinal)).ToDictionary(),
            TimeSpan.FromMinutes(11), head.Revision, default);
        var config = new Provisioner(new NatsCacheOptions { Prefix = _prefix, Replicas = 1 }).NotificationsConfig();
        await js.DeleteStreamAsync(config.Name!);
        await js.CreateStreamAsync(config);
        await a.SetAsync("other.1", "x"); // the new stream starts again at sequence 1

        // The ordered consumer follows the new stream silently; the 5 s position check finds the new creation time.
        await Eventually(async () => await Read(b, "users.1") == "raw", "b flushed L1 after the stream was recreated", TimeSpan.FromSeconds(15));
    }

    [Fact]
    public async Task Node_that_has_processed_no_event_converges_after_a_reconnect()
    {
        var (a, _) = await NodeAsync("a");
        await a.SetAsync("orders.1", "v1");
        var (b, bConnection) = await NodeAsync("b"); // subscribed after the only event: has processed none
        Assert.Equal("v1", await Read(b, "orders.1"));

        var reconnect = bConnection.ReconnectAsync();
        await a.SetAsync("orders.1", "v2");
        await reconnect;

        await Eventually(async () => await Read(b, "orders.1") == "v2", "b converged");
    }
}

[CollectionDefinition(nameof(FlushCounting), DisableParallelization = true)]
public sealed class FlushCounting;

/// <summary>
/// The flush counter is process-wide (every cache's meter shares one name), so this runs alone: other classes flush
/// on purpose.
/// </summary>
[Collection(nameof(FlushCounting))]
public sealed class SteadyStateIntegrationTests(NatsFixture nats) : IAsyncLifetime
{
    private readonly List<IAsyncDisposable> _disposables = [];
    private readonly string _prefix = NatsFixture.NewPrefix();

    public ValueTask InitializeAsync() => default;

    public async ValueTask DisposeAsync()
    {
        for (var i = _disposables.Count - 1; i >= 0; i--) await _disposables[i].DisposeAsync();
    }

    private async Task<NatsCache> NodeAsync(string node)
    {
        var nc = await nats.ConnectAsync();
        _disposables.Add(nc);
        var cache = new NatsCache(new NatsCacheOptions { Prefix = _prefix, NodeId = node, Replicas = 1 }, nc);
        _disposables.Add(cache);
        cache.Start();
        for (var i = 0; i < 100 && !cache.IsReady && cache.ProvisioningError is null; i++) await Task.Delay(100);
        Assert.True(cache.IsReady, cache.ProvisioningError?.Message);
        await cache.NotificationsLive.WaitAsync(TimeSpan.FromSeconds(5));
        return cache;
    }

    [Fact]
    public async Task Steady_writes_never_trigger_a_flush()
    {
        var a = await NodeAsync("a");
        var b = await NodeAsync("b");
        long flushes = 0;
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "NatsDistributedCache" && instrument.Name == "cache.l1.flushes") l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref flushes, value));
        listener.Start();

        var sw = Stopwatch.StartNew();
        for (var i = 0; sw.Elapsed < TimeSpan.FromSeconds(12); i++) // more than two 5 s position checks
        {
            await a.SetAsync($"orders.{i % 50}", $"v{i}");
            await b.TryGetAsync<string>($"orders.{i % 50}");
            await Task.Delay(20);
        }

        Assert.Equal(0, Interlocked.Read(ref flushes));
    }
}
