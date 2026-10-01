using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using NatsDistributedCache.Extensions;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.IntegrationTests;

public sealed class NatsCacheIntegrationTests(NatsFixture nats) : IAsyncLifetime
{
    private readonly List<IAsyncDisposable> _disposables = [];
    private NatsConnection _nc = null!;
    private string _prefix = null!;

    public async ValueTask InitializeAsync()
    {
        _nc = await nats.ConnectAsync();
        _prefix = NatsFixture.NewPrefix();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var d in _disposables) await d.DisposeAsync();
        await _nc.DisposeAsync();
    }

    private async Task<NatsCache> ReadyCacheAsync(string node, Action<NatsCacheOptions>? configure = null)
    {
        var o = new NatsCacheOptions { Prefix = _prefix, NodeId = node, Replicas = 1, CompressionThresholdBytes = 1024 };
        configure?.Invoke(o);
        var cache = new NatsCache(o, _nc);
        _disposables.Add(cache);
        cache.Start();
        for (var i = 0; i < 100 && !cache.IsReady && cache.ProvisioningError is null; i++) await Task.Delay(100);
        Assert.Null(cache.ProvisioningError);
        Assert.True(cache.IsReady);
        return cache;
    }

    private static Func<FactoryContext, CancellationToken, ValueTask<T>> Factory<T>(T value, Action? called = null) =>
        (_, _) => { called?.Invoke(); return new ValueTask<T>(value); };

    [Fact]
    public async Task Provisioning_creates_four_file_backed_stores_with_the_design_config()
    {
        await ReadyCacheAsync("a");
        var js = new NatsJSContext(_nc);

        foreach (var bucket in new[] { $"{_prefix}_cache", $"{_prefix}_locks" })
        {
            var c = (await js.GetStreamAsync($"KV_{bucket}")).Info.Config;
            Assert.Equal(StreamConfigStorage.File, c.Storage);
            Assert.Equal(1, c.MaxMsgsPerSubject);
            Assert.True(c.AllowMsgTTL);
            Assert.Equal(TimeSpan.FromSeconds(30), c.SubjectDeleteMarkerTTL);
        }

        Assert.Equal(TimeSpan.FromHours(24), (await js.GetStreamAsync($"KV_{_prefix}_cache")).Info.Config.MaxAge);
        Assert.Equal(TimeSpan.FromMinutes(2), (await js.GetStreamAsync($"KV_{_prefix}_locks")).Info.Config.MaxAge);
        var notify = (await js.GetStreamAsync($"{_prefix}_notifications")).Info.Config;
        Assert.Equal(StreamConfigStorage.File, notify.Storage);
        Assert.Equal([$"{_prefix}.notify.>"], notify.Subjects!);
        Assert.Equal(StreamConfigStorage.File, (await js.GetStreamAsync($"OBJ_{_prefix}_objects")).Info.Config.Storage);

        await ReadyCacheAsync("b"); // a second node verifies the existing stores instead of failing
    }

    [Fact]
    public async Task Existing_memory_bucket_is_a_provisioning_failure_not_a_crash()
    {
        await new NatsKVContext(new NatsJSContext(_nc)).CreateStoreAsync(new NatsKVConfig($"{_prefix}_cache") { Storage = NatsKVStorageType.Memory });
        var cache = new NatsCache(new NatsCacheOptions { Prefix = _prefix, Replicas = 1 }, _nc);
        _disposables.Add(cache);
        cache.Start();
        for (var i = 0; i < 100 && cache.ProvisioningError is null; i++) await Task.Delay(100);

        Assert.IsType<CacheProvisioningException>(cache.ProvisioningError);
        Assert.Contains("must be File", cache.ProvisioningError!.Message);
        Assert.Equal("local", await cache.GetOrCreateAsync("k", Factory("local"))); // degraded, Open mode
    }

    [Fact]
    public async Task Two_nodes_share_values_through_l2_and_run_the_factory_once()
    {
        var a = await ReadyCacheAsync("a");
        var b = await ReadyCacheAsync("b");
        var calls = 0;

        Assert.Equal("v1", await a.GetOrCreateAsync("orders.1", Factory("v1", () => calls++)));
        Assert.Equal("v1", await b.GetOrCreateAsync("orders.1", Factory("other", () => calls++)));
        Assert.Equal(1, calls);

        // The entry carries the design's headers and is read back through Direct Get.
        var entry = await new NatsL2Store(new NatsJSContext(_nc), $"{_prefix}_cache").ReadAsync("orders.1._s1", leader: false, default);
        Assert.NotNull(entry);
        Assert.Equal(L2Op.Put, entry.Op);
        Assert.Equal("a", entry.Header(NatsCache.HeaderNode));
        Assert.Equal("1", entry.Header(NatsCache.HeaderSchema));
        // Nats-TTL = T_L2 (600 s +/- 10 %) + 22 s grace, whole seconds (design section 6).
        Assert.InRange(int.Parse(entry.Header("Nats-TTL")!.TrimEnd('s')), 562, 682);
        Assert.InRange(entry.Created, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddSeconds(5));
    }

    [Fact]
    public async Task Leader_and_direct_reads_agree_and_absent_keys_read_as_null()
    {
        await ReadyCacheAsync("a");
        var store = new NatsL2Store(new NatsJSContext(_nc), $"{_prefix}_cache");
        Assert.Null(await store.ReadAsync("missing._s1", leader: false, default));
        Assert.Null(await store.ReadAsync("missing._s1", leader: true, default));

        var w = await store.PutAsync("k._s1", [1, 2, 3], new Dictionary<string, string> { ["x-cache-ttl"] = "1000" }, TimeSpan.FromSeconds(60), expected: 0, default);
        Assert.Equal(WriteStatus.Committed, w.Status);
        var direct = await store.ReadAsync("k._s1", leader: false, default);
        var leader = await store.ReadAsync("k._s1", leader: true, default);
        Assert.Equal(w.Seq, direct!.Revision);
        Assert.Equal(w.Seq, leader!.Revision);
        Assert.Equal(new byte[] { 1, 2, 3 }, leader.Payload.ToArray());
        Assert.Equal("1000", leader.Header("x-cache-ttl"));
        Assert.Equal(direct.Created, leader.Created, TimeSpan.FromMilliseconds(1));

        var stale = await store.PutAsync("k._s1", [4], new Dictionary<string, string>(), TimeSpan.FromSeconds(60), expected: 0, default);
        Assert.True(stale.IsWrongLastSequence);
        Assert.Equal(w.Seq, stale.LastSequenceFromError());

        var del = await store.DeleteAsync("k._s1", expected: w.Seq, default);
        Assert.Equal(WriteStatus.Committed, del.Status);
        Assert.Equal(L2Op.Delete, (await store.ReadAsync("k._s1", leader: true, default))!.Op);
    }

    [Fact]
    public async Task Logically_expired_values_are_reloaded_with_a_fenced_write()
    {
        var o = CacheEntryOptions.Default with { L2Ttl = TimeSpan.FromSeconds(1), L1Ttl = TimeSpan.FromMilliseconds(200), JitterRatio = 0 };
        var a = await ReadyCacheAsync("a", c => c.DefaultEntryOptions = o);
        Assert.Equal("v1", await a.GetOrCreateAsync("orders.1", Factory("v1")));
        await Task.Delay(1500);
        var b = await ReadyCacheAsync("b", c => c.DefaultEntryOptions = o);
        Assert.Equal("v2", await b.GetOrCreateAsync("orders.1", Factory("v2")));
        Assert.Equal("v2", (await a.TryGetAsync<string>("orders.1")).Value);
    }

    [Fact]
    public async Task Remove_remove_by_tag_and_update_work_against_nats()
    {
        var a = await ReadyCacheAsync("a");
        var b = await ReadyCacheAsync("b");
        await a.SetAsync("orders.42.head", "h");
        await a.SetAsync("orders.42.lines", "l");
        await a.SetAsync("orders.43.head", "x");

        var r = await b.TryGetAsync<string>("orders.43.head");
        Assert.True(await b.UpdateAsync("orders.43.head", r.Revision, "y"));
        Assert.False(await a.UpdateAsync("orders.43.head", r.Revision, "z"));

        await b.RemoveByTagAsync("orders.42");
        Assert.False((await b.TryGetAsync<string>("orders.42.head")).Found);
        Assert.False((await b.TryGetAsync<string>("orders.42.lines")).Found);

        await b.RemoveAsync("orders.43.head");
        Assert.False((await b.TryGetAsync<string>("orders.43.head")).Found);
    }

    [Fact]
    public async Task Compressed_values_round_trip_through_nats()
    {
        var a = await ReadyCacheAsync("a");
        var b = await ReadyCacheAsync("b");
        var big = string.Concat(Enumerable.Repeat("order-line;", 2000));
        await a.SetAsync("orders.big", big);
        Assert.Equal(big, (await b.TryGetAsync<string>("orders.big")).Value);
    }

    [Fact]
    public async Task Dependency_injection_registers_the_cache_and_provisions_on_host_start()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<INatsConnection>(_nc);
        builder.Services.AddNatsDistributedCache(o => { o.Prefix = _prefix; o.Replicas = 1; });
        using var host = builder.Build();
        await host.StartAsync();

        var cache = host.Services.GetRequiredService<INatsCache>();
        var concrete = host.Services.GetRequiredService<NatsCache>();
        for (var i = 0; i < 100 && !concrete.IsReady; i++) await Task.Delay(100);
        Assert.True(concrete.IsReady);
        Assert.Equal(5, await cache.GetOrCreateAsync("n.1", Factory(5)));
        await host.StopAsync();
    }
}
