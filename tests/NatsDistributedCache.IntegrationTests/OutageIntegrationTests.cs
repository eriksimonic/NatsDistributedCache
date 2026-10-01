using System.Diagnostics;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NatsDistributedCache.Extensions;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.IntegrationTests;

/// <summary>
/// A NATS container of its own: these tests pause it to cut every node off (design section 8), which must not
/// disturb the tests sharing <see cref="NatsFixture"/>.
/// </summary>
public sealed class PausableNatsFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder(NatsFixture.Image)
        .WithCommand("-js", "-sd", "/data", "-m", "8222")
        .WithPortBinding(4222, assignRandomHostPort: true)
        .WithPortBinding(8222, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8222).ForPath("/healthz")))
        .Build();

    public string Url => $"nats://{_container.Hostname}:{_container.GetMappedPublicPort(4222)}";

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    public Task PauseAsync() => _container.PauseAsync();

    public Task UnpauseAsync() => _container.UnpauseAsync();

    public async Task<NatsConnection> ConnectAsync()
    {
        var nc = new NatsConnection(new NatsOpts { Url = Url, RequestTimeout = TimeSpan.FromSeconds(2) });
        await nc.ConnectAsync();
        return nc;
    }
}

/// <summary>Outage, Open-mode journal replay and the readiness check against a NATS that stops answering.</summary>
public sealed class OutageIntegrationTests(PausableNatsFixture nats) : IClassFixture<PausableNatsFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly List<IAsyncDisposable> _disposables = [];
    private string _prefix = null!;

    public ValueTask InitializeAsync()
    {
        _prefix = NatsFixture.NewPrefix();
        return default;
    }

    public async ValueTask DisposeAsync()
    {
        await nats.UnpauseAsync().ContinueWith(_ => { }); // never leave the container paused for the next test
        for (var i = _disposables.Count - 1; i >= 0; i--) await _disposables[i].DisposeAsync();
    }

    private async Task<NatsConnection> ConnectAsync()
    {
        var nc = await nats.ConnectAsync();
        _disposables.Add(nc);
        return nc;
    }

    private async Task<NatsCache> NodeAsync(string node)
    {
        var cache = new NatsCache(new NatsCacheOptions { Prefix = _prefix, NodeId = node, Replicas = 1 }, await ConnectAsync());
        _disposables.Add(cache);
        cache.Start();
        for (var i = 0; i < 100 && !cache.IsReady && cache.ProvisioningError is null; i++) await Task.Delay(100);
        Assert.True(cache.IsReady, cache.ProvisioningError?.Message);
        await cache.NotificationsLive.WaitAsync(TimeSpan.FromSeconds(5));
        return cache;
    }

    private static async Task Eventually(Func<bool> condition, string what)
    {
        var sw = Stopwatch.StartNew();
        while (!condition() && sw.Elapsed < Bound) await Task.Delay(100);
        Assert.True(condition(), $"{what} within {Bound.TotalSeconds} s");
    }

    [Fact]
    public async Task Outage_write_is_replayed_so_no_node_keeps_serving_the_pre_outage_value()
    {
        var a = await NodeAsync("a");
        var b = await NodeAsync("b");
        await b.SetAsync("orders.1", "pre-outage");
        Assert.Equal("pre-outage", (await a.TryGetAsync<string>("orders.1")).Value);
        await Task.Delay(1_500); // the outage write must be later than the pre-outage write plus the 1 s skew margin

        await nats.PauseAsync();
        await a.SetAsync("orders.1", "outage-write"); // Open mode: L1 only, journaled
        Assert.Equal(CacheHealth.Degraded, a.Health);
        Assert.Equal("outage-write", (await a.TryGetAsync<string>("orders.1")).Value);

        await nats.UnpauseAsync();
        await Eventually(() => a.Health == CacheHealth.Healthy, "a recovered");

        // The replay deleted the pre-outage entry at its revision. (If the paused server applied a's buffered write
        // after the unpause, that newer entry is kept instead.) Either way the pre-outage value is gone everywhere.
        var l2 = new NatsL2Store(new NatsJSContext(await ConnectAsync()), $"{_prefix}_cache");
        var head = await l2.ReadAsync("orders.1._s1", leader: true, default);
        Assert.True(head is null || head.Op == L2Op.Delete || head.Payload.Span.SequenceEqual("\"outage-write\""u8),
            $"L2 still holds {head?.Op} {(head is null ? "" : System.Text.Encoding.UTF8.GetString(head.Payload.Span))}");
        await Eventually(() => b.TryGetAsync<string>("orders.1").AsTask().GetAwaiter().GetResult().Value != "pre-outage",
            "b stopped serving the pre-outage value");
    }

    [Fact]
    public async Task Readiness_check_goes_degraded_during_an_outage_and_healthy_again()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<INatsConnection>(await ConnectAsync());
        builder.Services.AddNatsDistributedCache(o => { o.Prefix = _prefix; o.Replicas = 1; });
        builder.Services.AddHealthChecks().AddNatsCache();
        using var host = builder.Build();
        await host.StartAsync();
        var cache = host.Services.GetRequiredService<NatsCache>();
        var health = host.Services.GetRequiredService<HealthCheckService>();
        await Eventually(() => cache.IsReady, "provisioned");

        async Task<HealthStatus> Ready() => (await health.CheckHealthAsync(r => r.Tags.Contains("ready"))).Status;
        Assert.Equal(HealthStatus.Healthy, await Ready());

        await nats.PauseAsync();
        await cache.TryGetAsync<string>("orders.1"); // fails against the paused server: the node enters the outage
        Assert.Equal(HealthStatus.Degraded, await Ready()); // Open mode: keep routing to the node

        await nats.UnpauseAsync();
        await Eventually(() => Ready().GetAwaiter().GetResult() == HealthStatus.Healthy, "healthy again");
        await host.StopAsync();
    }
}
