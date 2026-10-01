using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using NATS.Client.Core;

[assembly: AssemblyFixture(typeof(NatsDistributedCache.IntegrationTests.NatsFixture))]

namespace NatsDistributedCache.IntegrationTests;

/// <summary>
/// One NATS container (the pinned latest stable release, design section 3) with JetStream on file storage,
/// shared by all tests. Single node, so tests use Replicas = 1 and a unique prefix each.
/// </summary>
public sealed class NatsFixture : IAsyncLifetime
{
    public const string Image = "nats:2.15.0-alpine";

    private readonly IContainer _container = new ContainerBuilder(Image)
        .WithCommand("-js", "-sd", "/data", "-m", "8222")
        .WithPortBinding(4222, assignRandomHostPort: true)
        .WithPortBinding(8222, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8222).ForPath("/healthz")))
        .Build();

    public string Url => $"nats://{_container.Hostname}:{_container.GetMappedPublicPort(4222)}";

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    public async Task<NatsConnection> ConnectAsync()
    {
        var nc = new NatsConnection(new NatsOpts { Url = Url, RequestTimeout = TimeSpan.FromSeconds(2) });
        await nc.ConnectAsync();
        return nc;
    }

    public static string NewPrefix() => $"it{Guid.NewGuid():N}".Substring(0, 20);
}
