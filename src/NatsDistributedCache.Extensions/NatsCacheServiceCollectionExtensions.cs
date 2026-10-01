using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;

namespace NatsDistributedCache.Extensions;

public static class NatsCacheServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="INatsCache"/> as a singleton. Uses an <see cref="INatsConnection"/> from the container
    /// when one is registered, otherwise connects to <see cref="NatsCacheOptions.Url"/>. Store provisioning starts
    /// in the background when the host starts and never blocks startup (design section 8).
    /// </summary>
    public static IServiceCollection AddNatsDistributedCache(this IServiceCollection services, Action<NatsCacheOptions> configure)
    {
        if (configure is null) throw new ArgumentNullException(nameof(configure));
        services.AddOptions<NatsCacheOptions>().Configure(configure);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<NatsCache>(sp => new NatsCache(
            sp.GetRequiredService<IOptions<NatsCacheOptions>>().Value,
            sp.GetService<INatsConnection>(),
            sp.GetService<ILoggerFactory>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetService<ICacheSerializer>()));
        services.TryAddSingleton<INatsCache>(sp => sp.GetRequiredService<NatsCache>());
        services.AddHostedService<NatsCacheStartup>();
        return services;
    }

    private sealed class NatsCacheStartup : IHostedService
    {
        private readonly NatsCache _cache;

        public NatsCacheStartup(NatsCache cache) => _cache = cache;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cache.Start();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
