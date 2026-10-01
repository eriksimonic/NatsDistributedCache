using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace NatsDistributedCache.Extensions;

/// <summary>
/// Readiness check (design section 8): Healthy while NATS is reachable and the stores are provisioned; Degraded in
/// FailureMode.Open so the load balancer keeps routing to the node; Unhealthy in FailureMode.Closed. Register it
/// on the readiness endpoint only: liveness must ignore NATS.
/// </summary>
public sealed class NatsCacheHealthCheck : IHealthCheck
{
    private readonly NatsCache _cache;

    public NatsCacheHealthCheck(NatsCache cache) => _cache = cache;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var description = _cache.HealthDescription;
        var result = _cache.Health switch
        {
            CacheHealth.Healthy => HealthCheckResult.Healthy("NATS reachable, stores provisioned"),
            CacheHealth.Degraded => HealthCheckResult.Degraded(description),
            _ => HealthCheckResult.Unhealthy(description),
        };
        return Task.FromResult(result);
    }
}

public static class NatsCacheHealthChecksBuilderExtensions
{
    /// <summary>Adds the NatsCache readiness check, tagged "ready".</summary>
    public static IHealthChecksBuilder AddNatsCache(this IHealthChecksBuilder builder, string name = "nats-cache", params string[] tags) =>
        builder.AddCheck<NatsCacheHealthCheck>(name, failureStatus: null, tags: tags.Length > 0 ? tags : ["ready"]);
}
