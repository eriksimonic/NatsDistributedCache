using System.Diagnostics.Metrics;

namespace NatsDistributedCache.Internal;

/// <summary>Meter "NatsDistributedCache" (design section 2); the library has no OpenTelemetry dependency.</summary>
internal sealed class CacheMetrics : IDisposable
{
    public const string MeterName = "NatsDistributedCache";

    private readonly Meter _meter = new(MeterName);

    public CacheMetrics()
    {
        L1Hits = _meter.CreateCounter<long>("cache.l1.hits");
        L2Hits = _meter.CreateCounter<long>("cache.l2.hits");
        Misses = _meter.CreateCounter<long>("cache.misses");
        FactoryCalls = _meter.CreateCounter<long>("cache.factory.calls");
        FencedWrites = _meter.CreateCounter<long>("cache.fenced_writes");
        L2Full = _meter.CreateCounter<long>("cache.l2_full");
        Degraded = _meter.CreateCounter<long>("cache.degraded");
        LargeValuesSkipped = _meter.CreateCounter<long>("cache.large_values_skipped");
        LocksAcquired = _meter.CreateCounter<long>("cache.lock.acquired");
        LockWaits = _meter.CreateCounter<long>("cache.lock.waits");
        LockTakeovers = _meter.CreateCounter<long>("cache.lock.takeovers");
        LockWaitTimeouts = _meter.CreateCounter<long>("cache.lock.wait_timeouts");
        LeasesLost = _meter.CreateCounter<long>("cache.lock.leases_lost");
        EarlyRefreshes = _meter.CreateCounter<long>("cache.early_refreshes");
        LockRejected = _meter.CreateCounter<long>("cache.lock.rejected");
        EventsPublished = _meter.CreateCounter<long>("cache.events.published");
        EventPublishFailures = _meter.CreateCounter<long>("cache.events.publish_failures");
        EventsReceived = _meter.CreateCounter<long>("cache.events.received");
        L1Flushes = _meter.CreateCounter<long>("cache.l1.flushes");
        Outages = _meter.CreateCounter<long>("cache.outages");
        Recovered = _meter.CreateCounter<long>("cache.recovered");
    }

    public Counter<long> L1Hits { get; }
    public Counter<long> L2Hits { get; }
    public Counter<long> Misses { get; }
    public Counter<long> FactoryCalls { get; }
    public Counter<long> FencedWrites { get; }
    public Counter<long> L2Full { get; }
    public Counter<long> Degraded { get; }
    public Counter<long> LargeValuesSkipped { get; }
    public Counter<long> LocksAcquired { get; }
    public Counter<long> LockWaits { get; }
    public Counter<long> LockTakeovers { get; }
    public Counter<long> LockWaitTimeouts { get; }
    public Counter<long> LeasesLost { get; }
    public Counter<long> EarlyRefreshes { get; }
    public Counter<long> LockRejected { get; }
    public Counter<long> EventsPublished { get; }
    public Counter<long> EventPublishFailures { get; }
    public Counter<long> EventsReceived { get; }
    public Counter<long> L1Flushes { get; }
    public Counter<long> Outages { get; }
    public Counter<long> Recovered { get; }

    public void Dispose() => _meter.Dispose();
}
