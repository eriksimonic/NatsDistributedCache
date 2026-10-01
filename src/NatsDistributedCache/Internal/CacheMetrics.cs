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
    }

    public Counter<long> L1Hits { get; }
    public Counter<long> L2Hits { get; }
    public Counter<long> Misses { get; }
    public Counter<long> FactoryCalls { get; }
    public Counter<long> FencedWrites { get; }
    public Counter<long> L2Full { get; }
    public Counter<long> Degraded { get; }
    public Counter<long> LargeValuesSkipped { get; }

    public void Dispose() => _meter.Dispose();
}
