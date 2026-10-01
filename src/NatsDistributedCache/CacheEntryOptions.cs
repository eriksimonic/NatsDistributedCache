namespace NatsDistributedCache;

/// <summary>Per-call cache options (design section 4). Unset values fall back to the defaults below.</summary>
public sealed record CacheEntryOptions
{
    public static CacheEntryOptions Default { get; } = new();

    /// <summary>L2 lifetime before jitter.</summary>
    public TimeSpan L2Ttl { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>L1 lifetime before jitter; bounds staleness if invalidation events are lost.</summary>
    public TimeSpan L1Ttl { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Relative jitter j: TTLs are multiplied by a random factor in [1 - j, 1 + j].</summary>
    public double JitterRatio { get; init; } = 0.1;

    /// <summary>Maximum time a factory may run.</summary>
    public TimeSpan FactoryTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Maximum time to wait for another node's factory.</summary>
    public TimeSpan LockWaitTimeout { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>Lock lease; default min(FactoryTimeout + 2 s, MaxLeaseTtl). A value above MaxLeaseTtl throws.</summary>
    public TimeSpan? LeaseTtl { get; init; }

    /// <summary>Hard expiry (e.g. a token's exp). When set, jitter only shortens TTLs.</summary>
    public DateTimeOffset? AbsoluteExpiration { get; init; }

    /// <summary>Margin kept before <see cref="AbsoluteExpiration"/>.</summary>
    public TimeSpan SafetyMargin { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Per-call override of <see cref="NatsCacheOptions.FailureMode"/>.</summary>
    public FailureMode? FailureMode { get; init; }

    /// <summary>Early refresh in the last part of the value's life; 0 turns it off.</summary>
    public double EarlyRefreshRatio { get; init; } = 0.1;

    /// <summary>Negative caching: when set, a null factory result is cached for this long.</summary>
    public TimeSpan? CacheNullFor { get; init; }
}
