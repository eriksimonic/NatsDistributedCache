namespace NatsDistributedCache.Internal;

/// <summary>Source of uniform random numbers in [0, 1); replaceable in tests.</summary>
internal interface IRandomSource
{
    double NextDouble();
}

internal sealed class SharedRandomSource : IRandomSource
{
    public static readonly SharedRandomSource Instance = new();

#if NET6_0_OR_GREATER
    public double NextDouble() => Random.Shared.NextDouble();
#else
    [ThreadStatic] private static Random? _random;

    public double NextDouble() => (_random ??= new Random(Guid.NewGuid().GetHashCode())).NextDouble();
#endif
}

/// <summary>Lifetimes of one write (design section 6).</summary>
/// <param name="L2Ttl">Jittered logical L2 lifetime (T_L2), stored in x-cache-ttl.</param>
/// <param name="NatsTtl">Server-side Nats-TTL: T_L2 + grace, whole seconds.</param>
/// <param name="LeaseTtl">Effective lock lease.</param>
internal readonly record struct WritePlan(TimeSpan L2Ttl, TimeSpan NatsTtl, TimeSpan LeaseTtl);

internal sealed class Expiration
{
    private readonly NatsCacheOptions _options;
    private readonly IRandomSource _random;

    public Expiration(NatsCacheOptions options, IRandomSource random)
    {
        _options = options;
        _random = random;
    }

    /// <summary>
    /// Effective lease: FactoryTimeout + 2 s clamped to [MinLeaseTtl, MaxLeaseTtl] by default; an explicit value
    /// outside that range throws.
    /// </summary>
    public TimeSpan LeaseTtl(CacheEntryOptions o)
    {
        if (o.LeaseTtl is { } explicitLease)
        {
            if (explicitLease <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(o.LeaseTtl), "LeaseTtl must be positive.");
            if (explicitLease > _options.MaxLeaseTtl)
                throw new ArgumentOutOfRangeException(nameof(o.LeaseTtl), $"LeaseTtl {explicitLease} exceeds MaxLeaseTtl {_options.MaxLeaseTtl}.");
            if (explicitLease < _options.MinLeaseTtl)
                throw new ArgumentOutOfRangeException(nameof(o.LeaseTtl), $"LeaseTtl {explicitLease} is below MinLeaseTtl {_options.MinLeaseTtl}.");
            return explicitLease;
        }

        var lease = o.FactoryTimeout + TimeSpan.FromSeconds(2);
        if (lease > _options.MaxLeaseTtl) lease = _options.MaxLeaseTtl;
        return lease < _options.MinLeaseTtl ? _options.MinLeaseTtl : lease;
    }

    /// <summary>
    /// T_L2 = L2Ttl * (1 + U(-j, j)); with an absolute expiration the factor is in [1 - j, 1] and T_L2 is capped at
    /// AbsoluteExpiration - SafetyMargin - now. Nats-TTL = T_L2 + grace, grace = LeaseTtl + FactoryTimeout.
    /// Returns null when the value must not be cached at all (absolute expiry too close).
    /// </summary>
    public WritePlan? PlanWrite(CacheEntryOptions o, DateTimeOffset now, TimeSpan? ttlOverride = null)
    {
        ValidateJitter(o.JitterRatio);
        var baseTtl = ttlOverride ?? o.L2Ttl;
        if (baseTtl <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(o.L2Ttl), "L2Ttl must be positive.");

        var l2 = Jitter(baseTtl, o.JitterRatio, negativeOnly: o.AbsoluteExpiration is not null);
        if (o.AbsoluteExpiration is { } abs)
        {
            var cap = abs - o.SafetyMargin - now;
            if (cap <= TimeSpan.Zero) return null;
            if (l2 > cap) l2 = cap;
        }

        var lease = LeaseTtl(o);
        var grace = lease + o.FactoryTimeout;
        var natsTtl = TimeSpan.FromSeconds(Math.Ceiling((l2 + grace).TotalSeconds));
        if (natsTtl > _options.CacheMaxAge)
            throw new ArgumentOutOfRangeException(nameof(o.L2Ttl), $"L2Ttl + grace ({natsTtl}) exceeds the cache bucket MaxAge {_options.CacheMaxAge}.");
        return new WritePlan(l2, natsTtl, lease);
    }

    /// <summary>T_L1 = min(L1Ttl * (1 + U(-j, j)), expires - now); negative-only jitter with an absolute expiration.</summary>
    public TimeSpan PlanL1(CacheEntryOptions o, DateTimeOffset now, DateTimeOffset logicalExpiry)
    {
        ValidateJitter(o.JitterRatio);
        var l1 = Jitter(o.L1Ttl, o.JitterRatio, negativeOnly: o.AbsoluteExpiration is not null);
        var remaining = logicalExpiry - now;
        return remaining < l1 ? remaining : l1;
    }

    private TimeSpan Jitter(TimeSpan ttl, double j, bool negativeOnly)
    {
        var u = _random.NextDouble(); // [0, 1)
        var factor = negativeOnly ? 1 - j * u : 1 + j * (2 * u - 1);
        return TimeSpan.FromTicks((long)(ttl.Ticks * factor));
    }

    private static void ValidateJitter(double j)
    {
        if (j is < 0 or >= 1 || double.IsNaN(j)) throw new ArgumentOutOfRangeException(nameof(CacheEntryOptions.JitterRatio), "JitterRatio must be in [0, 1).");
    }
}
