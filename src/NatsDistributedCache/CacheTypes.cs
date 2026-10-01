namespace NatsDistributedCache;

/// <summary>What the cache does when NATS or the distributed lock is unavailable (design Q1).</summary>
public enum FailureMode
{
    /// <summary>Availability first: run the factory locally and cache in L1 only.</summary>
    Open,

    /// <summary>NATS is required: throw <see cref="CacheUnavailableException"/>.</summary>
    Closed,
}

/// <summary>Why a factory is running.</summary>
public enum FactoryReason
{
    /// <summary>The key was absent or logically expired.</summary>
    Miss,

    /// <summary>Background refresh in the last part of the value's life.</summary>
    EarlyRefresh,

    /// <summary>A previous lock owner failed or its lease expired.</summary>
    Takeover,

    /// <summary>NATS is unavailable and <see cref="FailureMode.Open"/> applies.</summary>
    Degraded,
}

/// <summary>Context passed to every factory call.</summary>
/// <param name="Key">The user key.</param>
/// <param name="Reason">Why the factory runs.</param>
/// <param name="LockToken">The distributed lock token, when a lock is held.</param>
/// <param name="Attempt">1-based attempt number.</param>
public sealed record FactoryContext(string Key, FactoryReason Reason, string? LockToken, int Attempt);

/// <summary>Result of <see cref="INatsCache.TryGetAsync{T}"/>.</summary>
/// <param name="Found">Whether a live, unexpired value exists.</param>
/// <param name="Value">The value, when found.</param>
/// <param name="Revision">The L2 revision of the value, usable with <see cref="INatsCache.UpdateAsync{T}"/>.</param>
public readonly record struct CacheResult<T>(bool Found, T? Value, ulong Revision);

/// <summary>Thrown in <see cref="FailureMode.Closed"/> when NATS is unavailable.</summary>
public sealed class CacheUnavailableException : Exception
{
    public CacheUnavailableException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }
}

/// <summary>Thrown when the NATS stores exist with a configuration the library cannot use.</summary>
public sealed class CacheProvisioningException : Exception
{
    public CacheProvisioningException(string message)
        : base(message)
    {
    }
}
