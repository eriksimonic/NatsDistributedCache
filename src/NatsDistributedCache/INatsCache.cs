namespace NatsDistributedCache;

/// <summary>
/// Two-level cache: an in-process L1 in front of a NATS JetStream KV L2 shared by every node.
/// </summary>
public interface INatsCache
{
    /// <summary>
    /// Returns the cached value for <paramref name="key"/>, or runs <paramref name="factory"/> once and caches its result.
    /// </summary>
    ValueTask<T> GetOrCreateAsync<T>(string key,
        Func<FactoryContext, CancellationToken, ValueTask<T>> factory,
        CacheEntryOptions? options = null, CancellationToken ct = default);

    /// <summary>Returns the cached value, if any, without running a factory.</summary>
    ValueTask<CacheResult<T>> TryGetAsync<T>(string key, CancellationToken ct = default);

    /// <summary>Writes a value unconditionally.</summary>
    ValueTask SetAsync<T>(string key, T value, CacheEntryOptions? options = null, CancellationToken ct = default);

    /// <summary>
    /// Writes a value only if the key's current revision is <paramref name="expectedRevision"/> (0 = absent).
    /// Returns false when another write happened first.
    /// </summary>
    ValueTask<bool> UpdateAsync<T>(string key, ulong expectedRevision, T value,
        CacheEntryOptions? options = null, CancellationToken ct = default);

    /// <summary>Deletes the key for every schema version in <see cref="NatsCacheOptions.KnownSchemaVersions"/>.</summary>
    ValueTask RemoveAsync(string key, CancellationToken ct = default);

    /// <summary>Deletes every key under the dot-separated <paramref name="prefix"/>, e.g. <c>orders.42</c>.</summary>
    ValueTask RemoveByTagAsync(string prefix, CancellationToken ct = default);

    /// <summary>
    /// Flushes L1 on every node (admin operation, e.g. after a restore); L2 is untouched. Publishes a <c>clear</c>
    /// event; this node's L1 is flushed even when NATS is unavailable.
    /// </summary>
    ValueTask ClearAsync(CancellationToken ct = default);
}
