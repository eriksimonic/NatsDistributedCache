namespace NatsDistributedCache.Internal;

/// <summary>
/// Key rules (design section 3): user keys are never hashed or rewritten. Allowed characters are the NATS
/// KV set A-Z a-z 0-9 - _ = /, segments are separated by dots, no segment may be empty or start with the
/// reserved "_s", and the key is at most 256 characters. The internal key appends the schema version.
/// </summary>
internal static class CacheKeys
{
    public const int MaxKeyLength = 256;
    private const string SchemaSegmentPrefix = "_s";

    public static void Validate(string key, string paramName = "key")
    {
        if (key is null) throw new ArgumentNullException(paramName);
        if (key.Length == 0) throw new ArgumentException("Key must not be empty.", paramName);
        if (key.Length > MaxKeyLength) throw new ArgumentException($"Key is longer than {MaxKeyLength} characters.", paramName);

        var segmentStart = 0;
        for (var i = 0; i <= key.Length; i++)
        {
            if (i < key.Length && key[i] != '.')
            {
                if (!IsAllowed(key[i]))
                    throw new ArgumentException($"Key contains '{key[i]}' at position {i}; allowed are A-Z a-z 0-9 - _ = / and '.' as separator.", paramName);
                continue;
            }

            if (i == segmentStart) throw new ArgumentException("Key has an empty dot-segment.", paramName);
            if (string.CompareOrdinal(key, segmentStart, SchemaSegmentPrefix, 0, SchemaSegmentPrefix.Length) == 0)
                throw new ArgumentException("Key segments starting with '_s' are reserved for schema versions.", paramName);
            segmentStart = i + 1;
        }
    }

    /// <summary>Validates a tag prefix; same rules as a key.</summary>
    public static void ValidatePrefix(string prefix) => Validate(prefix, nameof(prefix));

    public static string Internal(string key, int schemaVersion) => $"{key}.{SchemaSegmentPrefix}{schemaVersion}";

    /// <summary>The user key an internal key was built from.</summary>
    public static string UserKeyOf(string internalKey)
    {
        var dot = internalKey.LastIndexOf('.');
        return dot > 0 ? internalKey.Substring(0, dot) : internalKey;
    }

    /// <summary>Validates the store prefix: ^[A-Za-z0-9-]{1,32}$.</summary>
    public static void ValidateStorePrefix(string prefix)
    {
        if (string.IsNullOrEmpty(prefix) || prefix.Length > 32 || !prefix.All(c => c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-'))
            throw new ArgumentException("Prefix must match ^[A-Za-z0-9-]{1,32}$.", nameof(NatsCacheOptions.Prefix));
    }

    private static bool IsAllowed(char c) =>
        c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_' or '=' or '/';
}
