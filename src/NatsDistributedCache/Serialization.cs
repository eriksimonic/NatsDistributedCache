using System.Text.Json;

namespace NatsDistributedCache;

/// <summary>Pluggable value serializer (design Q12).</summary>
public interface ICacheSerializer
{
    byte[] Serialize<T>(T value);

    T? Deserialize<T>(ReadOnlyMemory<byte> data);
}

/// <summary>Default serializer: System.Text.Json; set a TypeInfoResolver on the options for source generation.</summary>
public sealed class SystemTextJsonCacheSerializer : ICacheSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonCacheSerializer(JsonSerializerOptions options) => _options = options;

    public byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T? Deserialize<T>(ReadOnlyMemory<byte> data) => JsonSerializer.Deserialize<T>(data.Span, _options);
}
