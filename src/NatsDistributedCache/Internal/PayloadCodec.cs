using System.IO.Compression;

namespace NatsDistributedCache.Internal;

/// <summary>Brotli above the configured threshold, flagged with the x-cache-enc header.</summary>
internal static class PayloadCodec
{
    public const string Brotli = "br";

    public static (byte[] Payload, string? Encoding) Encode(byte[] raw, int thresholdBytes)
    {
        if (thresholdBytes <= 0 || raw.Length <= thresholdBytes) return (raw, null);
        using var output = new MemoryStream(raw.Length / 2);
        using (var brotli = new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true))
            brotli.Write(raw, 0, raw.Length);
        var compressed = output.ToArray();
        return compressed.Length < raw.Length ? (compressed, Brotli) : (raw, null);
    }

    public static byte[] Decode(ReadOnlyMemory<byte> payload, string? encoding)
    {
        if (encoding is null) return payload.ToArray();
        if (encoding != Brotli) throw new InvalidDataException($"Unknown payload encoding '{encoding}'.");
        using var input = new MemoryStream(payload.ToArray());
        using var brotli = new BrotliStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        brotli.CopyTo(output);
        return output.ToArray();
    }
}
