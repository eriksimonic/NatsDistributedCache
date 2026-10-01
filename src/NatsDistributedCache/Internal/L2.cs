namespace NatsDistributedCache.Internal;

/// <summary>What the last message on a key's subject is.</summary>
internal enum L2Op
{
    Put,

    /// <summary>A DEL tombstone (explicit Remove or RemoveByTag, or a lock release).</summary>
    Delete,

    /// <summary>A PURGE tombstone.</summary>
    Purge,

    /// <summary>A server delete marker written when a per-key TTL expired.</summary>
    Marker,
}

/// <summary>The last message stored for a key.</summary>
/// <param name="Created">Server timestamp of the message (drives logical expiry, never the writer's clock).</param>
internal sealed record L2Entry(string Key, ulong Revision, DateTimeOffset Created, L2Op Op,
    ReadOnlyMemory<byte> Payload, IReadOnlyDictionary<string, string> Headers)
{
    public bool IsFree => Op != L2Op.Put;

    public string? Header(string name) => Headers.TryGetValue(name, out var v) ? v : null;
}

internal enum WriteStatus
{
    /// <summary>Stored at <see cref="WriteResult.Seq"/> (possibly by an earlier send of the same write).</summary>
    Committed,

    /// <summary>Refused by the server; <see cref="WriteResult.ErrCode"/> says why (10071 = wrong last sequence).</summary>
    Rejected,

    /// <summary>The outcome could not be learned; settle it with a leader read, never treat it as a conflict.</summary>
    Unknown,
}

internal readonly record struct WriteResult(WriteStatus Status, ulong Seq, int ErrCode, string? Error, string MsgId)
{
    public const int WrongLastSequence = 10071;

    public bool IsWrongLastSequence => Status == WriteStatus.Rejected && ErrCode == WrongLastSequence;

    /// <summary>The sequence named in a 10071 text ("wrong last sequence: {seq}").</summary>
    public ulong? LastSequenceFromError()
    {
        const string marker = "wrong last sequence: ";
        var i = Error?.IndexOf(marker, StringComparison.Ordinal) ?? -1;
        return i >= 0 && ulong.TryParse(Error!.Substring(i + marker.Length).Trim(), out var seq) ? seq : null;
    }
}

/// <summary>Thrown by the L2 store when NATS cannot be reached; the cache then applies its FailureMode.</summary>
internal sealed class L2UnavailableException : Exception
{
    public L2UnavailableException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }
}

/// <summary>The KV bucket as the cache sees it (design section 3). Implemented over NATS and faked in unit tests.</summary>
internal interface IL2Store
{
    /// <summary>
    /// The last message for <paramref name="key"/>, or null when the subject is empty.
    /// <paramref name="leader"/> = true reads through the stream leader (read-after-write coherent);
    /// false uses Direct Get (any replica, may lag).
    /// </summary>
    ValueTask<L2Entry?> ReadAsync(string key, bool leader, CancellationToken ct);

    /// <summary>Publishes a value with a server TTL; <paramref name="expected"/> fences on the subject's last sequence.</summary>
    ValueTask<WriteResult> PutAsync(string key, byte[] payload, IReadOnlyDictionary<string, string> headers,
        TimeSpan natsTtl, ulong? expected, CancellationToken ct);

    /// <summary>Publishes a DEL tombstone; the committed sequence is the tombstone revision.</summary>
    ValueTask<WriteResult> DeleteAsync(string key, ulong? expected, CancellationToken ct);

    /// <summary>Keys matching a KV filter such as <c>orders.42.&gt;</c>.</summary>
    IAsyncEnumerable<string> ListKeysAsync(string filter, CancellationToken ct);

    /// <summary>The bucket stream's last sequence (StreamInfo); also serves as the recovery probe.</summary>
    ValueTask<ulong> LastSequenceAsync(CancellationToken ct);
}
