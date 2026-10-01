using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

namespace NatsDistributedCache.Internal;

/// <summary>
/// L2 over NATS JetStream KV (design section 3). Writes go through the publish helper, never through
/// NATS.Net PutAsync/UpdateAsync/CreateAsync, because only a raw publish can carry Nats-TTL together with an
/// expected-sequence fence. Outcomes are Committed / Rejected / Unknown; an unknown outcome is resent with the
/// same Nats-Msg-Id so the server deduplicates it.
/// </summary>
internal sealed class NatsL2Store : IL2Store
{
    private const int MaxTransientRetries = 5;
    private const int MaxUnknownResends = 5;

    private readonly INatsJSContext _js;
    private readonly string _bucket;
    private readonly string _streamName;
    private INatsJSStream? _stream;
    private INatsKVStore? _kv;

    public NatsL2Store(INatsJSContext js, string bucket)
    {
        _js = js;
        _bucket = bucket;
        _streamName = $"KV_{bucket}";
    }

    public async ValueTask<L2Entry?> ReadAsync(string key, bool leader, CancellationToken ct)
    {
        var request = new StreamMsgGetRequest { LastBySubj = Subject(key) };
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var stream = await StreamAsync(ct).ConfigureAwait(false);
                return leader
                    ? await LeaderReadAsync(stream, key, request, ct).ConfigureAwait(false)
                    : await DirectReadAsync(stream, key, request, ct).ConfigureAwait(false);
            }
            catch (NatsJSApiException ex) when (ex.Error.ErrCode == 10037)
            {
                return null; // no message on the subject
            }
            catch (Exception ex) when (IsTransport(ex) && attempt < 2)
            {
                // Leader reads fail with no-response during an election (spike leader-failover test).
                await Task.Delay(250 * (attempt + 1), ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransport(ex))
            {
                throw new L2UnavailableException($"L2 read of '{key}' failed.", ex);
            }
        }
    }

    public ValueTask<WriteResult> PutAsync(string key, byte[] payload, IReadOnlyDictionary<string, string> headers,
        TimeSpan natsTtl, ulong? expected, CancellationToken ct)
    {
        var h = new NatsHeaders();
        foreach (var kv in headers) h[kv.Key] = kv.Value;
        h["Nats-TTL"] = $"{(long)Math.Ceiling(natsTtl.TotalSeconds)}s";
        return PublishAsync(key, payload, h, expected, ct);
    }

    public ValueTask<WriteResult> DeleteAsync(string key, ulong? expected, CancellationToken ct)
    {
        var h = new NatsHeaders { ["KV-Operation"] = "DEL" };
        return PublishAsync(key, [], h, expected, ct);
    }

    public async IAsyncEnumerable<string> ListKeysAsync(string filter, [EnumeratorCancellation] CancellationToken ct)
    {
        INatsKVStore kv;
        try
        {
            kv = _kv ??= await new NatsKVContext(_js).GetStoreAsync(_bucket, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsTransport(ex))
        {
            throw new L2UnavailableException("L2 key listing failed.", ex);
        }

        await foreach (var k in kv.GetKeysAsync([filter], cancellationToken: ct).ConfigureAwait(false))
            yield return k;
    }

    private async ValueTask<WriteResult> PublishAsync(string key, byte[] data, NatsHeaders h, ulong? expected, CancellationToken ct)
    {
        // One msg id per logical write; reused only for resends of this same write (design section 3).
        var msgId = Guid.NewGuid().ToString("N");
        h["Nats-Msg-Id"] = msgId;
        if (expected is { } e) h["Nats-Expected-Last-Subject-Sequence"] = e.ToString(CultureInfo.InvariantCulture);

        int transient = 0, unknown = 0;
        while (true)
        {
            try
            {
                var ack = await _js.PublishAsync(Subject(key), data, headers: h, cancellationToken: ct).ConfigureAwait(false);
                if (ack.Duplicate) return new(WriteStatus.Committed, ack.Seq, 0, null, msgId);
                ack.EnsureSuccess();
                return new(WriteStatus.Committed, ack.Seq, 0, null, msgId);
            }
            catch (NatsJSDuplicateMessageException ex)
            {
                return new(WriteStatus.Committed, ex.Sequence, 0, null, msgId);
            }
            catch (NatsJSApiException ex) when (ex.Error.ErrCode is 10164 or 10158)
            {
                // 10164: another write to the subject is in flight, nothing stored. 10158: our duplicate is still
                // being applied. Both are transient; resend the identical publish.
                if (++transient > MaxTransientRetries)
                    return new(WriteStatus.Unknown, 0, ex.Error.ErrCode, ex.Error.Description, msgId);
                await Task.Delay(2 << transient, ct).ConfigureAwait(false); // 4..64 ms
            }
            catch (NatsJSApiException ex)
            {
                return new(WriteStatus.Rejected, 0, ex.Error.ErrCode, ex.Error.Description, msgId);
            }
            catch (Exception ex) when (IsTransport(ex))
            {
                // Lost reply, timeout or reconnect: the write may have committed. Resend with the same msg id.
                if (++unknown > MaxUnknownResends) return new(WriteStatus.Unknown, 0, 0, ex.GetType().Name, msgId);
                await Task.Delay(250 * unknown, ct).ConfigureAwait(false);
            }
        }
    }

    private static async ValueTask<L2Entry?> LeaderReadAsync(INatsJSStream stream, string key, StreamMsgGetRequest request, CancellationToken ct)
    {
        var resp = await stream.GetAsync(request, ct).ConfigureAwait(false);
        var m = resp.Message;
        var headers = ParseHeaderBlock(m.Hdrs);
        return new L2Entry(key, m.Seq, m.Time, OpOf(headers), m.Data, headers);
    }

    private static async ValueTask<L2Entry?> DirectReadAsync(INatsJSStream stream, string key, StreamMsgGetRequest request, CancellationToken ct)
    {
        var msg = await stream.GetDirectAsync<byte[]>(request, cancellationToken: ct).ConfigureAwait(false);
        if (msg.HasNoResponders) throw new NatsNoRespondersException();
        if (msg.Headers is { Code: 404 }) return null;
        if (msg.Headers is null) throw new InvalidOperationException("Direct Get reply without headers.");

        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in msg.Headers) headers[kv.Key] = kv.Value.ToString();
        var seq = ulong.Parse(headers["Nats-Sequence"], CultureInfo.InvariantCulture);
        var created = ParseTimestamp(headers["Nats-Time-Stamp"]);
        return new L2Entry(key, seq, created, OpOf(headers), msg.Data ?? [], headers);
    }

    internal static L2Op OpOf(IReadOnlyDictionary<string, string> headers)
    {
        if (headers.ContainsKey("Nats-Marker-Reason")) return L2Op.Marker;
        if (headers.TryGetValue("KV-Operation", out var op))
        {
            if (string.Equals(op, "DEL", StringComparison.OrdinalIgnoreCase)) return L2Op.Delete;
            if (string.Equals(op, "PURGE", StringComparison.OrdinalIgnoreCase)) return L2Op.Purge;
        }

        return L2Op.Put;
    }

    /// <summary>Parses a NATS header block ("NATS/1.0[ status]\r\nName: value\r\n..."), base64 as in the API response.</summary>
    internal static Dictionary<string, string> ParseHeaderBlock(string? hdrs)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrEmpty(hdrs)) return result;
        string text;
        try
        {
            text = Encoding.UTF8.GetString(Convert.FromBase64String(hdrs));
        }
        catch (FormatException)
        {
            text = hdrs!;
        }

        foreach (var line in text.Split(["\r\n"], StringSplitOptions.None).Skip(1))
        {
            var idx = line.IndexOf(':');
            if (idx <= 0) continue;
            var name = line.Substring(0, idx).Trim();
            if (!result.ContainsKey(name)) result[name] = line.Substring(idx + 1).Trim(); // first value wins
        }

        return result;
    }

    /// <summary>RFC 3339 with up to nanosecond precision (Nats-Time-Stamp); .NET parses at most 7 fraction digits.</summary>
    internal static DateTimeOffset ParseTimestamp(string value)
    {
        var dot = value.IndexOf('.');
        if (dot > 0)
        {
            var end = dot + 1;
            while (end < value.Length && char.IsDigit(value[end])) end++;
            if (end - dot - 1 > 7) value = value.Substring(0, dot + 8) + value.Substring(end);
        }

        return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
    }

    internal static bool IsTransport(Exception ex) =>
        ex is NatsJSPublishNoResponseException or NatsNoReplyException or NatsNoRespondersException or NatsTimeoutException
            or NatsJSTimeoutException or NatsJSApiNoResponseException or NatsConnectionFailedException or TimeoutException
        || (ex is NatsException && ex is not NatsJSApiException);

    private string Subject(string key) => $"$KV.{_bucket}.{key}";

    private async ValueTask<INatsJSStream> StreamAsync(CancellationToken ct) =>
        _stream ??= await _js.GetStreamAsync(_streamName, cancellationToken: ct).ConfigureAwait(false);
}
