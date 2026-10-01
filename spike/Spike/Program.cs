// Milestone-1 spike: verifies the NATS behaviours DESIGN.md v1.2 depends on.
// Each test prints a PASS/FAIL line and writes results/<label>-<name>.json.
// Usage: dotnet run -- <all|ttl-markers|cas-marker|del-helper|read-latency|direct-staleness|create-contention|
//                       helper-contention|ttl-overwrite|lock-throughput-kv|lock-throughput-helper|
//                       consumer-below-firstseq|leader-failover|restart-setup|restart-check>

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

var urls = new[] { "nats://localhost:4222", "nats://localhost:4223", "nats://localhost:4224" };
var resultsDir = Environment.GetEnvironmentVariable("SPIKE_RESULTS") ?? "results";
var label = Environment.GetEnvironmentVariable("SPIKE_LABEL") ?? "default";
var project = Environment.GetEnvironmentVariable("SPIKE_PROJECT") ?? "spike";
Directory.CreateDirectory(resultsDir);

const string CacheBucket = "spike_cache";
const string LocksBucket = "spike_locks";
const string NotifyStream = "spike_notify";

var cmd = args.Length > 0 ? args[0] : "all";
var results = new List<TestResult>();

await using var nc0 = await Connect(0);
var js0 = new NatsJSContext(nc0);
var kv0 = new NatsKVContext(js0);
var (cache, locks) = await Setup(kv0);
var cacheStream0 = await js0.GetStreamAsync($"KV_{CacheBucket}");
var locksStream0 = await js0.GetStreamAsync($"KV_{LocksBucket}");

var tests = new Dictionary<string, Func<Task<TestResult>>>
{
    ["ttl-markers"] = () => TtlMarkers(cache, cacheStream0),
    ["cas-marker"] = () => CasMarker(cache, js0, cacheStream0),
    ["del-helper"] = () => DelHelper(cache, js0),
    ["read-latency"] = () => ReadLatency(cache, cacheStream0),
    ["direct-staleness"] = DirectStaleness,
    ["create-contention"] = CreateContention,
    ["helper-contention"] = HelperContention,
    ["ttl-overwrite"] = () => TtlOverwrite(cache, locks, js0, cacheStream0, locksStream0),
    ["lock-throughput-kv"] = () => LockThroughput("kv"),
    ["lock-throughput-helper"] = () => LockThroughput("helper"),
    ["consumer-below-firstseq"] = () => ConsumerBelowFirstSeq(js0),
    ["leader-failover"] = LeaderFailover,
    ["restart-setup"] = () => RestartSetup(cache, locks),
    ["restart-check"] = RestartCheck,
};

// "all" skips the tests the script runs separately (throughput, failover, restart).
string[] separate = ["lock-throughput", "leader-failover", "restart"];
var toRun = cmd == "all" ? tests.Keys.Where(k => !separate.Any(k.StartsWith)).ToList() : [cmd];

foreach (var name in toRun)
{
    Stats.Reset();
    TestResult r;
    try { r = await tests[name](); }
    catch (Exception ex) { r = new TestResult(name, false, new() { ["exception"] = ex.ToString() }); }
    r.Details["publishStats"] = Stats.Snapshot();
    results.Add(r);
    Console.WriteLine($"{(r.Pass ? "PASS" : "FAIL")}  {r.Name,-26} {JsonSerializer.Serialize(r.Details)}");
    await File.WriteAllTextAsync(Path.Combine(resultsDir, $"{label}-{name}.json"),
        JsonSerializer.Serialize(r, new JsonSerializerOptions { WriteIndented = true }));
}

return results.All(r => r.Pass) ? 0 : 1;

// ---------------------------------------------------------------- setup and helpers

async Task<NatsConnection> Connect(int i, bool allServers = false)
{
    var nc = new NatsConnection(new NatsOpts
    {
        Url = allServers ? string.Join(",", urls) : urls[i % urls.Length],
        Name = $"spike-{i}",
        MaxReconnectRetry = -1,
        RequestTimeout = TimeSpan.FromSeconds(2), // design section 8; the unknown-outcome path depends on it
    });
    await nc.ConnectAsync();
    return nc;
}

async Task<(INatsKVStore cache, INatsKVStore locks)> Setup(NatsKVContext kv)
{
    // History 1 is a hard invariant (design section 3); short marker TTL keeps the tests fast.
    NatsKVConfig Config(string bucket) => new(bucket)
    {
        History = 1,
        Storage = NatsKVStorageType.File,
        NumberOfReplicas = 3,
        LimitMarkerTTL = TimeSpan.FromSeconds(2),
    };

    for (var attempt = 0; ; attempt++)
    {
        try
        {
            return (await kv.CreateOrUpdateStoreAsync(Config(CacheBucket)), await kv.CreateOrUpdateStoreAsync(Config(LocksBucket)));
        }
        catch (Exception) when (attempt < 30)
        {
            await Task.Delay(2000); // JetStream meta leader not elected yet
        }
    }
}

// The publish helper (design section 3): raw publish to $KV.{bucket}.{key}. One msg id per logical
// write, reused only when resending that same write, so every write is idempotent:
// - 10164 / 10158: transient (not stored / duplicate still being applied) -> resend the identical publish
// - timeout / no reply: unknown outcome -> resend the identical publish; the server dedups by msg id
// - duplicate ack: the first send committed -> success at its sequence
// - 10071 and other API errors: rejected, never retried here
static async Task<PubResult> Publish(INatsJSContext js, string bucket, string key, byte[] data,
    TimeSpan? ttl = null, ulong? expected = null, bool del = false, string? msgId = null)
{
    msgId ??= Guid.NewGuid().ToString("N");
    var h = new NatsHeaders { ["Nats-Msg-Id"] = msgId };
    if (ttl is { } t) h["Nats-TTL"] = $"{(int)Math.Ceiling(t.TotalSeconds)}s";
    if (expected is { } e) h["Nats-Expected-Last-Subject-Sequence"] = e.ToString(CultureInfo.InvariantCulture);
    if (del) h["KV-Operation"] = "DEL";

    int transient = 0, unknown = 0;
    while (true)
    {
        Stats.Inc("sends");
        try
        {
            var ack = await js.PublishAsync($"$KV.{bucket}.{key}", data, headers: h);
            if (ack.Duplicate) { Stats.Inc("duplicateAcks"); return new(PubStatus.Committed, ack.Seq, 0, null, msgId); }
            ack.EnsureSuccess();
            return new(PubStatus.Committed, ack.Seq, 0, null, msgId);
        }
        catch (NatsJSDuplicateMessageException ex)
        {
            Stats.Inc("duplicateAcks");
            return new(PubStatus.Committed, ex.Sequence, 0, null, msgId);
        }
        catch (NatsJSApiException ex) when (ex.Error.ErrCode is 10164 or 10158)
        {
            Stats.Inc($"transient{ex.Error.ErrCode}");
            if (++transient > 5)
            {
                // Nothing was stored for this send, but the subject is busy: the caller resolves it
                // with a leader read, never as a conflict.
                return new(PubStatus.Unknown, 0, ex.Error.ErrCode, ex.Error.Description, msgId);
            }
            await Task.Delay(2 << transient); // 4, 8, 16, 32, 64 ms
        }
        catch (NatsJSApiException ex)
        {
            return new(PubStatus.Rejected, 0, ex.Error.ErrCode, ex.Error.Description, msgId);
        }
        catch (Exception ex) when (ex is NatsJSPublishNoResponseException or NatsNoReplyException or NatsTimeoutException
                                        or NatsJSTimeoutException or NatsJSApiNoResponseException or TimeoutException)
        {
            Stats.Inc("unknownOutcomes");
            if (++unknown > 5) return new(PubStatus.Unknown, 0, 0, ex.GetType().Name, msgId);
            await Task.Delay(250 * unknown);
        }
    }
}

// Leader read (design section 3): INatsJSStream.GetAsync is $JS.API.STREAM.MSG.GET, answered by the stream
// leader only, so it is read-after-write coherent (unlike Direct Get). 10037 = no message on the subject.
static async Task<LeaderMsg?> LeaderGet(INatsJSStream stream, string bucket, string key)
{
    StreamMsgGetResponse resp;
    try { resp = await stream.GetAsync(new StreamMsgGetRequest { LastBySubj = $"$KV.{bucket}.{key}" }); }
    catch (NatsJSApiException ex) when (ex.Error.ErrCode == 10037) { return null; }
    var m = resp.Message;
    return new LeaderMsg(m.Seq, m.Time, ParseHeaders(m.Hdrs), m.Data.ToArray());
}

// NATS header block: "NATS/1.0[ status]\r\nName: value\r\n...\r\n\r\n" (base64 in the API response).
static Dictionary<string, List<string>> ParseHeaders(string? hdrs)
{
    var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
    if (string.IsNullOrEmpty(hdrs)) return result;
    string text;
    try { text = Encoding.UTF8.GetString(Convert.FromBase64String(hdrs)); }
    catch (FormatException) { text = hdrs; }
    foreach (var line in text.Split("\r\n").Skip(1)) // first line is the version/status line
    {
        var idx = line.IndexOf(':');
        if (idx <= 0) continue;
        var name = line[..idx].Trim();
        if (!result.TryGetValue(name, out var values)) result[name] = values = [];
        values.Add(line[(idx + 1)..].Trim());
    }
    return result;
}

static bool IsFree(LeaderMsg m) =>
    m.Has("Nats-Marker-Reason") || m.First("KV-Operation") is "DEL" or "PURGE";

// Acquire (design section 5): publish a fresh token expecting an empty subject; on 10071 or an
// unresolved outcome, leader-read the subject: our own token = owned (the write committed although
// we did not see the ack), tombstone or TTL marker = free -> publish expecting its sequence (new msg id),
// empty = publish expecting 0, someone else's live token = held. At most 3 publish attempts.
static async Task<Acquired?> HelperAcquire(INatsJSContext js, INatsJSStream stream, string bucket, string key,
    TimeSpan ttl, List<string>? trace = null)
{
    var token = Encoding.UTF8.GetBytes($"owner:{Guid.NewGuid():N}");
    var expected = 0UL;
    for (var attempt = 0; attempt < 3; attempt++)
    {
        var r = await Publish(js, bucket, key, token, ttl, expected);
        trace?.Add($"pub(exp {expected}) -> {r}");
        if (r.Status == PubStatus.Committed) return new(r.Seq, token, null);
        if (r.Status == PubStatus.Rejected && r.ErrCode != 10071)
            throw new InvalidOperationException($"acquire {r.ErrCode} {r.Err}");
        if (r.Status == PubStatus.Rejected && LastSeqFromError(r.Err) == 0) { expected = 0; continue; }

        var last = await LeaderGet(stream, bucket, key);
        trace?.Add(last is null ? "leader -> none" : $"leader -> seq {last.Seq} own={last.Data.AsSpan().SequenceEqual(token)} free={IsFree(last)}");
        if (last is null) { expected = 0; continue; }
        if (last.Data.AsSpan().SequenceEqual(token)) return new(last.Seq, token, last.Time); // lease started at commit
        if (!IsFree(last)) return null;
        expected = last.Seq;
    }
    return null;
}

// Release: DEL expecting the owner's latest revision; an unresolved outcome is settled by a leader read.
static async Task<bool> HelperRelease(INatsJSContext js, INatsJSStream stream, string bucket, string key, ulong rev)
{
    var r = await Publish(js, bucket, key, [], expected: rev, del: true);
    if (r.Status == PubStatus.Committed) return true;
    if (r.Status == PubStatus.Rejected) return false;
    var last = await LeaderGet(stream, bucket, key);
    return last is null || (IsFree(last) && last.Seq > rev);
}

static ulong? LastSeqFromError(string? err)
{
    const string prefix = "wrong last sequence: ";
    var i = err?.IndexOf(prefix, StringComparison.Ordinal) ?? -1;
    return i >= 0 && ulong.TryParse(err![(i + prefix.Length)..].Trim(), out var seq) ? seq : null;
}

static async Task<(ulong? DeletedRev, bool NotFound)> WaitForDeleted(INatsKVStore kv, string key, TimeSpan timeout)
{
    var sw = Stopwatch.StartNew();
    while (sw.Elapsed < timeout)
    {
        try { await kv.GetEntryAsync<string>(key); }
        catch (NatsKVKeyDeletedException ex) { return (ex.Revision, false); }
        catch (NatsKVKeyNotFoundException) { return (null, true); }
        await Task.Delay(50);
    }
    return (null, false);
}

static double Pct(List<double> xs, double p)
{
    if (xs.Count == 0) return double.NaN;
    var s = xs.OrderBy(x => x).ToList();
    return Math.Round(s[Math.Clamp((int)Math.Ceiling(p / 100 * s.Count) - 1, 0, s.Count - 1)], 3);
}

// ---------------------------------------------------------------- tests

// Per-key TTL writes a MaxAge delete marker on expiry, and the marker itself expires.
async Task<TestResult> TtlMarkers(INatsKVStore kv, INatsJSStream stream)
{
    const string key = "t1.k";
    var ops = new List<string>();
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
    var watch = Task.Run(async () =>
    {
        try
        {
            await foreach (var e in kv.WatchAsync<string>(key, opts: new NatsKVWatchOpts { MetaOnly = true, IgnoreDeletes = false }, cancellationToken: cts.Token))
                lock (ops) ops.Add($"{e.Operation}@{e.Revision}");
        }
        catch (OperationCanceledException) { }
    });

    await Task.Delay(300);
    var sw = Stopwatch.StartNew();
    var rev = await kv.CreateAsync(key, "v", TimeSpan.FromSeconds(3));
    var (markerRev, _) = await WaitForDeleted(kv, key, TimeSpan.FromSeconds(10));
    var expiredAfter = sw.Elapsed.TotalSeconds;
    var leader = await LeaderGet(stream, CacheBucket, key);
    await Task.Delay(TimeSpan.FromSeconds(4)); // marker TTL (2 s) passes
    var gone = await LeaderGet(stream, CacheBucket, key) is null;
    await Task.Delay(500);
    cts.Cancel();
    await watch;

    var reason = leader?.First("Nats-Marker-Reason");
    string[] watchOps;
    lock (ops) watchOps = [.. ops];
    var watchOk = watchOps.Length >= 2 && watchOps[0] == $"Put@{rev}" && watchOps[1] == $"Purge@{markerRev}";
    var pass = markerRev > rev && expiredAfter is >= 2.5 and < 6 && reason == "MaxAge" && gone && watchOk;
    return new TestResult("ttl-markers", pass, new()
    {
        ["createRev"] = rev, ["markerRev"] = markerRev, ["expiredAfterS"] = Math.Round(expiredAfter, 2),
        ["markerReason"] = reason, ["markerGoneAfterMarkerTtl"] = gone, ["watchOps"] = string.Join(",", watchOps),
        ["watchPutThenPurge"] = watchOk,
    });
}

// Fence rule (section 5): expected 0 fails while a marker exists; expected = marker revision succeeds;
// after the marker expires, expected 0 succeeds.
async Task<TestResult> CasMarker(INatsKVStore kv, INatsJSContext js, INatsJSStream stream)
{
    const string key = "t2.k";
    await kv.CreateAsync(key, "v1", TimeSpan.FromSeconds(2));
    var marker = (await WaitForDeleted(kv, key, TimeSpan.FromSeconds(8))).DeletedRev ?? 0;
    var zero = await Publish(js, CacheBucket, key, "v2"u8.ToArray(), TimeSpan.FromSeconds(30), expected: 0);
    var onMarker = await Publish(js, CacheBucket, key, "v2"u8.ToArray(), TimeSpan.FromSeconds(30), expected: marker);
    var entry = await kv.GetEntryAsync<string>(key);

    const string key2 = "t2.b";
    await kv.CreateAsync(key2, "v1", TimeSpan.FromSeconds(2));
    var marker2 = (await WaitForDeleted(kv, key2, TimeSpan.FromSeconds(8))).DeletedRev ?? 0;
    await Task.Delay(TimeSpan.FromSeconds(4)); // marker expires
    var afterMarker = await LeaderGet(stream, CacheBucket, key2);
    var zeroAfter = await Publish(js, CacheBucket, key2, "v2"u8.ToArray(), TimeSpan.FromSeconds(30), expected: 0);

    var pass = marker > 0 && zero.ErrCode == 10071 && LastSeqFromError(zero.Err) == marker
               && onMarker.Status == PubStatus.Committed && entry.Revision == onMarker.Seq
               && marker2 > 0 && afterMarker is null && zeroAfter.Status == PubStatus.Committed;
    return new TestResult("cas-marker", pass, new()
    {
        ["markerRev"] = marker, ["expectZeroOnMarker"] = zero.ToString(), ["expectMarkerRev"] = onMarker.ToString(),
        ["entryRevAfter"] = entry.Revision, ["marker2Rev"] = marker2, ["subjectEmptyAfterMarkerTtl"] = afterMarker is null,
        ["expectZeroAfterMarkerExpired"] = zeroAfter.ToString(),
    });
}

// DEL through the publish helper: PubAck sequence = tombstone revision seen by readers; a stale fenced
// delete is rejected; resending the same msg id is deduplicated to the original sequence.
async Task<TestResult> DelHelper(INatsKVStore kv, INatsJSContext js)
{
    const string key = "t3.k";
    var put = await Publish(js, CacheBucket, key, "v"u8.ToArray(), TimeSpan.FromSeconds(30));
    var del = await Publish(js, CacheBucket, key, [], expected: put.Seq, del: true);
    ulong? seenRev = null;
    var op = "none";
    try { await kv.GetEntryAsync<string>(key); op = "put"; }
    catch (NatsKVKeyDeletedException ex) { seenRev = ex.Revision; op = "deleted"; }
    var resend = await Publish(js, CacheBucket, key, [], expected: put.Seq, del: true, msgId: del.MsgId);
    var staleDel = await Publish(js, CacheBucket, key, [], expected: put.Seq, del: true);

    var pass = put.Status == PubStatus.Committed && del.Status == PubStatus.Committed && seenRev == del.Seq
               && resend.Status == PubStatus.Committed && resend.Seq == del.Seq && staleDel.ErrCode == 10071;
    return new TestResult("del-helper", pass, new()
    {
        ["putSeq"] = put.Seq, ["delSeq"] = del.Seq, ["readerOp"] = op, ["readerRev"] = seenRev,
        ["sameMsgIdResend"] = resend.ToString(), ["staleFencedDelete"] = staleDel.ToString(),
    });
}

// Direct Get vs leader read latency at 0.5 CPU per NATS node (host client over loopback: a lower bound).
async Task<TestResult> ReadLatency(INatsKVStore kv, INatsJSStream stream)
{
    const string key = "t4.k";
    await kv.PutAsync(key, new string('x', 1024));
    var direct = new List<double>();
    var leader = new List<double>();
    for (var i = 0; i < 50; i++) { await kv.GetEntryAsync<string>(key); await LeaderGet(stream, CacheBucket, key); } // warm-up
    for (var i = 0; i < 1000; i++)
    {
        var sw = Stopwatch.StartNew();
        await kv.GetEntryAsync<string>(key);
        direct.Add(sw.Elapsed.TotalMilliseconds);
        sw.Restart();
        await LeaderGet(stream, CacheBucket, key);
        leader.Add(sw.Elapsed.TotalMilliseconds);
    }
    var pass = Pct(direct, 50) < 5 && Pct(leader, 50) < 5 && Pct(leader, 99) < 25;
    return new TestResult("read-latency", pass, new()
    {
        ["directP50Ms"] = Pct(direct, 50), ["directP99Ms"] = Pct(direct, 99),
        ["leaderP50Ms"] = Pct(leader, 50), ["leaderP99Ms"] = Pct(leader, 99),
    });
}

// Read-after-write coherency right after each write: Direct Get vs leader read, from the other servers.
async Task<TestResult> DirectStaleness()
{
    const string key = "t5.k";
    await using var pool = await ConnPool.Create(Connect, 2, offset: 1);
    var readers = new List<(INatsKVStore Kv, INatsJSStream Stream)>();
    foreach (var nc in pool.Conns)
    {
        var js = new NatsJSContext(nc);
        readers.Add((await new NatsKVContext(js).GetStoreAsync(CacheBucket), await js.GetStreamAsync($"KV_{CacheBucket}")));
    }
    int directStale = 0, leaderStale = 0, n = 2000;
    for (var i = 0; i < n; i++)
    {
        var w = await Publish(js0, CacheBucket, key, Encoding.UTF8.GetBytes(i.ToString()), TimeSpan.FromSeconds(60));
        var (kv, stream) = readers[i % readers.Count];
        if ((await kv.GetEntryAsync<string>(key)).Revision < w.Seq) directStale++;
        var l = await LeaderGet(stream, CacheBucket, key);
        if (l is null || l.Seq < w.Seq) leaderStale++;
    }
    return new TestResult("direct-staleness", leaderStale == 0, new()
    {
        ["reads"] = n, ["directStale"] = directStale, ["directStalePct"] = Math.Round(100.0 * directStale / n, 3),
        ["leaderStale"] = leaderStale,
        ["note"] = "lower bound: some Direct Gets are answered by the leader itself",
    });
}

// Five nodes race NATS.Net CreateAsync on one lock key (kept as a baseline; the design does not use it).
async Task<TestResult> CreateContention()
{
    const string key = "t6.lock";
    await using var pool = await ConnPool.Create(Connect, 5);
    var stores = new List<INatsKVStore>();
    foreach (var nc in pool.Conns) stores.Add(await new NatsKVContext(new NatsJSContext(nc)).GetStoreAsync(LocksBucket));
    int rounds = 300, badRounds = 0, zeroWinnerRounds = 0;
    var otherErrors = new Dictionary<string, int>();
    for (var r = 0; r < rounds; r++)
    {
        var outcome = await Task.WhenAll(stores.Select(async (s, i) =>
        {
            try { return (i, rev: await s.CreateAsync(key, $"node{i}:{r}", TimeSpan.FromSeconds(12)), err: (string?)null); }
            catch (Exception ex) when (ex is NatsKVCreateException or NatsKVWrongLastRevisionException) { return (i, rev: 0UL, err: (string?)null); }
            catch (Exception ex) { return (i, rev: 0UL, err: ex.GetType().Name); }
        }));
        foreach (var o in outcome.Where(o => o.err is not null)) otherErrors[o.err!] = otherErrors.GetValueOrDefault(o.err!) + 1;
        var winners = outcome.Where(o => o.rev > 0).ToList();
        if (winners.Count == 0) zeroWinnerRounds++;
        if (winners.Count > 1) badRounds++;
        foreach (var w in winners) await stores[w.i].DeleteAsync(key, new NatsKVDeleteOpts { Revision = w.rev });
    }
    return new TestResult("create-contention", badRounds == 0, new()
    {
        ["rounds"] = rounds, ["multiWinnerRounds"] = badRounds, ["zeroWinnerRounds"] = zeroWinnerRounds,
        ["otherErrors"] = JsonSerializer.Serialize(otherErrors),
        ["note"] = "winner releases after all losers finish, so this does not exercise the #5162 window",
    });
}

// Five nodes contend with the helper acquire while each owner releases after a short hold, so releases
// race the losers' leader reads and second publishes. Mutual exclusion is checked on server sequences:
// ownership intervals [acquireSeq, releaseSeq] must not overlap.
async Task<TestResult> HelperContention()
{
    const string key = "t10.lock";
    const int nodes = 5, acquisitionsPerNode = 100;
    await using var pool = await ConnPool.Create(Connect, nodes);
    var intervals = new ConcurrentBag<(ulong Acq, ulong Rel, int Node)>();
    int errors = 0, starved = 0, releaseFailures = 0;
    long heldRetries = 0;
    var errorKinds = new ConcurrentDictionary<string, int>();
    var waits = new ConcurrentBag<double>();
    await Task.WhenAll(Enumerable.Range(0, nodes).Select(async n =>
    {
        var js = new NatsJSContext(pool.Conns[n]);
        var stream = await js.GetStreamAsync($"KV_{LocksBucket}");
        var rnd = new Random(n);
        for (var a = 0; a < acquisitionsPerNode; a++)
        {
            var sw = Stopwatch.StartNew();
            Acquired? lease = null;
            while (lease is null && sw.Elapsed < TimeSpan.FromSeconds(10))
            {
                try { lease = await HelperAcquire(js, stream, LocksBucket, key, TimeSpan.FromSeconds(12)); }
                catch (Exception ex) { Interlocked.Increment(ref errors); errorKinds.AddOrUpdate($"acquire {ex.GetType().Name}", 1, (_, x) => x + 1); }
                if (lease is null) { Interlocked.Increment(ref heldRetries); await Task.Delay(rnd.Next(1, 4)); }
            }
            if (lease is null) { Interlocked.Increment(ref starved); continue; }
            waits.Add(sw.Elapsed.TotalMilliseconds);
            await Task.Delay(rnd.Next(5, 9)); // hold
            var rel = await Publish(js, LocksBucket, key, [], expected: lease.Seq, del: true);
            if (rel.Status != PubStatus.Committed) { Interlocked.Increment(ref releaseFailures); errorKinds.AddOrUpdate($"release {rel}", 1, (_, x) => x + 1); continue; }
            intervals.Add((lease.Seq, rel.Seq, n));
        }
    }));

    var sorted = intervals.OrderBy(i => i.Acq).ToList();
    var overlaps = sorted.Zip(sorted.Skip(1)).Count(p => p.Second.Acq < p.First.Rel);
    var pass = overlaps == 0 && errors == 0 && starved == 0 && releaseFailures == 0 && sorted.Count == nodes * acquisitionsPerNode;
    return new TestResult("helper-contention", pass, new()
    {
        ["nodes"] = nodes, ["acquisitions"] = sorted.Count, ["overlappingOwnerships"] = overlaps,
        ["errors"] = errors, ["starved"] = starved, ["releaseFailures"] = releaseFailures, ["heldRetries"] = heldRetries,
        ["acquireWaitP50Ms"] = Pct([.. waits], 50), ["acquireWaitP99Ms"] = Pct([.. waits], 99),
        ["errorKinds"] = JsonSerializer.Serialize(errorKinds),
    });
}

// Overwriting a TTL'd message must not let the old timer remove or marker the new one; renewals with the
// lease token keep a lease alive past its original TTL, and it expires one TTL after the last renewal.
async Task<TestResult> TtlOverwrite(INatsKVStore kv, INatsKVStore lk, INatsJSContext js, INatsJSStream cacheStream, INatsJSStream locksStream)
{
    const string key = "t11.k";
    var first = await Publish(js, CacheBucket, key, "short"u8.ToArray(), TimeSpan.FromSeconds(5));
    var second = await Publish(js, CacheBucket, key, "long"u8.ToArray(), TimeSpan.FromSeconds(60), expected: first.Seq);
    await Task.Delay(TimeSpan.FromSeconds(10));
    var leader = await LeaderGet(cacheStream, CacheBucket, key);
    var overwriteOk = second.Status == PubStatus.Committed && leader is { } l && l.Seq == second.Seq
                      && Encoding.UTF8.GetString(l.Data) == "long" && !l.Has("Nats-Marker-Reason");

    const string lease = "t11.lease";
    var leaseTtl = TimeSpan.FromSeconds(5);
    var acquired = await HelperAcquire(js, locksStream, LocksBucket, lease, leaseTtl);
    var renewals = 0;
    var cur = acquired?.Seq ?? 0;
    var sw = Stopwatch.StartNew();
    while (acquired is not null && sw.Elapsed < TimeSpan.FromSeconds(15))
    {
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var r = await Publish(js, LocksBucket, lease, acquired.Token, leaseTtl, expected: cur);
        if (r.Status != PubStatus.Committed) break;
        cur = r.Seq;
        renewals++;
    }
    var afterRenewals = await LeaderGet(locksStream, LocksBucket, lease);
    var aliveAfterRenewals = afterRenewals is { } a && a.Seq == cur && !IsFree(a);
    var stop = Stopwatch.StartNew();
    double? expiredAfterS = null;
    while (stop.Elapsed < TimeSpan.FromSeconds(15))
    {
        try { await lk.GetEntryAsync<string>(lease); }
        catch (Exception ex) when (ex is NatsKVKeyDeletedException or NatsKVKeyNotFoundException) { expiredAfterS = stop.Elapsed.TotalSeconds; break; }
        await Task.Delay(50);
    }
    var pass = overwriteOk && acquired is not null && renewals >= 9 && aliveAfterRenewals && expiredAfterS is >= 3.5 and <= 7;
    return new TestResult("ttl-overwrite", pass, new()
    {
        ["overwriteOk"] = overwriteOk, ["acquired"] = acquired is not null, ["renewals"] = renewals,
        ["aliveAfter15sOfRenewals"] = aliveAfterRenewals,
        ["expiredAfterLastRenewalS"] = expiredAfterS is null ? null : Math.Round(expiredAfterS.Value, 2),
    });
}

// Lock acquire/release cycles on file-backed R3 at 0.5 CPU per node, one key per worker (so every failed
// acquire is spurious). mode "kv" = NATS.Net CreateAsync/DeleteAsync (evidence, expected to fail);
// mode "helper" = the design's helper acquire and release.
async Task<TestResult> LockThroughput(string mode)
{
    // SPIKE_WORKERS sets the closed-loop concurrency for the design section 9 sweep (8/16/32/64); default 16.
    var workers = int.TryParse(Environment.GetEnvironmentVariable("SPIKE_WORKERS"), out var w0) && w0 > 0 ? w0 : 16;
    var duration = TimeSpan.FromSeconds(15);
    await using var pool = await ConnPool.Create(Connect, 3);
    var ctx = new List<(INatsJSContext Js, INatsKVStore Kv, INatsJSStream Stream)>();
    foreach (var nc in pool.Conns)
    {
        var js = new NatsJSContext(nc);
        ctx.Add((js, await new NatsKVContext(js).GetStoreAsync(LocksBucket), await js.GetStreamAsync($"KV_{LocksBucket}")));
    }
    long cycles = 0, errors = 0;
    var latencies = new ConcurrentBag<double>();
    var errorKinds = new ConcurrentDictionary<string, int>();
    var traces = new ConcurrentQueue<string>();
    var deadline = DateTime.UtcNow + duration;
    await Task.WhenAll(Enumerable.Range(0, workers).Select(async w =>
    {
        var (js, kv, stream) = ctx[w % ctx.Count];
        var key = $"t7.{mode}.w{w}";
        while (DateTime.UtcNow < deadline)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                if (mode == "kv")
                {
                    var rev = await kv.CreateAsync(key, "owner", TimeSpan.FromSeconds(12));
                    await kv.DeleteAsync(key, new NatsKVDeleteOpts { Revision = rev });
                }
                else
                {
                    var trace = new List<string>();
                    var lease = await HelperAcquire(js, stream, LocksBucket, key, TimeSpan.FromSeconds(12), trace);
                    if (lease is null)
                    {
                        if (traces.Count < 5) traces.Enqueue(string.Join(" | ", trace));
                        throw new InvalidOperationException("spurious lost acquire");
                    }
                    if (!await HelperRelease(js, stream, LocksBucket, key, lease.Seq)) throw new InvalidOperationException("release failed");
                }
                Interlocked.Increment(ref cycles);
                latencies.Add(sw.Elapsed.TotalMilliseconds);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                var msg = ex.Message.Length > 80 ? ex.Message[..80] : ex.Message;
                errorKinds.AddOrUpdate($"{ex.GetType().Name}: {msg}", 1, (_, n) => n + 1);
            }
        }
    }));
    return new TestResult($"lock-throughput-{mode}", errors == 0 && cycles > 0, new()
    {
        ["workers"] = workers, ["cycles"] = cycles, ["cyclesPerSec"] = Math.Round(cycles / duration.TotalSeconds, 1),
        ["cycleP50Ms"] = Pct([.. latencies], 50), ["cycleP99Ms"] = Pct([.. latencies], 99), ["errors"] = errors,
        ["errorPct"] = Math.Round(100.0 * errors / Math.Max(1, cycles + errors), 2),
        ["errorKinds"] = JsonSerializer.Serialize(errorKinds), ["failedAcquireTraces"] = traces.ToArray(),
    });
}

// Ordered consumer asked to start below FirstSeq (events already discarded): error or silent skip?
async Task<TestResult> ConsumerBelowFirstSeq(INatsJSContext js)
{
    try { await js.DeleteStreamAsync(NotifyStream); } catch (NatsJSApiException) { }
    await js.CreateStreamAsync(new StreamConfig(NotifyStream, ["spike.notify.>"])
    {
        Storage = StreamConfigStorage.File, NumReplicas = 3, MaxMsgs = 20,
    });
    for (var i = 1; i <= 50; i++)
        (await js.PublishAsync($"spike.notify.k{i}", Encoding.UTF8.GetBytes(i.ToString()))).EnsureSuccess();
    var firstSeq = (await js.GetStreamAsync(NotifyStream)).Info.State.FirstSeq;

    var consumer = await js.CreateOrderedConsumerAsync(NotifyStream, new NatsJSOrderedConsumerOpts
    {
        DeliverPolicy = ConsumerConfigDeliverPolicy.ByStartSequence,
        OptStartSeq = 5,
    });
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    ulong? firstDelivered = null;
    string? error = null;
    try
    {
        await foreach (var m in consumer.ConsumeAsync<byte[]>(cancellationToken: cts.Token))
        {
            firstDelivered = m.Metadata?.Sequence.Stream;
            break;
        }
    }
    catch (OperationCanceledException) { error = "timeout, nothing delivered"; }
    catch (Exception ex) { error = ex.GetType().Name + ": " + ex.Message; }

    // The design detects this on the consuming side: a stream-sequence jump > 1 means lost events.
    var gapDetected = firstDelivered is { } f && f > 5;
    return new TestResult("consumer-below-firstseq", gapDetected && firstDelivered == firstSeq, new()
    {
        ["requestedStart"] = 5, ["streamFirstSeq"] = firstSeq, ["firstDelivered"] = firstDelivered,
        ["silentSkip"] = firstDelivered == firstSeq, ["error"] = error, ["gapDetectableFromSequence"] = gapDetected,
    });
}

// Unknown outcomes: workers cycle helper acquire/release on their own keys while the locks stream leader
// is restarted twice. Publishes in flight time out; the helper resends with the same msg id and settles
// by dedup or leader read. Afterwards every key must be free: no lock left behind, no spurious loss.
async Task<TestResult> LeaderFailover()
{
    const int workers = 6;
    var duration = TimeSpan.FromSeconds(40);
    await using var pool = await ConnPool.Create((i, _) => Connect(i, allServers: true), 3);
    var ctx = new List<(INatsJSContext Js, INatsJSStream Stream)>();
    foreach (var nc in pool.Conns)
    {
        var js = new NatsJSContext(nc);
        ctx.Add((js, await js.GetStreamAsync($"KV_{LocksBucket}")));
    }
    long cycles = 0, lost = 0, releaseFailed = 0, errors = 0;
    var errorKinds = new ConcurrentDictionary<string, int>();
    double maxCycleMs = 0;
    var restarted = new List<string>();
    var deadline = DateTime.UtcNow + duration;

    var chaos = Task.Run(async () =>
    {
        foreach (var wait in new[] { 8, 14 })
        {
            await Task.Delay(TimeSpan.FromSeconds(wait));
            string leaderName;
            try { leaderName = (await js0.GetStreamAsync($"KV_{LocksBucket}")).Info.Cluster?.Leader ?? "nats-1"; }
            catch (Exception) { leaderName = "nats-1"; }
            using var p = Process.Start(new ProcessStartInfo("docker", $"restart -t 0 {project}-{leaderName}-1")
                { RedirectStandardOutput = true, RedirectStandardError = true })!;
            await p.WaitForExitAsync();
            lock (restarted) restarted.Add($"{leaderName} (exit {p.ExitCode})");
        }
    });

    await Task.WhenAll(Enumerable.Range(0, workers).Select(async w =>
    {
        var (js, stream) = ctx[w % ctx.Count];
        var key = $"t12.w{w}";
        while (DateTime.UtcNow < deadline)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                var lease = await HelperAcquire(js, stream, LocksBucket, key, TimeSpan.FromSeconds(12));
                if (lease is null) { Interlocked.Increment(ref lost); await Task.Delay(100); continue; }
                await Task.Delay(5);
                if (!await HelperRelease(js, stream, LocksBucket, key, lease.Seq)) Interlocked.Increment(ref releaseFailed);
                Interlocked.Increment(ref cycles);
            }
            catch (Exception ex)
            {
                // Leader reads during an election can fail; the next cycle retries.
                Interlocked.Increment(ref errors);
                errorKinds.AddOrUpdate(ex.GetType().Name, 1, (_, n) => n + 1);
                await Task.Delay(200);
            }
            var ms = sw.Elapsed.TotalMilliseconds;
            lock (errorKinds) maxCycleMs = Math.Max(maxCycleMs, ms);
        }
    }));
    await chaos;
    await Task.Delay(TimeSpan.FromSeconds(3)); // let the restarted node catch up before the final check

    var stuck = 0;
    for (var w = 0; w < workers; w++)
    {
        var last = await LeaderGet(ctx[0].Stream, LocksBucket, $"t12.w{w}");
        if (last is not null && !IsFree(last)) stuck++;
    }
    var snapshot = Stats.Snapshot();
    var pass = restarted.Count == 2 && restarted.All(r => r.EndsWith("(exit 0)")) && cycles > 0
               && lost == 0 && releaseFailed == 0 && stuck == 0;
    return new TestResult("leader-failover", pass, new()
    {
        ["restartedLeaders"] = string.Join(", ", restarted), ["cycles"] = cycles, ["spuriousLost"] = lost,
        ["releaseFailed"] = releaseFailed, ["stuckLocks"] = stuck, ["transientErrors"] = errors,
        ["errorKinds"] = JsonSerializer.Serialize(errorKinds), ["maxCycleMs"] = Math.Round(maxCycleMs),
        ["unknownOutcomes"] = snapshot.GetValueOrDefault("unknownOutcomes"),
        ["duplicateAcks"] = snapshot.GetValueOrDefault("duplicateAcks"),
    });
}

// Phase 1 of the restart test: a TTL'd lock and a plain value, then the script restarts all NATS nodes.
async Task<TestResult> RestartSetup(INatsKVStore c, INatsKVStore l)
{
    var ttl = TimeSpan.FromSeconds(45);
    await c.PutAsync("t9.data", "persist");
    var rev = await l.CreateAsync("t9.lock", "owner", ttl);
    var state = new Dictionary<string, object?> { ["createdAt"] = DateTimeOffset.UtcNow.ToString("O"), ["ttlS"] = ttl.TotalSeconds, ["lockRev"] = rev };
    await File.WriteAllTextAsync(Path.Combine(resultsDir, $"{label}-restart-state.json"), JsonSerializer.Serialize(state));
    return new TestResult("restart-setup", true, state);
}

// Phase 2: after the full restart, data survived and the lock still expires on time (TTL timer resumed).
async Task<TestResult> RestartCheck()
{
    var state = JsonNode.Parse(await File.ReadAllTextAsync(Path.Combine(resultsDir, $"{label}-restart-state.json")))!;
    var createdAt = DateTimeOffset.Parse((string)state["createdAt"]!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
    var ttlS = (double)state["ttlS"]!;
    if (DateTimeOffset.UtcNow - createdAt > TimeSpan.FromMinutes(5))
        return new TestResult("restart-check", false, new() { ["error"] = "stale state file; run restart-setup first" });
    string? data = null;
    try { data = (await cache.GetEntryAsync<string>("t9.data")).Value; }
    catch (Exception ex) when (ex is NatsKVKeyDeletedException or NatsKVKeyNotFoundException) { }

    double? goneAfterS = null;
    var limit = createdAt + TimeSpan.FromSeconds(ttlS + 60);
    while (DateTimeOffset.UtcNow < limit)
    {
        try { await locks.GetEntryAsync<string>("t9.lock"); }
        catch (Exception ex) when (ex is NatsKVKeyDeletedException or NatsKVKeyNotFoundException) { goneAfterS = (DateTimeOffset.UtcNow - createdAt).TotalSeconds; break; }
        await Task.Delay(250);
    }
    var pass = data == "persist" && goneAfterS is { } g && g >= ttlS - 1 && g <= ttlS + 10;
    return new TestResult("restart-check", pass, new()
    {
        ["dataSurvived"] = data == "persist", ["lockTtlS"] = ttlS,
        ["lockGoneAfterS"] = goneAfterS is null ? null : Math.Round(goneAfterS.Value, 1),
    });
}

enum PubStatus { Committed, Rejected, Unknown }

record PubResult(PubStatus Status, ulong Seq, int ErrCode, string? Err, string MsgId)
{
    public override string ToString() => Status switch
    {
        PubStatus.Committed => $"committed seq {Seq}",
        PubStatus.Rejected => $"rejected {ErrCode} {Err}",
        _ => $"unknown {ErrCode} {Err}",
    };
}

record Acquired(ulong Seq, byte[] Token, DateTimeOffset? CommittedAt);
record TestResult(string Name, bool Pass, Dictionary<string, object?> Details);

record LeaderMsg(ulong Seq, DateTimeOffset Time, Dictionary<string, List<string>> Headers, byte[] Data)
{
    public bool Has(string name) => Headers.ContainsKey(name);
    public string? First(string name) => Headers.TryGetValue(name, out var v) ? v.FirstOrDefault() : null;
}

static class Stats
{
    static readonly ConcurrentDictionary<string, long> Counters = new();
    public static void Inc(string name) => Counters.AddOrUpdate(name, 1, (_, n) => n + 1);
    public static void Reset() => Counters.Clear();
    public static Dictionary<string, long> Snapshot() => new(Counters);
}

sealed class ConnPool : IAsyncDisposable
{
    public List<NatsConnection> Conns { get; } = [];

    public static async Task<ConnPool> Create(Func<int, bool, Task<NatsConnection>> connect, int count, int offset = 0)
    {
        var pool = new ConnPool();
        try
        {
            for (var i = 0; i < count; i++) pool.Conns.Add(await connect(i + offset, false));
            return pool;
        }
        catch
        {
            await pool.DisposeAsync();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in Conns) await c.DisposeAsync();
    }
}
