// Milestone-1 spike: verifies the NATS behaviours DESIGN.md v1.0 depends on.
// Each test prints a PASS/FAIL line and writes results/<name>.json.
// Usage: dotnet run -- <all|ttl-markers|cas-marker|del-helper|read-latency|direct-staleness|
//                       create-contention|lock-throughput-kv|lock-throughput-helper|consumer-below-firstseq|restart-setup|restart-check>

using System.Diagnostics;
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

var tests = new Dictionary<string, Func<Task<TestResult>>>
{
    ["ttl-markers"] = () => TtlMarkers(cache, nc0),
    ["cas-marker"] = () => CasMarker(cache, js0, nc0),
    ["del-helper"] = () => DelHelper(cache, js0),
    ["read-latency"] = () => ReadLatency(cache, nc0),
    ["direct-staleness"] = DirectStaleness,
    ["create-contention"] = CreateContention,
    ["lock-throughput-kv"] = () => LockThroughput("kv"),
    ["lock-throughput-helper"] = () => LockThroughput("helper"),
    ["consumer-below-firstseq"] = () => ConsumerBelowFirstSeq(js0),
    ["restart-setup"] = () => RestartSetup(cache, locks),
    ["restart-check"] = RestartCheck,
};

var toRun = cmd == "all"
    ? tests.Keys.Where(k => !k.StartsWith("restart") && !k.StartsWith("lock-throughput")).ToList()
    : [cmd];

foreach (var name in toRun)
{
    TestResult r;
    try { r = await tests[name](); }
    catch (Exception ex) { r = new TestResult(name, false, new() { ["exception"] = ex.ToString() }); }
    results.Add(r);
    Console.WriteLine($"{(r.Pass ? "PASS" : "FAIL")}  {r.Name,-26} {JsonSerializer.Serialize(r.Details)}");
    await File.WriteAllTextAsync(Path.Combine(resultsDir, $"{label}-{name}.json"),
        JsonSerializer.Serialize(r, new JsonSerializerOptions { WriteIndented = true }));
}

return results.All(r => r.Pass) ? 0 : 1;

// ---------------------------------------------------------------- setup and helpers

async Task<NatsConnection> Connect(int i)
{
    var nc = new NatsConnection(new NatsOpts { Url = urls[i % urls.Length], Name = $"spike-{i}", MaxReconnectRetry = -1 });
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
            var c = await kv.CreateOrUpdateStoreAsync(Config(CacheBucket));
            var l = await kv.CreateOrUpdateStoreAsync(Config(LocksBucket));
            return (c, l);
        }
        catch (Exception) when (attempt < 30)
        {
            await Task.Delay(2000); // JetStream meta leader not elected yet
        }
    }
}

// The TTL publish helper from design section 3: raw publish to $KV.{bucket}.{key}.
static async Task<(ulong Seq, int ErrCode, string? Err)> Publish(INatsJSContext js, string bucket, string key,
    byte[] data, TimeSpan? ttl = null, ulong? expected = null, bool del = false)
{
    var h = new NatsHeaders();
    if (ttl is { } t) h["Nats-TTL"] = $"{(int)Math.Ceiling(t.TotalSeconds)}s";
    if (expected is { } e) h["Nats-Expected-Last-Subject-Sequence"] = e.ToString();
    if (del) h["KV-Operation"] = "DEL";
    try
    {
        var ack = await js.PublishAsync($"$KV.{bucket}.{key}", data, headers: h);
        ack.EnsureSuccess();
        return (ack.Seq, 0, null);
    }
    catch (NatsJSApiException ex)
    {
        return (0, ex.Error.ErrCode, ex.Error.Description);
    }
}

// Leader read (design section 3): $JS.API.STREAM.MSG.GET is answered by the stream leader only.
static async Task<LeaderMsg?> LeaderGet(NatsConnection nc, string bucket, string key)
{
    var req = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new Dictionary<string, string> { ["last_by_subj"] = $"$KV.{bucket}.{key}" }));
    var reply = await nc.RequestAsync<byte[], byte[]>($"$JS.API.STREAM.MSG.GET.KV_{bucket}", req);
    var json = JsonNode.Parse(reply.Data!)!;
    if (json["error"] is { } err)
    {
        var code = (int?)err["err_code"] ?? 0;
        return code == 10037 ? null : throw new InvalidOperationException($"leader get error {err.ToJsonString()}");
    }
    var m = json["message"]!;
    var hdrs = new Dictionary<string, string>();
    if ((string?)m["hdrs"] is { } b64)
    {
        foreach (var line in Encoding.UTF8.GetString(Convert.FromBase64String(b64)).Split("\r\n").Skip(1))
        {
            var idx = line.IndexOf(':');
            if (idx > 0) hdrs[line[..idx].Trim()] = line[(idx + 1)..].Trim();
        }
    }
    return new LeaderMsg((ulong)m["seq"]!, DateTimeOffset.Parse((string)m["time"]!), hdrs);
}

static async Task<ulong?> WaitForDeleted(INatsKVStore kv, string key, TimeSpan timeout)
{
    var sw = Stopwatch.StartNew();
    while (sw.Elapsed < timeout)
    {
        try { await kv.GetEntryAsync<string>(key); }
        catch (NatsKVKeyDeletedException ex) { return ex.Revision; }
        catch (NatsKVKeyNotFoundException) { return 0; }
        await Task.Delay(100);
    }
    return null;
}

static double Pct(List<double> xs, double p)
{
    var s = xs.OrderBy(x => x).ToList();
    return Math.Round(s[Math.Min(s.Count - 1, (int)Math.Ceiling(p / 100 * s.Count) - 1)], 3);
}

// ---------------------------------------------------------------- tests

// Per-key TTL writes a MaxAge delete marker on expiry (>= 2.11.2), and the marker itself expires.
async Task<TestResult> TtlMarkers(INatsKVStore kv, NatsConnection nc)
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
    var markerRev = await WaitForDeleted(kv, key, TimeSpan.FromSeconds(10));
    var expiredAfter = sw.Elapsed.TotalSeconds;
    var leader = await LeaderGet(nc, CacheBucket, key);
    await Task.Delay(TimeSpan.FromSeconds(4)); // marker TTL (2 s) passes
    var gone = false;
    try { await kv.GetEntryAsync<string>(key); }
    catch (NatsKVKeyNotFoundException) { gone = true; }
    catch (NatsKVKeyDeletedException) { gone = false; }
    await Task.Delay(500);
    cts.Cancel();
    await watch;

    var reason = leader?.Headers.GetValueOrDefault("Nats-Marker-Reason");
    var pass = markerRev > rev && expiredAfter is >= 2.5 and < 6 && reason == "MaxAge" && gone;
    return new TestResult("ttl-markers", pass, new()
    {
        ["createRev"] = rev, ["markerRev"] = markerRev, ["expiredAfterS"] = Math.Round(expiredAfter, 2),
        ["markerReason"] = reason, ["markerGoneAfterMarkerTtl"] = gone, ["watchOps"] = string.Join(",", ops),
    });
}

// Fence rule (section 5): expected 0 fails while a marker exists; expected = marker revision succeeds;
// after the marker expires, expected 0 succeeds.
async Task<TestResult> CasMarker(INatsKVStore kv, INatsJSContext js, NatsConnection nc)
{
    const string key = "t2.k";
    await kv.CreateAsync(key, "v1", TimeSpan.FromSeconds(2));
    var marker = await WaitForDeleted(kv, key, TimeSpan.FromSeconds(8)) ?? 0;
    var zero = await Publish(js, CacheBucket, key, "v2"u8.ToArray(), TimeSpan.FromSeconds(30), expected: 0);
    var onMarker = await Publish(js, CacheBucket, key, "v2"u8.ToArray(), TimeSpan.FromSeconds(30), expected: marker);
    var entry = await kv.GetEntryAsync<string>(key);

    const string key2 = "t2.b";
    await kv.CreateAsync(key2, "v1", TimeSpan.FromSeconds(2));
    var marker2 = await WaitForDeleted(kv, key2, TimeSpan.FromSeconds(8)) ?? 0;
    await Task.Delay(TimeSpan.FromSeconds(4)); // marker expires
    var afterMarker = await LeaderGet(nc, CacheBucket, key2);
    var zeroAfter = await Publish(js, CacheBucket, key2, "v2"u8.ToArray(), TimeSpan.FromSeconds(30), expected: 0);

    var pass = zero.ErrCode == 10071 && onMarker.ErrCode == 0 && entry.Revision == onMarker.Seq
               && afterMarker is null && zeroAfter.ErrCode == 0;
    return new TestResult("cas-marker", pass, new()
    {
        ["markerRev"] = marker, ["expectZeroOnMarker"] = $"{zero.ErrCode} {zero.Err}",
        ["expectMarkerRev"] = onMarker.ErrCode == 0 ? $"ok seq {onMarker.Seq}" : $"{onMarker.ErrCode} {onMarker.Err}",
        ["entryRevAfter"] = entry.Revision, ["marker2Rev"] = marker2,
        ["subjectEmptyAfterMarkerTtl"] = afterMarker is null,
        ["expectZeroAfterMarkerExpired"] = zeroAfter.ErrCode == 0 ? $"ok seq {zeroAfter.Seq}" : $"{zeroAfter.ErrCode} {zeroAfter.Err}",
    });
}

// DEL through the publish helper: PubAck sequence = tombstone revision seen by readers.
async Task<TestResult> DelHelper(INatsKVStore kv, INatsJSContext js)
{
    const string key = "t3.k";
    var put = await Publish(js, CacheBucket, key, "v"u8.ToArray(), TimeSpan.FromSeconds(30));
    var del = await Publish(js, CacheBucket, key, [], expected: put.Seq, del: true);
    ulong? seenRev = null;
    string op = "none";
    try { await kv.GetEntryAsync<string>(key); op = "put"; }
    catch (NatsKVKeyDeletedException ex) { seenRev = ex.Revision; op = "deleted"; }
    var staleDel = await Publish(js, CacheBucket, key, [], expected: put.Seq, del: true);

    var pass = put.ErrCode == 0 && del.ErrCode == 0 && seenRev == del.Seq && staleDel.ErrCode == 10071;
    return new TestResult("del-helper", pass, new()
    {
        ["putSeq"] = put.Seq, ["delSeq"] = del.Seq, ["readerOp"] = op, ["readerRev"] = seenRev,
        ["staleFencedDelete"] = $"{staleDel.ErrCode} {staleDel.Err}",
    });
}

// Direct Get vs leader read latency at 0.5 CPU per NATS node.
async Task<TestResult> ReadLatency(INatsKVStore kv, NatsConnection nc)
{
    const string key = "t4.k";
    await kv.PutAsync(key, new string('x', 1024));
    var direct = new List<double>();
    var leader = new List<double>();
    for (var i = 0; i < 50; i++) { await kv.GetEntryAsync<string>(key); await LeaderGet(nc, CacheBucket, key); } // warm-up
    for (var i = 0; i < 1000; i++)
    {
        var sw = Stopwatch.StartNew();
        await kv.GetEntryAsync<string>(key);
        direct.Add(sw.Elapsed.TotalMilliseconds);
        sw.Restart();
        await LeaderGet(nc, CacheBucket, key);
        leader.Add(sw.Elapsed.TotalMilliseconds);
    }
    return new TestResult("read-latency", true, new()
    {
        ["directP50Ms"] = Pct(direct, 50), ["directP99Ms"] = Pct(direct, 99),
        ["leaderP50Ms"] = Pct(leader, 50), ["leaderP99Ms"] = Pct(leader, 99),
    });
}

// Read-after-write coherency: Direct Get on other servers vs leader read, right after each write.
async Task<TestResult> DirectStaleness()
{
    const string key = "t5.k";
    await using var nc1 = await Connect(1);
    await using var nc2 = await Connect(2);
    var readers = new[] { await new NatsKVContext(new NatsJSContext(nc1)).GetStoreAsync(CacheBucket),
                          await new NatsKVContext(new NatsJSContext(nc2)).GetStoreAsync(CacheBucket) };
    int directStale = 0, leaderStale = 0, n = 2000;
    for (var i = 0; i < n; i++)
    {
        var w = await Publish(js0, CacheBucket, key, Encoding.UTF8.GetBytes(i.ToString()), TimeSpan.FromSeconds(60));
        var e = await readers[i % 2].GetEntryAsync<string>(key);
        if (e.Revision < w.Seq) directStale++;
        var l = await LeaderGet(i % 2 == 0 ? nc1 : nc2, CacheBucket, key);
        if (l is null || l.Seq < w.Seq) leaderStale++;
    }
    return new TestResult("direct-staleness", leaderStale == 0, new()
    {
        ["reads"] = n, ["directStale"] = directStale, ["directStalePct"] = Math.Round(100.0 * directStale / n, 3),
        ["leaderStale"] = leaderStale,
    });
}

// Five nodes race KV Create on one lock key: exactly one winner per round, release by revision.
async Task<TestResult> CreateContention()
{
    const string key = "t6.lock";
    var conns = new List<NatsConnection>();
    var stores = new List<INatsKVStore>();
    for (var i = 0; i < 5; i++)
    {
        var nc = await Connect(i);
        conns.Add(nc);
        stores.Add(await new NatsKVContext(new NatsJSContext(nc)).GetStoreAsync(LocksBucket));
    }
    int rounds = 300, badRounds = 0, zeroWinnerRounds = 0;
    var otherErrors = new Dictionary<string, int>();
    for (var r = 0; r < rounds; r++)
    {
        var attempts = stores.Select(async (s, i) =>
        {
            try { return (i, rev: await s.CreateAsync(key, $"node{i}:{r}", TimeSpan.FromSeconds(12)), err: (string?)null); }
            catch (NatsKVCreateException) { return (i, rev: 0UL, err: (string?)null); }
            catch (NatsKVWrongLastRevisionException) { return (i, rev: 0UL, err: (string?)null); }
            catch (Exception ex) { return (i, rev: 0UL, err: ex.GetType().Name); }
        }).ToList();
        var outcome = await Task.WhenAll(attempts);
        foreach (var o in outcome.Where(o => o.err is not null))
            otherErrors[o.err!] = otherErrors.GetValueOrDefault(o.err!) + 1;
        var winners = outcome.Where(o => o.rev > 0).ToList();
        if (winners.Count == 0) zeroWinnerRounds++;
        if (winners.Count > 1) badRounds++;
        foreach (var w in winners)
            await stores[w.i].DeleteAsync(key, new NatsKVDeleteOpts { Revision = w.rev });
    }
    foreach (var c in conns) await c.DisposeAsync();
    return new TestResult("create-contention", badRounds == 0, new()
    {
        ["rounds"] = rounds, ["multiWinnerRounds"] = badRounds, ["zeroWinnerRounds"] = zeroWinnerRounds,
        ["otherErrors"] = JsonSerializer.Serialize(otherErrors),
    });
}

// Lock create/delete cycles on file-backed R3 at 0.5 CPU per node. Each worker owns its key, so
// every failed acquire is spurious. mode "kv" = NATS.Net CreateAsync/DeleteAsync; mode "helper" =
// the design's publish helper with a leader read when the subject already holds a tombstone or marker.
async Task<TestResult> LockThroughput(string mode)
{
    const int workers = 16;
    var duration = TimeSpan.FromSeconds(15);
    var conns = new List<NatsConnection>();
    var stores = new List<INatsKVStore>();
    var jss = new List<INatsJSContext>();
    for (var i = 0; i < 3; i++)
    {
        var nc = await Connect(i);
        conns.Add(nc);
        var js = new NatsJSContext(nc);
        jss.Add(js);
        stores.Add(await new NatsKVContext(js).GetStoreAsync(LocksBucket));
    }
    long cycles = 0, errors = 0;
    var latencies = new List<double>();
    var errorKinds = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
    var deadline = DateTime.UtcNow + duration;
    await Task.WhenAll(Enumerable.Range(0, workers).Select(async w =>
    {
        var s = stores[w % stores.Count];
        var js = jss[w % jss.Count];
        var nc = conns[w % conns.Count];
        var local = new List<double>();
        var key = $"t7.{mode}.w{w}";
        while (DateTime.UtcNow < deadline)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                if (mode == "kv")
                {
                    var rev = await s.CreateAsync(key, "owner", TimeSpan.FromSeconds(12));
                    await s.DeleteAsync(key, new NatsKVDeleteOpts { Revision = rev });
                }
                else
                {
                    var rev = await HelperAcquire(js, nc, LocksBucket, key, TimeSpan.FromSeconds(12))
                              ?? throw new InvalidOperationException("spurious lost acquire");
                    var rel = await Publish(js, LocksBucket, key, [], expected: rev, del: true);
                    if (rel.ErrCode != 0) throw new InvalidOperationException($"release {rel.ErrCode}");
                }
                Interlocked.Increment(ref cycles);
                local.Add(sw.Elapsed.TotalMilliseconds);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                var msg = ex.Message.Length > 80 ? ex.Message[..80] : ex.Message;
                errorKinds.AddOrUpdate($"{ex.GetType().Name}: {msg}", 1, (_, n) => n + 1);
            }
        }
        lock (latencies) latencies.AddRange(local);
    }));
    foreach (var c in conns) await c.DisposeAsync();
    return new TestResult($"lock-throughput-{mode}", errors == 0 && cycles > 0, new()
    {
        ["workers"] = workers, ["cycles"] = cycles, ["lockOpsPerSec"] = Math.Round(cycles * 2 / duration.TotalSeconds, 1),
        ["cycleP50Ms"] = Pct(latencies, 50), ["cycleP99Ms"] = Pct(latencies, 99), ["errors"] = errors,
        ["errorPct"] = Math.Round(100.0 * errors / Math.Max(1, cycles + errors), 2),
        ["errorKinds"] = JsonSerializer.Serialize(errorKinds),
    });
}

// Acquire: publish expecting an empty subject; on 10071 read the last message from the leader and,
// if it is a tombstone or TTL marker, publish again expecting exactly that sequence.
static async Task<ulong?> HelperAcquire(INatsJSContext js, NatsConnection nc, string bucket, string key, TimeSpan ttl)
{
    var first = await Publish(js, bucket, key, "owner"u8.ToArray(), ttl, expected: 0);
    if (first.ErrCode == 0) return first.Seq;
    if (first.ErrCode != 10071) throw new InvalidOperationException($"acquire {first.ErrCode} {first.Err}");
    var last = await LeaderGet(nc, bucket, key);
    var expected = 0UL;
    if (last is not null)
    {
        var free = last.Headers.ContainsKey("Nats-Marker-Reason")
                   || last.Headers.GetValueOrDefault("KV-Operation") is "DEL" or "PURGE";
        if (!free) return null; // held by a live owner
        expected = last.Seq;
    }
    var second = await Publish(js, bucket, key, "owner"u8.ToArray(), ttl, expected: expected);
    return second.ErrCode == 0 ? second.Seq : null;
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
    var stream = await js.GetStreamAsync(NotifyStream);
    var firstSeq = stream.Info.State.FirstSeq;

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

    var silentSkip = firstDelivered == firstSeq;
    // The design detects this case on the consuming side: a stream-sequence jump > 1 means lost events.
    var gapDetected = firstDelivered is { } f && f > 5;
    return new TestResult("consumer-below-firstseq", gapDetected, new()
    {
        ["requestedStart"] = 5, ["streamFirstSeq"] = firstSeq, ["firstDelivered"] = firstDelivered,
        ["silentSkip"] = silentSkip, ["error"] = error, ["gapDetectableFromSequence"] = gapDetected,
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
    var createdAt = DateTimeOffset.Parse((string)state["createdAt"]!);
    var ttlS = (double)state["ttlS"]!;
    string? data = null;
    try { data = (await cache.GetEntryAsync<string>("t9.data")).Value; } catch (NatsKVException) { }

    double? goneAfterS = null;
    var limit = createdAt + TimeSpan.FromSeconds(ttlS + 60);
    while (DateTimeOffset.UtcNow < limit)
    {
        try { await locks.GetEntryAsync<string>("t9.lock"); }
        catch (NatsKVException) { goneAfterS = (DateTimeOffset.UtcNow - createdAt).TotalSeconds; break; }
        await Task.Delay(250);
    }
    var pass = data == "persist" && goneAfterS is { } g && g >= ttlS - 1 && g <= ttlS + 10;
    return new TestResult("restart-check", pass, new()
    {
        ["dataSurvived"] = data == "persist", ["lockTtlS"] = ttlS,
        ["lockGoneAfterS"] = goneAfterS is null ? null : Math.Round(goneAfterS.Value, 1),
    });
}

record TestResult(string Name, bool Pass, Dictionary<string, object?> Details);
record LeaderMsg(ulong Seq, DateTimeOffset Time, Dictionary<string, string> Headers);
