# NATS Distributed Cache for .NET 10: Design Document

Sep 30, 2026 · Erik Simonič · Status: v1.2 (NATS requirement changed to the latest stable release; frozen after review 6). Changes from here go through a new review and a version bump

From v1.1 on this file is the source of truth; the online draft used up to v1.0 is no longer maintained. Spike evidence: `spike/RESULTS.md`.

## 1. Overview and goals

We are building a .NET 10 library that gives every app node a two-level cache (in-process L1, NATS JetStream KV L2), runs a value factory at most once per key across the whole cluster, and evicts stale L1 copies on all nodes within a bounded time after a write. It ships with a minimal-API test host, a 3-node NATS cluster, k6 load tests, and a validation suite that proves those guarantees under load and failure.

### Requirements

| ID | Requirement | Acceptance signal |
| --- | --- | --- |
| R1 | Distributed single-flight: one factory execution per key per miss, cluster-wide | Factory-call counter per key = 1 per expiry window, across 5 nodes |
| R2 | L1 (memory) + L2 (NATS KV) layering | L1 hit, L2 hit and miss rates exposed as metrics |
| R3 | Jittered expiration on L1 and L2 | Expiry timestamps spread over the configured jitter window, no synchronized stampede |
| R4 | Cross-node L1 eviction after Set / Update | Other nodes stop serving the old value within the invalidation SLO |
| R5 | Delete key, cluster-wide | L2 entry gone, all L1 copies evicted |
| R6 | Bucket naming with a prefix: `{prefix}_cache`, `{prefix}_locks`, `{prefix}_notifications` | Buckets/streams created with exactly those names |
| R7 | Resilient connection, automatic recovery | Library survives NATS node loss and full-cluster restart without app restart |
| R8 | k6 load test | Scripted scenarios with thresholds that fail the run |
| R9 | Everything on .NET 10 | `net10.0` for TestApi, Origin, Validator; libraries multi-target netstandard2.1 + net10.0 (Q17) |
| R10 | Test topology: 3 NATS nodes at 1 CPU each, 5 app nodes at 0.5 CPU each | Enforced by container limits in docker compose (scaled rig: halved, section 9) |
| R11 | Validation plan that asserts correctness | Automated checks with pass/fail, not dashboards only |

### Non-goals (v1)

- Strong consistency across nodes: L1 is eventually consistent, bounded by the invalidation SLO.
- Multi-region or leaf-node topologies.
- Large values inside KV messages: values above `LargeValueThresholdBytes` go to Object Store (section 3).
- Replacing `IDistributedCache` semantics beyond what the interface allows; HybridCache and IDistributedCache are implemented where compatible (Q10).

## 2. Architecture

Each app node holds one `NatsCache` singleton that owns an L1 `MemoryCache`, one shared `NatsConnection`, a KV store handle per bucket, and a background invalidation subscriber. All cross-node coordination goes through NATS; nodes never talk to each other directly.

```
+---------------------------------------+        +------------------------------------+
| App node x 5                          |        | NATS cluster, 3 nodes, R3          |
|                                       |get/put |                                    |
|  NatsCache facade  <-----------------------------> [prefix]_cache KV                |
|  (GetOrCreate, TryGet, Set, Remove)   |        |   L2 values, per-key TTL           |
|                                       |        |                                    |
|  L1 MemoryCache                       | lease  |  [prefix]_locks KV                 |
|  (jittered TTL, revision per entry)   |   +------>  leases via create-if-absent     |
|                                       |   |    |                                    |
|  Single-flight + distributed lock ------+    |  [prefix]_notifications stream      |
|                                       |events  |   set, del, fail, clear, tag       |
|  Invalidation consumer  <---------------------------                                 |
|                                       |        |  [prefix]_objects Object Store     |
+-------------------+-------------------+        |   values > LargeValueThreshold     |
                    | factory, on lock win       +------------------------------------+
                    v
+---------------------------------------+
| Origin service (test rig only)        |
| slow backend, versioned values, ledger|
+---------------------------------------+
```

The facade also publishes to the notifications stream on every write; the Origin exists only in the test rig to count factory calls.

### Components

| Component | Responsibility |
| --- | --- |
| `NatsCache` (public facade) | `GetOrCreateAsync`, `TryGetAsync`, `SetAsync`, `UpdateAsync`, `RemoveAsync`, `RemoveByTagAsync`; orchestrates L1 → L2 → lock → factory |
| `L1Store` | Wraps `Microsoft.Extensions.Caching.Memory`; size limit, jittered TTL, per-entry revision |
| `L2Store` | JetStream KV `{prefix}_cache`; get/put/delete with revision, per-message TTL |
| `LocalSingleFlight` | In-process dedupe (one `Task` per key per node) before any network lock |
| `DistributedLock` | KV `{prefix}_locks`; create-if-absent lease with TTL through the publish helper (never NATS.Net `CreateAsync`, section 5), owner id, release by revision |
| `InvalidationBus` | Publishes and consumes eviction events on `{prefix}_notifications` |
| `ConnectionSupervisor` | Connection state, reconnect, resync of L1 after outages, health checks |
| `ICacheSerializer` | Pluggable; default System.Text.Json with source generators, optional MessagePack |
| Telemetry | `System.Diagnostics.Metrics` + `ActivitySource`, OpenTelemetry-ready. Decided: meter and source named NatsDistributedCache, no OTel dependency in the library; TestApi wires OpenTelemetry to a Prometheus exporter and OTLP traces |

### Solution layout

```
src/
  NatsDistributedCache/              # the library (netstandard2.1 + net10.0, Q17)
  NatsDistributedCache.Extensions/   # DI + options binding (own package, Q11)
tests/
  NatsDistributedCache.UnitTests/    # xUnit v3, fakes for KV
  NatsDistributedCache.IntegrationTests/ # Testcontainers NATS cluster
  TestApi/                           # minimal API host used by k6 + validator
  Validator/                         # console app asserting invariants after runs
load/
  k6/                                # scenarios, thresholds
deploy/
  docker-compose.yml                 # 3x NATS, 5x TestApi, Envoy LB, Prometheus, Grafana
  nats/                              # cluster configs
  grafana/provisioning/datasources/  # prometheus.yml
  grafana/provisioning/dashboards/   # dashboards.yml provider
  grafana/dashboards/nats-cache.json # imported on compose up
docs/
```

NuGet dependencies: `NATS.Net` (pinned 3.3.0, the version the spike ran; includes `NATS.Client.KeyValueStore` and JetStream), `Microsoft.Extensions.Caching.Memory`, `Microsoft.Extensions.Options`, `Microsoft.Extensions.Hosting.Abstractions`.

## 3. NATS topology and buckets

The library requires the latest stable NATS Server release, currently 2.15.0 (decided in v1.2: we support only the latest stable minor line and upgrade the pin when a new stable release ships, after the spike suite passes on it; release candidates are never pinned. The full spike suite passes on 2.15.0. History that motivates the policy: 2.11.2 does not resume per-key TTL timers after a full restart, nats-server #7344, and 2.12.x no longer receives fixes. TTL'd buckets are never purged, rolled up, truncated or compacted: TTL bug #8594, timer entries leaking on purge/rollup/truncate, is fixed by PR #8595 in 2.15.1, which is still a release candidate; 2.15.0 does remove the timer entry on a normal delete or History-1 overwrite, so Set, renewals and DEL are safe. CI checks the server version through `/varz` against the pin and refuses release-candidate images), because per-key TTL in KV (`AllowMsgTTL`) is what makes jittered L2 expiry and self-expiring locks possible without a sweeper. All stores are replicated R3 across the 3-node cluster, so any single NATS node can die without data loss or unavailability.

Storage rule (decided): every NATS store (`{prefix}_cache`, `{prefix}_locks`, `{prefix}_notifications`, `{prefix}_objects`) uses file storage; memory storage is never used. The library creates them with `StorageType.File` and treats an existing memory store as a provisioning failure (NATS can't change storage type in place): it logs an error and the node reports Unhealthy (Closed) or Degraded (Open); the process never crashes. Each NATS node keeps its JetStream `store_dir` on a persistent volume, so all data survives a full cluster restart. The server config sets `jetstream.sync_interval` explicitly to the 2 min default, never `always` (that would fsync every write). Volumes are named Docker volumes, not tmpfs or overlay bind mounts; `docker compose down -v` wipes them. The `_locks` MaxAge backstop stays and is tested in C4.

| Store | Name | JetStream type | Storage | Replicas | Retention and TTL | Purpose |
| --- | --- | --- | --- | --- | --- | --- |
| Cache | `{prefix}_cache` | KV bucket | File (Q3) | 3 | History 1 (hard invariant), per-key TTL, bucket `MaxAge` = hard ceiling, `MaxBytes` cap, `LimitMarkerTTL` required (subject delete markers) | L2 values + metadata headers |
| Locks | `{prefix}_locks` | KV bucket | File (Q3) | 3 | History 1 (hard invariant), per-key TTL = lease time, `LimitMarkerTTL` required, `MaxAge` = 2 × max lease as backstop | Single-flight leases |
| Notifications | `{prefix}_notifications` | Stream, subjects `{prefix}.notify.>` | File (survives full restart) | 3 | `MaxAge` 5 min, `MaxMsgs` cap, discard old | Invalidation fan-out with short replay |
| Objects | `{prefix}_objects` | Object Store | File | 3 | `MaxAge` = cache ceiling | Values above `LargeValueThresholdBytes` |

History = 1 is a hard invariant for `_cache` and `_locks` (after review 4): the server raises any per-message TTL below `LimitMarkerTtl` up to it unless the bucket keeps one message per subject, so with History > 1 a 12 s lease would silently become 30 s. The library refuses (provisioning failure) a bucket with History > 1.

### Writes with TTL and fencing (after review)

NATS.Net 3.3.0 accepts a TTL only on `CreateAsync` and `PurgeAsync` (purge with a TTL is banned, even in tests, section 3); `PutAsync` and `UpdateAsync` have no TTL, and an `UpdateAsync` replaces a TTL'd entry with one that never expires. So every write that needs a TTL (Set, the fenced factory write, lock acquire and lease renewal) goes through one helper that publishes directly to `$KV.{bucket}.{key}` via `INatsJSContext.PublishAsync` with `Nats-TTL`, `Nats-Expected-Last-Subject-Sequence` (when a revision is expected) and a `Nats-Msg-Id`. Every helper write returns one of three outcomes (revised in v1.2 after review 6, spike-verified including leader restarts):

- **Committed(seq)**: a normal ack, or a duplicate ack (`ack.Duplicate`, or `NatsJSDuplicateMessageException` from `EnsureSuccess`), which means an earlier send of the same write committed at that sequence.
- **Rejected(code)**: 10071 or any other API error. An expected-sequence error is produced before the write is stored, or at apply time without storing, so it never coexists with a commit. Never retried blindly.
- **Unknown**: a reply timeout, no reply or reconnect (`NatsJSPublishNoResponseException`, `NatsNoReplyException`, `NatsTimeoutException`, `NatsJSTimeoutException`, `NatsJSApiNoResponseException`). This is the only way a write can commit while the client sees an error. The helper resends the identical publish (same headers, same msg id) up to 5 times with 250 ms steps: the server answers with a duplicate ack at the original sequence, or 10158 ("duplicate message id is in process") while it is still being applied. If it is still unknown, the caller settles it with a leader read and never treats it as a conflict.

Msg-id rules: one id per logical write. Reuse it only for a 10164/10158 retry or an unknown-outcome resend; any new expected revision gets a new id. An id whose apply failed stays registered with seq 0 for the duplicate window, so reusing it would return 10158 instead of the real result. KV duplicate window = min(2 min, bucket MaxAge); at the measured 1 700–2 000 lock ops/s that is about 240 000 dedup entries on the stream leader (see risks).

Error 10164 ("wrong last sequence" without a sequence) and 10158 are transient: 10164 is returned before proposal while another write to the same subject is still in flight (`checkMsgHeadersPreClusteredProposal` in nats-server 2.14/2.15), so nothing was stored. The helper retries the identical publish up to 5 more times with 4–64 ms backoff (6 sends, about 124 ms at most); if it is still refused, the outcome is Unknown and the caller does a leader read. Leader reads can themselves fail with `NatsJSApiNoResponseException` during a leader election (seen twice in the spike's failover test) and are retried the same way. Both KV buckets must be created with `LimitMarkerTTL` ≥ 1 s (NATS.Net rejects less) (server `allow_msg_ttl` + `subject_delete_marker_ttl`), otherwise NATS.Net refuses TTL operations. KV buckets always discard new, so a bucket at `MaxBytes` rejects writes; the library then logs, counts `cache.l2_full` and serves the value uncached.

### Internal keys, leader reads and deletes (after review 3)

- Internal key = `{userKey}._s{n}` (schema version, see the entry format). The internal key is used for the KV entry, the lock key, the event subject and `key` field, and the high-water revision. Only `tag` and `clear` events are prefix-based, on the user-key prefix, so they cover every schema version.
- Leader-read parsing: `INatsJSStream.GetAsync` returns a raw `StreamMsgGetResponse` (base64 `Hdrs`/`Data`, `Time`), not a KV entry. The library decodes it itself with `INatsConnection.HeaderParser`, takes `Created` from `Message.Time`, maps `KV-Operation` / `Nats-Marker-Reason` the way NATS.Net does (MaxAge, Purge → Purge; Remove → Del), and treats `NatsJSApiException` 10037 (no message found) as absent.
- Normal reads use Direct Get (fast, any replica). The fence re-read and the high-water re-read use a leader read instead, because Direct Get is not read-after-write coherent: `INatsJSStream.GetAsync(new StreamMsgGetRequest { LastBySubj = "$KV.{bucket}.{key}" })` on the KV stream, or a fetch by the sequence named in error 10071 (`wrong last sequence: {seq}`).
- Deletes also go through the publish helper, as a DEL marker (`KV-Operation: DEL`, empty payload). NATS.Net's `DeleteAsync` returns no revision; the helper's PubAck sequence is the tombstone revision that goes into the `del` event.

### Large values (decided)

Values above `LargeValueThresholdBytes` (after compression, default 512 KB) go to a NATS Object Store bucket `{prefix}_objects`, which chunks them, so NATS messages stay small. The KV entry in `{prefix}_cache` then holds only a pointer (object name `{internalKey}.{guid}` + digest) with the usual TTL and revision; invalidation and single-flight work unchanged on the pointer. Cleanup (decided): the cache bucket enables subject delete markers, so a TTL expiry or delete emits an event and one elected node (holding a sweeper lease in `_locks`) sees it through a meta-only watch on `_cache` (`WatchAsync(">")` with `MetaOnly` and `IgnoreDeletes = false`) and deletes the object. An hourly sweeper deletes objects with no live pointer that are older than the maximum L2 TTL + grace. Readers treat a pointer whose object is missing as a miss. A fenced writer deletes its own object. Object Store has only a bucket `MaxAge`, no per-object TTL, so a chunk can expire mid-read; a missing chunk or digest mismatch is also a miss.

### Naming rules

- `prefix` is required, validated against `^[A-Za-z0-9-]{1,32}$`, e.g. `orders` gives `orders_cache`, `orders_locks`, `orders_notifications`, `orders_objects`.
- Several apps can share one NATS cluster by using different prefixes; nothing is shared between prefixes.
- Buckets and streams are created by the library on startup if missing (`CreateOrUpdate`) (Q4).

### Key encoding

NATS KV keys only allow `A-Z a-z 0-9 - _ = . /`. Decided: keys are never hashed or rewritten. A key with other characters, an empty dot-segment, or more than 256 characters throws `ArgumentException`, so callers build clean hierarchical keys such as `orders.42.lines`. Dots are KV token separators, which is what makes prefix tags (Q9) work. Exception (after review): the `IDistributedCache` and `HybridCache` adapters accept arbitrary framework keys (e.g. OutputCache), so all adapter keys live under a `d.` namespace, never shared with `INatsCache` keys: valid keys as `d.<key>`, invalid ones as `d.h.<base32 SHA-256>`. Tags are not available for hashed keys. HybridCache tags must be prefix-shaped: tag `t` maps to `d.{t}.>`, and GetOrCreate/Set with a tag that isn't a dot-prefix of the key throws `ArgumentException` at write time. `IDistributedCache` sliding expiration (`Refresh`) is implemented as a re-put with a new TTL.

### Stored entry format (L2)

| Part | Content |
| --- | --- |
| Payload | Serialized value bytes (Brotli above `CompressionThresholdBytes`, default 4 KB, flagged in a header, Q12) |
| `Nats-TTL` header | Jittered L2 TTL in seconds (server enforces expiry) |
| `x-cache-created` | Unix ms when the factory produced the value |
| `x-cache-expires` | Informational only (writer clock). Logical expiry = server `entry.Created` + `x-cache-ttl`, see section 6 |
| `x-cache-ttl` | T_L2 in ms (after review 2) |
| `x-cache-node` | Writer node id |
| `x-cache-schema` | Value type version. Decided: the version is also the last segment of the internal KV key (`orders.42` → `orders.42._s2`), so old and new nodes don't overwrite each other during a rolling deploy and v1 entries just expire. The version comes from the `SchemaVersion` option (global, overridable per type via `ForType<T>(v)`), and `KnownSchemaVersions` lists the versions still in use (current + previous, default current only). RemoveAsync deletes exactly those internal keys, with no key listing; RemoveByTag lists by prefix and so covers every version. User key segments starting with `_s` are rejected |

## 4. Public API and data flows

The API is a small async interface built around `GetOrCreateAsync`, which is where single-flight and layering live; `Set` and `Remove` exist for explicit writes and always trigger cluster-wide eviction.

```csharp
public interface INatsCache
{
    ValueTask<T> GetOrCreateAsync<T>(string key,
        Func<FactoryContext, CancellationToken, ValueTask<T>> factory, // FactoryContext: Reason, LockToken, Attempt
        CacheEntryOptions? options = null, CancellationToken ct = default);

    ValueTask<CacheResult<T>> TryGetAsync<T>(string key, CancellationToken ct = default);
    ValueTask SetAsync<T>(string key, T value, CacheEntryOptions? options = null, CancellationToken ct = default);
    ValueTask<bool> UpdateAsync<T>(string key, ulong expectedRevision, T value, CacheEntryOptions? options = null, CancellationToken ct = default); // CAS, Q8
    ValueTask RemoveAsync(string key, CancellationToken ct = default);
    ValueTask RemoveByTagAsync(string prefix, CancellationToken ct = default);   // prefix tags, Q9
}

public enum FailureMode { Open, Closed }
public enum FactoryReason { Miss, EarlyRefresh, Takeover, Degraded }
public sealed record FactoryContext(string Key, FactoryReason Reason, string? LockToken, int Attempt);
public readonly record struct CacheResult<T>(bool Found, T? Value, ulong Revision);

public sealed record CacheEntryOptions
{
    public TimeSpan L2Ttl { get; init; } = TimeSpan.FromMinutes(10);
    public TimeSpan L1Ttl { get; init; } = TimeSpan.FromSeconds(30); // worst-case staleness ~33 s if events are lost
    public double JitterRatio { get; init; } = 0.1;      // ±10 %
    public TimeSpan FactoryTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan LockWaitTimeout { get; init; } = TimeSpan.FromSeconds(15);
    public TimeSpan? LeaseTtl { get; init; }                 // default min(FactoryTimeout + 2 s, MaxLeaseTtl); an explicit value above MaxLeaseTtl throws
    public DateTimeOffset? AbsoluteExpiration { get; init; }  // hard expiry, e.g. JWT exp
    public TimeSpan SafetyMargin { get; init; } = TimeSpan.FromSeconds(60);
    public FailureMode? FailureMode { get; init; }            // per-call override of Open | Closed
    public double EarlyRefreshRatio { get; init; } = 0.1;     // 0 = off
    public TimeSpan? CacheNullFor { get; init; }              // negative caching opt-in (Q5), e.g. 5 s
}

services.AddNatsDistributedCache(o => {
    o.Url = "nats://nats-1:4222,nats://nats-2:4222,nats://nats-3:4222";
    o.Prefix = "orders";
    o.NodeId = Environment.MachineName;
    o.CompressionThresholdBytes = 4096; // Brotli above this size, 0 = off
    o.L1SizeLimitBytes = 64 * 1024 * 1024; // by serialized size; values above LargeValueThresholdBytes skip L1
    o.LargeValueThresholdBytes = 512 * 1024; // above this, Object Store
    o.LimitMarkerTtl = TimeSpan.FromSeconds(30); // delete-marker TTL on both KV buckets, >= 1 s
    o.MaxLeaseTtl = TimeSpan.FromSeconds(60);    // per-call LeaseTtl must not exceed it; _locks MaxAge = 2 x this
    o.FailureMode = FailureMode.Open;            // default, overridable per call
    o.SchemaVersion = 1;                         // internal key suffix _s{n}; per type: o.ForType<Order>(2)
    o.KnownSchemaVersions = [1];                 // versions RemoveAsync deletes; add the previous one during a rolling deploy
});
```

### Read path: `GetOrCreateAsync`

1. L1 lookup. Hit and not expired → return.
2. Local single-flight: if another caller on this node is already loading the key, await its task.
3. L2 `GetEntry` (Direct Get) on the internal key. Hit and logical expiry in the future → put into L1 with TTL = min(L1Ttl, remaining L2 life), jittered, but only if the entry's revision ≥ the key's high-water revision (section 7); if it is lower, re-read once from the stream leader, and if still lower return the newest value seen without filling L1 → return.
4. Miss, logically expired (`entry.Created` + `x-cache-ttl` in the past, even if NATS has not removed it yet), or large-value object missing → remember the key's KV revision (or absent) and acquire the distributed lock (section 5).
    1. Won the lock → re-check L2 (double-checked; this revision is the fence's expected revision), run factory, fenced `Put` to L2, set L1, publish `set` notification, release lock.
    2. Lost the lock → wait for the internal key's set, del or fail event on the notifications stream (plus 250 ms polling fallback) until `LockWaitTimeout`.
5. Return the value to every waiter on this node.

### Write path: `SetAsync`

1. Serialize, `Put` to `{prefix}_cache` through the TTL publish helper (section 3) with `Nats-TTL` = jittered logical TTL + grace (section 6); obtain the new revision.
2. Update local L1 with that revision, compare-on-revision: never overwrite a newer revision already in L1.
3. Publish `{"op":"set","key":…,"rev":…,"node":…}` to `{prefix}.notify.{internalKey}`.
4. Other nodes evict their L1 copy if its revision is lower (section 7).

### Delete path: `RemoveAsync`

1. Publish a DEL marker through the helper (tombstone, not purge, so watchers see it) on the internal key of each version in `KnownSchemaVersions` in `{prefix}_cache` (no listing); the PubAck sequence is the tombstone revision.
2. Evict local L1 and raise the key's high-water revision to that revision.
3. Publish `{"op":"del","key":…,"rev":tombstoneRev,…}`; other nodes evict and raise their high-water revision, so a lagging Direct Get can't refill L1 with the deleted value.

### Tag delete path: `RemoveByTagAsync`

1. List keys under the prefix (`GetKeysAsync` with filter `{prefix}.>`, a headers-only consumer; cost grows with the number of keys under the prefix).
2. Delete each through the helper as above; keys written after the listing are not affected (they are newer than the tag delete).
3. Publish one `tag` event on `{prefix}.notify.{tagPrefix}` with `rev` = the highest tombstone sequence (or, if no keys matched, the stream's current last sequence, so the prefix HWM still blocks lagging reads). Each node evicts L1 keys under the prefix and holds a prefix high-water revision = `rev` for `L1Ttl × (1 + j)`: no entry under the prefix with a lower revision is filled into L1. Keys written between the listing and the last tombstone survive the tag delete (they are newer) but may be kept out of L1 for that window; this is expected.

Update is the same as Set in v1, plus a compare-and-swap `UpdateAsync(key, rev, value)` using KV revisions (Q8, decided: in v1).

## 5. Distributed single-flight factory per key

A key's factory runs on exactly one node at a time: the node that wins the atomic acquire (below) on `{prefix}_locks/{internalKey}` runs it, everyone else waits for the value to land in L2. Two layers keep lock traffic low: local single-flight collapses all callers on one node to one contender, so at most 5 nodes (not thousands of requests) race for a lock.

### Lock protocol

| Step | Operation | Notes |
| --- | --- | --- |
| Acquire | TTL publish helper: publish a unique token (fresh per acquire) with `Nats-TTL` = LeaseTtl, expecting last subject sequence 0. On 10071, leader-read the last message: a tombstone (`KV-Operation: DEL` or `PURGE`) or a TTL marker (`Nats-Marker-Reason`) means free, so publish again expecting exactly that sequence; a live token means held (lost). A 10071 on the second publish is interpreted by the sequence in its text (`wrong last sequence: {seq}`): seq 0 (the marker expired meanwhile) → publish again expecting 0; seq > S → repeat the leader-read step (e.g. a MaxAge marker replaced the tombstone); a live token → lost, unless it is this acquire's own token (compared as bytes): that means an earlier send committed although its outcome was Unknown (lost reply, timeout or reconnect), so the node owns the lock, with the lease starting at that message's server time. An Unknown outcome from the helper is settled the same way, by a leader read. A 10037 (no message) from the leader read → publish expecting 0. At most 3 attempts, then lost | Never NATS.Net `CreateAsync` (changed in v1.1): on a tombstoned key it re-reads through Direct Get, and the spike measured 4.4–5.3 % spurious failures on uncontended keys on 2.12.15, each of which would make an idle node wait for a factory nobody runs. The helper acquire had 0 failures in 30 000+ cycles and was slightly faster. Atomic across the cluster via the stream leader; a release racing the second publish (nats-server #5162) can only produce a lost result, never two owners |
| Token | `{nodeId}:{guid}:{unixMs}:{leaseMs}:{factoryTimeoutMs}` | Identifies the owner; carries the owner's LeaseTtl and FactoryTimeout so waiters cap their wait with the owner's values; logged for validation |
| Lease TTL | `FactoryTimeout + 2 s` | Server-side per-key TTL, so a crashed owner's lock disappears by itself. Decided: this is only the default; LeaseTtl, FactoryTimeout and LockWaitTimeout can be overridden globally and per call. A per-call LeaseTtl above `MaxLeaseTtl` (default 60 s) throws; `_locks` MaxAge = 2 × MaxLeaseTtl |
| Renew | TTL publish helper (key, token, expected revision, `Nats-TTL`) every `LeaseTtl / 3` | Done with the TTL publish helper (expected revision + `Nats-TTL`), never KV `UpdateAsync`, which would drop the TTL and leave a lock that never expires. Only for factories that run longer than one lease; stops if the revision check fails |
| Release | DEL marker through the publish helper, expecting the owner's latest revision (the last renewal's, not the acquire's); it completes before the `fail` or `set` event is published, so waiters that retry on the event never see a live token | Only the current owner can release; a stale owner's release gets 10071 and is a no-op (spike-verified) |
| Wake waiters | Value lands in L2 plus a `set` notification | Woken via the notifications stream, not a per-key KV watch, for `{prefix}_cache/{internalKey}` and also poll every 250 ms as a fallback |

### Waiter behaviour

1. Lose the acquire → register in the node's local waiter table (no new NATS consumer). The node's single notifications consumer wakes the waiter on the key's set, del or fail event; as a fallback it polls L2 and the lock key every 250 ms (decided).
2. Cache key gets a value → return it.
3. Lock key deleted or expired without a value (owner failed or crashed) → retry acquire, with a small random backoff (10–50 ms) so waiters do not all hit the server at once.
4. `LockWaitTimeout` reached → if the lock revision changed since the last check (the owner is renewing), keep waiting, but at most the owner's FactoryTimeout + LeaseTtl (read from the lock value) past the first observed lease revision; otherwise apply FailureMode (Q1): Open runs the factory locally (its result reaches L2 only through the fenced write) and the run is logged as a degraded window, Closed throws.

### Failure cases

| Case | Behaviour |
| --- | --- |
| Factory throws | Release lock, publish `fail` notification so waiters retry immediately, do not cache (negative caching is Q5) |
| Owner node crashes mid-factory | Lock expires after `LeaseTtl`; a waiter takes over. Worst-case added latency = `LeaseTtl` |
| Factory outlives lease without renew (GC pause, network partition) | A second owner can start: a double execution. Detected by the validator via factory counters and lock tokens; mitigated by renewals and by the fenced write (below), which rejects the stale owner's write when a newer value exists |
| NATS unreachable | No lock possible; FailureMode applies (Q1): Open runs the factory locally with local single-flight only, Closed throws CacheUnavailableException |

The guarantee we can honestly promise is "at most one factory execution per key per lease, as long as the owner renews in time". Truly exactly-once would need a consensus-backed fencing token checked by the data source, which is out of scope.

### Write fencing against resurrection (decided)

Revised after review. The lock owner writes through the TTL publish helper with `Nats-Expected-Last-Subject-Sequence` = the revision from the post-lock double-check (section 4, step 4.1) (0 only when the subject has no message at all; if the miss saw a delete or TTL marker, that marker's revision from `NatsKVKeyDeletedException.Revision`). On a wrong-last-sequence error it re-reads the key from the stream leader, or fetches the sequence named in error 10071 (section 3):

- Absent, or a MaxAge / Purge TTL marker: the old entry just expired. Not a conflict: retry with the new revision, at most 3 attempts in total.
- A PUT with a higher revision that is already logically expired (a lagging double-check read, or an entry kept by the grace period): not a conflict either; retry with that revision, counting toward the 3 attempts. It is never handed to waiters. After 3 attempts: return the factory result uncached, increment `cache.fenced_writes`, outcome = fenced-exhausted.
- A DEL (explicit Remove or RemoveByTag): fenced. Return the factory result to the waiting callers uncached, since the delete happened after they asked. Outcome = fenced-del.
- A live newer value: fenced. Do not cache; return that newer value to the waiting callers, subject to the high-water rule (section 7), so no node ever serves an older version after a newer one (I5). Increment `cache.fenced_writes`; outcome = fenced-newer.

Because `Nats-TTL` = logical TTL + grace (section 6), a reader that sees a logically expired entry still finds it on the server, so ordinary misses are not fenced. A fenced write never triggers another factory run: callers get the newer value or the uncached result, so I1 and I2 are unaffected.

## 6. Expiration and jitter

Every TTL the library sets is multiplied by a random factor in [1 − j, 1 + j] (default j = 0.1), so keys written together do not expire together and hit the factory in one burst. L1 is always capped by the remaining L2 life, so no node serves a value longer than the cluster copy exists.

```
T_L2    = L2Ttl * (1 + U(-j, j))
expires = created + T_L2
T_L1    = min(L1Ttl * (1 + U(-j, j)), expires - now)
```

Hard expiry (decided): when `AbsoluteExpiration` is set, jitter only shortens the TTL (factor in [1 − j, 1]), and both TTLs are capped at `AbsoluteExpiration − SafetyMargin − now`. A JWT with exp in 15 min and a 60 s margin therefore lives 12.6 to 14 min in cache and is never served expired.

| Layer | Enforced by | Jitter applied | Notes |
| --- | --- | --- | --- |
| L2 | NATS per-key TTL (`Nats-TTL` header) + logical expiry (`entry.Created` + `x-cache-ttl`) | Once, by the writer | Readers use the server timestamp, so all nodes agree on one expiry |
| L1 | `MemoryCache` absolute expiration | Per node, on each L1 fill | Different nodes refresh L1 at different moments from L2, which spreads L2 reads |
| Lock | NATS per-key TTL | None | Lease must be predictable |
| Bucket ceiling | `MaxAge` on `{prefix}_cache` | None | Safety net, e.g. 24 h, above any configured L2 TTL |

Server TTL grace (after review): the server-side `Nats-TTL` is `T_L2 + grace`, with grace = LeaseTtl + FactoryTimeout (22 s by default). Logical expiry still decides what is served, so readers never serve past `expires`; the grace only keeps the old entry on the server long enough for the fenced refresh write to find it. Logical expiry is computed from the entry's server timestamp (`entry.Created`) plus T_L2 stored in the `x-cache-ttl` header, not from the writer's clock; `entry.Created − x-cache-created` is logged as measured clock skew.

### Stampede protection beyond jitter (Q6)

- Early refresh: when a read finds a value in its last 10 % of L2 life, one node (via the same lock) refreshes it in the background while everyone keeps getting the current value. Decided: `EarlyRefreshRatio` = 0.1 by default, 0 turns it off per call; the refresh uses the same lock and write fencing. Refreshes are locally single-flighted, run with Reason = EarlyRefresh, and losing the lock is a no-op: the node keeps serving the current value and does not wait.
- Stale-while-revalidate: v2.

Clock skew between nodes shifts logical expiry by the skew. We assume NTP-synced containers (skew < 100 ms); the validator measures it.

## 7. Cross-node invalidation

After any Set, Update or Delete, every other node drops its L1 copy within the invalidation SLO (proposed: p99 < 100 ms, max < 1 s under nominal load). Events carry the L2 revision, so a node only evicts copies older than the write, and a late or duplicated event can never remove a newer value.

### Event format

Subject `{prefix}.notify.{internalKey}` on stream `{prefix}_notifications` (`key` = internal key; for `tag` it is the user-key prefix):

```json
{ "op": "set" | "del" | "fail" | "clear" | "tag", "key": "orders.42", "rev": 1874, "node": "api-3", "ts": 1790000000123 }
```

### Consumer rules

1. Each node runs one ordered consumer (pull-based in NATS.Net, recreated by the client on gaps) on `{prefix}.notify.>`, starting at "new" on first boot.
2. Events from the node itself are processed like any other. Every L1 write compares revisions, so own events are harmless, and this fixes the race where a newer remote event arrives before the node's own L1 write.
3. `set`: evict L1 if the local entry's revision < `rev`. We evict rather than fetch, so the next read pulls from L2 (lazy, Q7 decided).
4. `del`: evict unconditionally and raise the key's high-water revision to `rev`.
5. `tag`: evict every L1 key under the prefix and hold a prefix high-water revision = `rev` (section 4, tag delete path).
6. `clear`: flush the whole L1 (admin operation, used after restores); published on `{prefix}.notify._clear` with `key` = `*`.
7. The node tracks the last processed stream sequence. After a reconnect, and whenever the delivered stream sequence (`Metadata.Sequence.Stream`) jumps by more than 1, it reads `StreamInfo` first. NATS.Net has no callback when it recreates the ordered consumer, but the stream carries only notify subjects and the filter covers them all, so any gap means lost events. If `Created` changed, `FirstSeq > lastSeen + 1`, or `LastSeq < lastSeen`, events were lost (stream recreated, `MaxMsgs` or `MaxAge` discard), so it flushes L1 and restarts from new; otherwise it resumes from `lastSeen + 1`.

### Per-key high-water revision (after review 2)

Compare-on-revision alone misses one race: a node reads rev 10 from L2, the rev 11 event arrives while its L1 is empty (nothing to evict), and the node then fills L1 with rev 10. KV reads also use Direct Get, which any replica may answer without read-after-write coherency, so a read after the rev 11 event can still return rev 10. Each node therefore keeps a high-water revision (HWM) per internal key: the highest revision it has served or seen in a `set` or `del` event. L1 is filled only with entries at or above the HWM; a lower L2 read is re-read once from the stream leader, and if still lower the newest value seen is returned without filling L1. A new Set after a delete gets a higher revision than the tombstone, so the HWM never blocks it. HWM entries live for `L1Ttl × (1 + j)` and count toward the L1 size limit. Expect leader re-reads to be common under load: the spike measured 0.05 % stale Direct Gets on an idle cluster but about 5 % under lock load (NATS.Net `CreateAsync` failures are that same stale read).

### Why a stream and not core pub/sub or a KV watch

| Option | Pros | Cons |
| --- | --- | --- |
| JetStream stream (chosen) | Replay after short disconnects; ordered; matches the `_notifications` naming requirement | A little more latency and server load than core NATS |
| Core NATS pub/sub | Lowest latency | Messages lost during disconnects, so a node can serve stale data until L1 TTL |
| Watch on `{prefix}_cache` itself | No separate publish; can never miss a write that reached L2 | Every node receives every value payload (use headers-only watch to avoid it); no place for `fail` / `clear` events |

The L1 TTL stays the final safety net: even if every event were lost, staleness is bounded by `L1Ttl × (1 + j)`.

## 8. Resilience and auto recovery

The library never needs an app restart to recover: the NATS client reconnects forever with jittered backoff, KV calls retry through a resilience pipeline, and the cache drops to an L1-only degraded mode while NATS is gone (Open mode), then resyncs when it returns.

### Connection settings (NATS.Net 3.3.0)

| Setting | Value | Why |
| --- | --- | --- |
| Seed URLs | All 3 servers | Client also learns cluster topology from gossip |
| `MaxReconnectRetry` | -1 (forever) | App must outlive any outage |
| `ReconnectWaitMin / Max` | 100 ms / 2 s, jittered | Avoid 5 nodes reconnecting in lockstep |
| `ConnectTimeout` / `RequestTimeout` | 2 s / 2 s, set explicitly (NATS.Net's default request timeout is 5 s) | Fail fast into retry; the helper's unknown-outcome path depends on this value |
| `PingInterval` | 10 s, 2 missed pings → reconnect | Detect half-open TCP |
| Startup | Do not block app start; retry in background | Pod can start while NATS is down |

### Operation retries

KV calls go through a `Microsoft.Extensions.Resilience` pipeline: timeout 2 s, retry 3 times with exponential backoff and jitter on transient errors (no responders / 503, timeout, leader election in progress), then a circuit breaker that opens after 50 % failures over 10 s and probes every 5 s.

### Degraded mode

| State | Reads | Writes | Single-flight |
| --- | --- | --- | --- |
| Healthy | L1 → L2 → factory | L2 + notify | Distributed |
| NATS down (breaker open) | Open: L1 → factory; Closed: throw (Q1) | Open: L1 only, written keys journaled and replayed as revision-fenced del on recovery; Closed: throw (Q1, Q2) | Open: local only; Closed: none (throws) |
| Recovering | L1 cleared if the StreamInfo check (section 7, rule 7) shows lost events, then normal | Normal | Distributed |

### Recovery steps on reconnect

1. Re-verify buckets and stream exist (recreate if the cluster was wiped).
2. Resume the notification consumer from the last sequence, or flush L1 if the gap is too old. A node that wrote L1-only during the outage (Open mode) always flushes L1: outage writes are discarded from L1, and every key written during the outage is re-read from the stream leader on recovery; if its `entry.Created` is earlier than the node's first outage write for that key minus a 1 s skew margin (server vs local clock, NTP assumed), it is deleted at that revision through the helper plus a del event, otherwise skipped because someone wrote after the outage began, so no other node keeps serving the pre-outage L2 value (decided after review).
3. Close the breaker after a successful probe.
4. Emit a `cache.recovered` metric and log event with outage duration.

Health checks: `/health/live` ignores NATS; `/health/ready` reports `Degraded` (not `Unhealthy`) while NATS is down in Open mode, so the load balancer keeps routing to the node; in Closed mode it reports Unhealthy (Q1, Q2). Envoy's health check runs every 1 s with healthy_threshold 1. TestApi takes the run's FailureMode from the `Cache__FailureMode` env var, with an optional `X-Cache-FailureMode` request header for per-call tests.

## 9. Test environment

The test rig is one docker compose file: a 3-node NATS JetStream cluster, 5 TestApi replicas behind Envoy, a separate Origin service that plays the slow backend and counts every factory call, plus Prometheus and Grafana. k6 and the Validator run outside the resource limits so they never become the bottleneck. (Kubernetes with a StatefulSet for NATS is the alternative, Q13.)

Scaled rig (decided): the dev machine has 12 logical CPUs (6 cores with SMT, corrected in v1.1), and every limit stays halved: NATS 0.5 CPU each, API 0.25 CPU each, 2.75 CPU in total. That leaves about 3 CPU for Envoy, Origin, Prometheus, Grafana and k6. API and NATS containers set `cpu_period` to 10 ms (quota 2.5 ms for 0.25 CPU), so a throttled container stalls at most 7.5 ms instead of 75 ms and, after review, NATS and API containers are pinned with cpuset to logical CPUs 0–5 (cores 0–2 with their SMT siblings) while Envoy, Origin, observability and k6 run on logical CPUs 6–11, so neighbours can't steal CPU. A request needing more than 2.5 ms CPU spans two periods, so latency gates are set from the first K1 run, with the warm-up stage excluded. Cores 0–2 carry 2.75 CPU of quota with little headroom for Raft and disk I/O, so expect some throttling noise in latency results. RPS targets are halved to 2 500; the correctness invariants are unchanged. The full-size profile (1 / 0.5 CPU) stays as a compose override; with 12 logical CPUs it may fit on this machine, but it must be measured before its numbers are trusted. Spike baseline (0.5 CPU per NATS node, 16 lock workers, one concurrency level): about 2 000 lock ops/s, cycle p50 12 ms and p99 40 ms, with the stream leader at its CPU quota. This is a single closed-loop point; a sweep of 8/16/32/64 workers reported as helper cycles/s (with CPU sampled during the helper run) replaces it before milestone 6, and the K3 budget is provisional until then.

### Containers and limits

| Service | Replicas | CPU | Memory | Image / runtime |
| --- | --- | --- | --- | --- |
| `nats-1..3` | 3 | 0.5 each (scaled; 1.0 full size) | 1 GB each (Q14) | `nats:2.15.0-alpine` (latest stable), JetStream on with `store_dir` on a named volume per node (`nats-1-data` … `nats-3-data`), cluster routes, monitoring on 8222 |
| `api-1..5` | 5 | 0.25 each (scaled; 0.5 full size) | 256 MB each (Q14) | `mcr.microsoft.com/dotnet/aspnet:10.0`, TestApi |
| `lb` | 1 | unlimited | — | Envoy (envoyproxy/envoy), round-robin (or random) to the 5 APIs |
| `origin` | 1 | unlimited | — | .NET 10 minimal API: fake backend with delay, per-key version, call ledger |
| `prometheus`, `grafana`, `nats-exporter` | 1 each | unlimited | — | Observability; Grafana provisions the Prometheus datasource and the NatsCache dashboard from deploy/grafana/ on docker compose up (panels: RPS and p95/p99 per node, L1/L2 hit ratio, factory calls/s, lock wins/waits, invalidation lag, NATS JetStream and KV stats) |
| `k6`, `validator` | on demand | unlimited | — | Run from host or compose profile |

.NET at low CPU: `Environment.ProcessorCount` reports 1, so we run workstation or DATAS GC, set a heap hard limit at 75 % of container memory, and raise the thread pool minimum to avoid starvation during bursts. These settings are part of the test, since production will look similar.

### TestApi endpoints

| Method + route | Calls | Used by |
| --- | --- | --- |
| `GET /items/{id}` | `GetOrCreateAsync`; factory = HTTP call to Origin (default 200 ms delay) | k6 read load |
| `PUT /items/{id}` | Origin bumps version, then `SetAsync` with the new value | k6 writes, invalidation tests |
| `DELETE /items/{id}` | `RemoveAsync` | Delete tests |
| `GET /tokens/{jwtId}` | `GetOrCreateAsync`; factory = the TestApi itself generates and signs an ES512 JWT (ECDSA P-521 + SHA-512, key shared by all 5 nodes, about 600 bytes, exp 15 min) and counts each sign in /debug/stats; cache TTL = exp − 60 s so a cached token is never served expired | k6 K8 JWT cache |
| `GET /debug/l1/{id}` | Returns local L1 value + revision, or 404 | Validator: per-node view |
| `GET /debug/stats` | Hits, misses, factory calls, lock wins/waits, invalidations received | Validator + Grafana |
| `GET /health/live`, `/health/ready` | Health checks | Envoy, compose |
| `GET /metrics` | Prometheus scrape | Prometheus |

Every value the Origin returns carries `{id, version, producedBy, producedAt}`. That makes staleness checkable: a node that returns version 3 after the Origin already moved to version 4 plus the SLO window is a failure.

### Origin call ledger

Origin records each factory call as `(key, version, callerNode, lockToken, reason, startedAt, endedAt)`, reason ∈ miss | early-refresh | takeover | degraded. The library passes a `FactoryContext` (Key, Reason, LockToken, Attempt) to every factory call; TestApi forwards it to Origin as request headers, and after the L2 write reports the outcome (cached | fenced-newer | fenced-del | fenced-exhausted | uncached-l2full), keyed by lockToken. Because it sits outside the system under test, it is the ground truth for single-flight: two overlapping calls for the same key and version mean a violation.

## 10. Load testing with k6

k6 drives eight scenarios, each with thresholds that fail the run, and tags every request with scenario and target node so results can be split per node. Most scenarios go through Envoy; the invalidation scenarios hit each API node directly so we can see what each node's L1 serves.

| # | Scenario | Executor and shape | What it proves | Thresholds (Q15) |
| --- | --- | --- | --- | --- |
| K1 | Baseline mixed load | `ramping-arrival-rate` to max RPS, 95 % reads / 5 % writes, Zipf over 10 000 keys | Capacity of the API tier, hit ratios | errors < 0.1 %; latency gates set from the first run (measure then set), warm-up excluded |
| K2 | Thundering herd | 1 cold key, 1 000 VUs released at once across all nodes | R1 single-flight | Origin calls for that key = 1; no timeouts |
| K3 | Herd on many keys | 500 cold keys, about 2 000 VUs using `http.batch` (about 20 requests per key in flight at once) | R1 at scale, lock bucket load | Run in Closed mode: Origin calls with reason ≠ degraded = 500 exactly and degraded = 0; lock ops/s and NATS CPU recorded and gated. Budget: the spike measured a ceiling of about 2 000 lock ops/s (about 1 000 cold misses/s) at 0.5 CPU per NATS node; K3's 500 misses are about 1 000 lock ops, so they must complete in well under 1 s of lock time, and a K3 run that approaches the ceiling is reported as capacity-bound, not as a correctness failure |
| K4 | Synchronized write, jittered expiry | Write 5 000 keys in 1 s, TTL 60 s, then steady reads | R3 jitter | Origin calls spread over ≥ 80 % of the jitter window, peak/mean < 2 |
| K5 | Update invalidation | 1 writer per key at 10 writes/s; 5 readers pinned to api-1..5 polling every 10 ms | R4 eviction | Time until all 5 nodes serve the new version: p99 < 100 ms, max < 1 s |
| K6 | Delete invalidation | Delete random hot keys under read load | R5 | No node returns a deleted version after the SLO window |
| K7 | Soak | K1 at 70 % of max for 1 h | Leaks, drift | Memory flat ±10 %, no error increase over time |
| K8 | JWT cache | `constant-arrival-rate` at target RPS, three runs with 1, 100 and 10 000 JWT ids, each id picked uniformly at random | Cache value for an expensive, CPU-bound factory; hit ratio vs key-space size; single-flight on token minting | Signs per id per TTL window ≤ 1 (sum of /debug/stats counters over 5 nodes; I2 only, no I1 overlap check for K8); p95 < 20 ms in all three runs; no expired token served |

Chaos runs (section 11) replay K1 + K5 while faults are injected. Results export to Prometheus remote-write and a JSON summary that the Validator reads.

```
load/k6/
  lib/        # client helpers, Zipf generator, version checks
  k1-baseline.js  k2-herd.js  k3-herd-many.js
  k4-jitter.js    k5-update.js k6-delete.js  k7-soak.js  k8-jwt.js
  chaos/      # scripts that call docker to kill/pause/partition
```

## 11. Validation plan

The system counts as working only when an automated Validator checks eight invariants against ground truth (the Origin ledger, per-node debug endpoints, NATS state) and all pass, at nominal load and under each fault. Dashboards help diagnose; they never decide pass or fail.

### Invariants

| ID | Invariant | How it is checked | Pass criterion |
| --- | --- | --- | --- |
| I1 | Single-flight: no two overlapping factory calls for the same key and version | Origin ledger: group by key, look for overlapping `[startedAt, endedAt]` | 0 overlaps in healthy runs; under chaos every overlap must match a lease expiry or a logged degraded (Open-mode) window |
| I2 | One factory call per key per expiry window | Per key from the Origin ledger (Reason + Outcome). Exact for K2, K4, K5, K6, K8, whose key sets and timings are fixed; for K1 an upper bound per key of 1 + writes + ceil(D / (0.81 · L2Ttl)) for a run of length D. Fenced outcomes never add factory runs | Count ≤ expected (healthy) |
| I3 | Bounded staleness after write | K5: for each write at t0, the first moment all 5 nodes return the new version | p99 < 100 ms, max < 1 s; never > `L1Ttl × (1 + j)` even under chaos |
| I4 | Delete is final | After `DELETE` + SLO, `/debug/l1/{id}` returns 404 on all nodes and KV has a tombstone | 100 % of sampled keys |
| I5 | No revision regression | A node never serves a lower version than it served before for the same key | 0 regressions |
| I6 | Jitter spreads expiry | Histogram of logical expiry (`entry.Created` + `x-cache-ttl`) and Origin calls for K4 | Uniform-ish: Kolmogorov–Smirnov test vs uniform, p > 0.01 |
| I7 | Buckets named correctly | `nats kv ls`, `nats stream ls` via Validator | Exactly `{prefix}_cache`, `{prefix}_locks`, `{prefix}_notifications` and `{prefix}_objects` as bucket names (`nats stream ls` shows them as `KV_…` / `OBJ_…`), storage = file, all R3 |
| I8 | Recovery without restart | Chaos runs: error rate returns to baseline and I1–I5 hold after the fault clears | Recovery < 10 s after NATS quorum returns, measured on each node directly, not through Envoy; app process start time unchanged |

### Test layers

1. Unit tests (xUnit v3): jitter math, key validation, revision compare, local single-flight, state machine of degraded mode, with an in-memory KV fake.
2. Integration tests (Testcontainers, real 3-node NATS cluster): lock contention with 50 tasks across 5 `NatsCache` instances in one process, lease expiry takeover, notification replay after reconnect, bucket provisioning.
3. System tests: compose stack + k6 K1–K8 + Validator report.
4. Chaos tests: compose stack + fault scripts, K1 + K5 running.

### Chaos matrix

| # | Fault | Injection | Expected behaviour |
| --- | --- | --- | --- |
| C1 | Kill one NATS node (not the leader) | `docker kill nats-2`, restart after 60 s | No errors beyond retries; all invariants hold |
| C2 | Kill the JetStream meta and stream leaders | Find leaders via `/jsz`, kill them | Short write stall (< 5 s) during election; I1–I5 hold |
| C3 | Lose NATS quorum (2 of 3 down) | Kill 2 nodes for 30 s | Open: degraded mode, L1-only reads, local single-flight; Closed: full outage; recovery per I8 |
| C4 | Full NATS cluster restart | Stop all 3, start all 3 | All buckets and the stream survive with their data (file storage on persistent volumes); L1 flushed only if the StreamInfo check shows lost events or the node wrote during the outage; recovery per I8 |
| C5 | Kill an API node that holds a lock | `docker kill api-3` mid-factory (Origin delay 5 s) | Lock expires after `LeaseTtl`, another node takes over; one overlap allowed and logged |
| C6 | Pause an API node (simulated GC stall) | `docker pause api-2` for 20 s | Its lease expires; after unpause it must not overwrite a newer L2 revision (fencing) |
| C7 | Network partition of one API node | iptables or toxiproxy between api-4 and NATS | api-4 degrades; on heal it flushes L1 only if the StreamInfo check shows lost events or it wrote during the outage; I3 holds |
| C8 | Latency on NATS links | toxiproxy +50 ms | SLOs may widen; no correctness loss |

### Report

The Validator prints a table of invariants × scenarios with pass/fail and the offending keys, writes `validation-report.json`, and exits non-zero on any failure, so CI can gate on it.

## 12. Decisions, risks, milestones

### Decision log

| ID | Question | Decision |
| --- | --- | --- |
| Q1 | Lock unavailable or wait timed out: run the factory locally, or throw? | Configurable `FailureMode` = `Open` \| `Closed`. Open: run the factory locally, L1-only is acceptable. Closed: NATS is required, throw `CacheUnavailableException`. Per-call override. |
| Q2 | While NATS is down, do writes succeed L1-only, and is the node still "ready"? | By Q1 mode. Open: writes succeed L1-only, readiness = Degraded but routed. Closed: writes throw `CacheUnavailableException`, readiness = Unhealthy so Envoy drains the node; with NATS down on all nodes this is a full outage, accepted as intended. On recovery, Open-mode nodes flush L1 and replay the keys they wrote during the outage as leader-read, revision-fenced deletes (section 8). |
| Q3 | Storage for all NATS stores: file or memory? | File for all NATS stores (cache, locks, notifications, objects); memory storage is never used (survives C4; stale locks still expire by their TTL) |
| Q4 | Does the library create buckets, or does ops provision them? | The library creates buckets and streams |
| Q5 | Cache factory failures or null results (negative caching)? | By Q1 mode. Exceptions are never cached. Open: null cached for 5 s if opted in, L1-only when NATS is down. Closed: null cached for 5 s if opted in, only when it reaches NATS KV |
| Q6 | Early refresh and stale-while-revalidate in v1? | Early refresh yes (last 10 %, via the lock), stale-while-revalidate v2. While a node can't reach NATS it follows the Q1 mode, and its L1 staleness is bounded by L1 TTL (C7) |
| Q7 | After a remote Set, evict (lazy) or refetch (eager)? | Evict |
| Q8 | Need compare-and-swap `UpdateAsync` using KV revisions? | Yes, in v1 |
| Q9 | Tag or prefix invalidation? | v1 via key prefixes. A tag is a key prefix such as `orders.42`; RemoveByTagAsync deletes every KV key under `orders.42.>` and publishes one tag event, and each node evicts L1 keys with that prefix |
| Q10 | Implement .NET `HybridCache` / `IDistributedCache`? | Implement both wherever the interface is compatible (the spike confirms which members); `RemoveByTagAsync` works for prefix tags and otherwise throws `NotSupportedException`. Registering Microsoft's `AddHybridCache()` together with our `IDistributedCache` logs a startup warning (after review: detection is only reliable when Microsoft's registration comes first) |
| Q11 | One NuGet package or core + extensions? | Two packages: core (NatsDistributedCache) + extensions (NatsDistributedCache.Extensions: DI and options binding) |
| Q12 | Serializer and compression? | System.Text.Json source-gen (pluggable), Brotli above a configurable `CompressionThresholdBytes` (default 4 KB, 0 = off) |
| Q13 | Test rig on docker compose or Kubernetes? | Docker compose on the dev machine (12 logical CPUs), every limit halved (section 9); k8s manifests later |
| Q14 | Memory limits per NATS and API container? | NATS 1 GB, API 256 MB |
| Q15 | Target RPS, key count, value size, SLO numbers? | First target: 2 500 RPS on the scaled rig (5 000 at full size), 10 000 keys, 1 KB values, SLOs as in section 10; revisit after the K1 capacity run |
| Q16 | Security: TLS, auth? | Off in the test rig; TLS, user/password, NKey and creds (JWT) supported via options |
| Q17 | .NET target frameworks? | Libraries multi-target netstandard2.1 + net10.0; netstandard2.0 only if .NET Framework consumers appear. Test hosts stay net10.0 |
| — | Jitter with hard expiry | Negative-only jitter when `AbsoluteExpiration` is set, capped at exp − SafetyMargin |
| — | Waiter wake-up | Via the notifications stream + 250 ms polling, no per-key KV watch |
| — | Keys | Invalid keys rejected, never hashed; schema version as internal `_s{n}` key segment |
| — | L1 memory | 64 MB cap by serialized size; large values skip L1 |
| — | Large values | Object Store `{prefix}_objects` above 512 KB, KV pointer, delete markers + hourly sweeper |
| — | Resurrection race | Fenced (expected-revision) factory writes; fence rule revised after review (section 5) |
| — | Lease timing | Draft defaults, overridable globally and per call |
| — | Telemetry | Meter + ActivitySource in library, OTel in TestApi |
| — | NATS version | Latest stable release only, currently 2.15.0 (v1.2); pin moves forward after the spike suite passes on the new release |
| — | Lock acquire | Publish helper + leader read, never NATS.Net `CreateAsync` (v1.1, spike-verified) |
| — | JWT single-flight evidence | Counters only (I2), no overlap check |

### Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Each miss costs about 4 replicated writes (lock create, value put, notify, lock delete) | Low-CPU NATS nodes may cap miss throughput well below read throughput | Local single-flight first; measure in K3; optional lock-free mode for cheap factories |
| KV per-key TTL still has open server bugs (#8594 on purge/rollup) | Leaks or missed expiry | Latest stable only, purge/rollup/truncate banned, spike suite re-run before every pin move; fallback = logical expiry only + bucket `MaxAge` |
| Dedup state on the stream leader | About 240 000 msg-id entries at 2 000 lock ops/s (2 min window) | Measure leader memory in the milestone-6 worker sweep; shorten the duplicate window on `_locks` if needed |
| Low-CPU .NET nodes starve the thread pool under bursts | Timeouts that look like cache bugs | Fallback: fewer API workers (e.g. 3 replicas × 1 CPU instead of 5 × 0.5); GC and thread-pool tuning, measure with `dotnet-counters` |
| Lease expiry under GC pause gives double execution | Violates I1 | Renewal, fencing check, and honest guarantee wording |
| Clock skew between containers | Early or late logical expiry | Validator measures skew; use server timestamps where possible |

### Milestones

1. Done (Sep 30, 2026; `spike/RESULTS.md`). Spike on NATS 2.15.0 (current requirement), 2.14.7, 2.12.15 and 2.11.2: per-key TTL and delete markers on expiry; CAS against a marker revision; TTL timers resuming after a full restart on file storage; lock create/delete throughput on file-backed R3 at 0.5 CPU with the default `sync_interval`; Direct Get staleness under load; leader-read latency at 0.5 CPU; ordered consumer started below FirstSeq (silent skip?); meta-only watch delivering TTL markers; DEL through the publish helper; `Create` contention.
2. Core library: L1/L2, jitter, key validation, bucket provisioning, unit tests.
3. Distributed single-flight with leases, renewal, fencing; integration tests.
4. Invalidation stream, reconnect resume, degraded mode.
5. TestApi, Origin, compose rig with CPU limits, Prometheus/Grafana.
6. k6 K1–K8 and Validator with invariants I1–I8.
7. Chaos matrix C1–C8, tuning, v1.0 release notes.

## 13. Design review log

### Review 1: Fable, Sep 30 2026 (all findings accepted)

| Finding | Severity | Change made |
| --- | --- | --- |
| KV Put/Update can't carry a TTL; UpdateAsync renew makes a permanent lock | Critical | TTL publish helper (section 3) for Set, fenced write and renew; `_locks` MaxAge backstop |
| Fencing rejects ordinary misses as NATS TTL = logical TTL | Critical | Nats-TTL = logical + grace; exact fence rule (section 5) |
| TTL requires LimitMarkerTTL; Create can fail spuriously (#5162) | High | Mandatory on both buckets; spurious failure = lost, retry |
| Memory notifications stream loses events after full restart | High | File-backed stream + StreamInfo-based resume check (section 7) |
| Fenced value returned to waiters breaks I5 | High | Waiters get the re-read L2 value |
| Open-mode writes leave stale L2 on other nodes | High | Outage keys journaled, replayed as del |
| Large-value cleanup had no listener | Medium | Meta-only watch by the sweeper-lease holder; chunk loss = miss |
| Self-event suppression + unconditional L1 write | Medium | Compare-on-revision L1 writes; own events processed |
| I1 false positives under renewals / Open mode | Medium | Waiters wait while the lease is renewed; degraded windows whitelisted |
| I2 not measurable per key | Medium | Reason-tagged Origin ledger |
| K3 shape and CPU contention on 6 cores | Medium | http.batch with about 2 000 VUs; cpuset split; measure-then-set latency gates |
| Strict keys break framework adapter callers | Medium | Adapters hash invalid keys; no tags for them |
| Stale text, version pin, names, AddHybridCache detection, skew, MaxBytes | Low | Pull-based consumer wording; pin 2.12.15 (reason corrected in review 2); KV_/OBJ_ names; warning not throw; server timestamps; l2_full behaviour |

### Review 2: Fable, Sep 30 2026 (all findings accepted, NATS pinned to 2.12.15)

Verdict: ready to start the milestone-1 spike with the extended scope; H1, M1, M3 and M4 had to be settled before milestone 3 and are now in the design.

| Finding | Severity | Change made |
| --- | --- | --- |
| L1 fill race after a newer event; Direct Get replica lag can regress a node | High | Per-key high-water revision; L1 filled only at or above it, bounded re-read (section 7) |
| Version pin rested on a non-bug (#7361) | High | Pin 2.12.15; minimum 2.11.2 (#6741), 2.12.1+ preferred (#7344) |
| File-backed R3 cost and startup semantics | High | sync_interval 2 min explicit, named volumes, memory store = provisioning failure not crash, K3 gates on lock ops/s and NATS CPU, MaxAge backstop tested in C4 |
| Fence rule gaps: DEL has no value; "0 = absent" fails on markers | Medium | DEL returns the factory result uncached; expected revision taken from the marker; at most 3 attempts |
| Stale text after review 1 | Medium | x-cache-ttl + entry.Created everywhere; Renew row uses the helper; failure table and degraded table updated; Q3 covers all stores |
| Reason tag unreachable from the library; I2 circular | Medium | FactoryContext (Reason, LockToken, Attempt); I2 expected from the k6 timeline |
| Open-mode replay-as-del clobbers newer writes | Medium | Revision-fenced delete, skipped when someone wrote newer; local fallback writes only through the fence |
| Unbounded wait while the lease renews | Medium | Capped at FactoryTimeout + LeaseTtl past the first observed revision |
| K3 under Open mode | Low | K3 runs Closed; gate on non-degraded calls |
| Adapter key namespace, sliding expiration | Low | d. namespace; Refresh = re-put |
| LimitMarkerTTL wording | Low | ≥ 1 s |
| cpuset headroom | Low | Documented as expected noise |
| StreamInfo check only on reconnect | Low | Also on every ordered-consumer recreate |

### Review 3: Fable, Sep 30 2026 (all findings accepted)

Verdict: close to v1.0; H1–H3, M1 and M2 had to land before code because they change the key model, delete path, read primitive and ledger schema. The spike could start in parallel.

| Finding | Severity | Change made |
| --- | --- | --- |
| HWM, lock and events keyed by user key, not the `_s{n}` internal key | High | Internal key everywhere; only tag and clear are prefix-based (section 3) |
| Open-mode replay has no revision for keys the node never read | High | Leader-read each journaled key; delete only if created before the node's first outage write |
| Fence and HWM re-reads hit lagging replicas via Direct Get; no outcome after 3 attempts | High | Leader read (LastBySubj) or fetch by the 10071 sequence; fenced-exhausted outcome |
| del/tag don't raise HWM; DeleteAsync returns no revision | Medium | DEL via publish helper; del raises HWM; tag holds a prefix HWM |
| fence-retry reason unreachable | Medium | Reason = miss / early-refresh / takeover / degraded + separate Outcome |
| I2 not computable for Zipf K1 | Medium | Exact for fixed-key scenarios, upper bound for K1 |
| Object naming vs fencing | Medium | `{internalKey}.{guid}`; fenced writer deletes its object; sweeper removes pointer-less objects |
| No consumer-recreate hook in NATS.Net | Medium | Detect gaps from the delivered stream sequence |
| I8 via Envoy; no per-run mode selection | Medium | Per-node measurement; Envoy hc 1 s; `Cache__FailureMode` env var + header |
| API and options drift | Low | Interface, enums, records and options completed |
| Fence expected revision source | Low | Revision from the post-lock double-check |
| Lease vs MaxAge; waiters use own timeouts | Low | `MaxLeaseTtl`; owner's timeouts in the lock value |
| Early refresh on lost lock; HybridCache tags; spike additions | Low | No-op on lost lock; prefix-shaped tags validated at write; spike scope extended |

### Review 4: Fable, Sep 30 2026 (all findings accepted; document frozen as v1.0)

Verdict: freeze as v1.0 after small edits, no further round. All APIs named in the design were verified against the NATS.Net and nats-server sources. Answered from source, so dropped from the spike: `CreateAsync(ttl)` against a delete marker keeps the TTL.

| Finding | Severity | Change made |
| --- | --- | --- |
| RemoveAsync can't enumerate schema versions; no version source | High | `SchemaVersion` + `KnownSchemaVersions` options; delete exactly those keys, no listing |
| Fence: expired newer PUT undefined | Medium | Retry with that revision, counts toward 3 attempts, never handed to waiters |
| Leader read returns a raw stream message | Medium | Library parses headers, Time, markers; 10037 = absent |
| Chaos matrix claimed unconditional L1 flush | Medium | C4/C7 flush only on lost events or outage writes |
| TTL below LimitMarkerTtl clamped unless History 1 | Medium | History = 1 hard invariant, enforced at provisioning |
| Stale `{key}` wording; waiter events | Low | Internal key everywhere; set, del or fail |
| Two sweeper rules | Low | One rule: no live pointer and older than max L2 TTL + grace |
| long vs ulong revisions | Low | `ulong` |
| Default lease above MaxLeaseTtl | Low | Default clamps; explicit value throws |
| Recovery compares server and local clocks | Low | 1 s skew margin |
| Tag rev with zero matches; clear subject; tag window | Low | Defined in sections 4 and 7 |

### v1.1: changes from the milestone-1 spike (Sep 30, 2026)

Evidence in `spike/RESULTS.md` (NATS 2.12.15 and 2.11.2, NATS.Net 3.3.0, 0.5 CPU per NATS node).

| Spike finding | Change made |
| --- | --- |
| 2.11.2 never expires a TTL'd lock written before a full cluster restart | Minimum server version 2.12.1; CI tests 2.12.1; pin stays 2.12.15 |
| NATS.Net `CreateAsync` fails spuriously (4.4–5.3 % on 2.12.15) on uncontended tombstoned keys | Acquire through the publish helper with a leader read; release as a helper DEL |
| Lock operations saturate 0.5 CPU NATS nodes at about 2 000 ops/s | K3 budget and capacity-bound reporting; baseline numbers in section 9 |
| Host has 12 logical CPUs, not 6 | Section 9 corrected; cpuset mapping by logical CPU; full-size profile to be measured |
| Markers, CAS on marker revisions, helper DEL, 10071 text, leader-read cost, Direct Get staleness, ordered-consumer silent skip | Confirmed as designed; no change |

### Review 5: Fable, Sep 30 2026 (all findings accepted; v1.1 frozen)

Verdict: freeze v1.1 with small edits; milestone 2 is clear to start. Three spike additions before milestone 3 (results in `spike/RESULTS.md`).

| Finding | Severity | Change made |
| --- | --- | --- |
| 2.12.x no longer receives fixes; #8594 TTL leak fixed only in 2.14.8 / 2.15.1 | High | CI matrix adds latest 2.14.x; spike re-run on 2.14.7; never purge or roll up TTL'd buckets |
| Second-publish 10071 not always "lost" (marker expired, MaxAge marker) | Medium | Acquire interprets the 10071 sequence; 10037 → expect 0; at most 3 attempts |
| Helper acquire never run under contention; release didn't race losers | Medium | New spike test helper-contention: 500 acquisitions over 5 nodes, never more than one owner, no starvation, on 2.12.15 and 2.14.7. It exposed two more rules, now in section 5: transient 10164 is retried in the helper, and an acquire recognises its own committed token |
| Overwrite of a TTL'd message and renewals untested | Medium | New spike test ttl-overwrite: the replacement survives the old 5 s timer; 10 renewals keep a 5 s lease alive for 15 s and it expires 5.1 s after the last one (2.12.15 and 2.14.7) |
| Throughput ceiling is one closed-loop point | Medium | Marked provisional; worker sweep before milestone 6 |
| Direct Get staleness ~5 % under load | Low | Noted in the HWM section |
| #7344 also in 2.11.10 | Low | Version text corrected |
| Rule gaps: 10037 after 10071; release revision and ordering | Low | Written into the Acquire and Release rows |
| Stale text: 6-CPU, NATS.Net v2, TTL only on CreateAsync | Low | Corrected; NATS.Net pinned 3.3.0 |

### v1.2: NATS version requirement (Sep 30, 2026)

The owner set the requirement to the latest stable NATS release. The full spike suite passes on 2.15.0 (13 of 13 checks; `lock-throughput-kv` fails by design as evidence).

| Change | Detail |
| --- | --- |
| Required server version | Latest stable only, currently 2.15.0; no older lines are supported or tested |
| Upgrade policy | Move the pin when a new stable release ships and the spike suite passes on it; never pin a release candidate |
| CI | Runs the spike suite and later the system tests on the pinned release only |

### Review 6: Fable, Sep 30 2026 (all findings accepted; v1.2 frozen)

Verdict: freeze v1.2 after text edits; the protocol is unchanged. The spike was fixed and re-run on 2.15.0: all checks pass, including a new leader-failover test (two leader restarts, 10 unknown outcomes and 2 duplicate acks resolved, 0 spurious losses, 0 stuck locks).

| Finding | Severity | Change made |
| --- | --- | --- |
| "Committed but client saw an error" cause was wrong: expected-sequence errors never coexist with a commit; the real case is a lost reply/timeout, which the helper did not handle | High | Helper outcomes Committed / Rejected / Unknown; Unknown resent with the same msg id, settled by dedup or leader read; own-token rule kept with the correct cause |
| Msg-id semantics; duplicate ack = success; lease start; dedup memory | Medium | Msg-id rules in section 3; lease starts at the committed message's time; dedup memory added to risks |
| 10164 retry bounds and exhaustion | Medium | 6 sends, about 124 ms; exhausted = Unknown, never a conflict |
| #8594 scope on 2.15.0 | Medium | Ban extended to truncate and compact; `PurgeAsync` with TTL banned even in tests |
| Stale text: RequestTimeout, 2.11 risk row, RESULTS minimum version | Low | Corrected |
| CI version gate | Low | Script checks `/varz` against the pin and refuses RC images |
| Spike code: publish exception handling, timing-based owner check, token as string, raw leader read, loose gates, script robustness | High–Low | Rewritten: tri-state helper, sequence-interval mutual-exclusion check, byte comparison, `INatsJSStream.GetAsync`, assertions on watch order and latency, try/finally connection pool, trap cleanup, failing health wait, per-run results cleanup, three CPU samples |
| Leader reads fail with no-response during an election (found by the new failover test) | New | Leader reads are retried like Unknown outcomes (section 3) |
