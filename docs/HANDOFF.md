# Handoff: NATS distributed cache

Oct 1, 2026 (late evening). Milestones 1–3 are done and pushed. Milestone 4 (invalidation) is implemented and committed, the owner's `//TODO-AI` review is addressed, and the unit tests are validated: 242 tests, green under a 4× parallel stress run, Stryker 56.9 % (71.4 % of covered mutants), and all 41 sabotage checks caught (see "Proving the tests catch bugs" under "How to run"). Not yet done for milestone 4: integration tests against real NATS, the DESIGN.md update and its Fable review. The design is frozen at v1.3 in `docs/DESIGN.md`, the single source of truth; milestone 4 hasn't changed it yet.

## State at a glance

| Milestone (DESIGN.md §12) | Status | Where |
| --- | --- | --- |
| 1. Spike: NATS behaviour the design relies on | Done, committed (`74209b3`) | `spike/`, results in `spike/RESULTS.md` |
| 2. Core library: L1/L2, jitter, keys, provisioning, unit tests | Done, committed (`7e0aae4`) | `src/`, `tests/` |
| 3. Distributed single-flight: leases, renewal, fencing, early refresh | Done, reviewed (review 7), committed (`4a16c84`) | `Internal/DistributedLock.cs`, `NatsCache.cs` |
| Spike worker sweep (§9) | Done, committed (`b1c9f5b`), run on `eriks` | `spike/run-sweep.sh`, `spike/docker-compose.full.yml` |
| 4. Invalidation stream, reconnect resume, degraded-mode journal | Implemented, owner review addressed, unit tests validated, committed; integration tests and Fable review to do | see below |
| 5. TestApi, Origin, compose rig with CPU limits, Prometheus/Grafana | To do | — |
| 6. k6 K1–K8 and Validator (I1–I8) | To do | — |
| 7. Chaos C1–C8, tuning, v1.0 | To do | — |

## Milestone 4: where it stands

New files (uncommitted):
- `src/NatsDistributedCache/Internal/Notifications.cs`: `CacheEvent` (`{"op","key","rev","node","ts"}`, encode/decode), `INotificationTransport`, `KeySignals` (the local waiter table), `NotifySubjects`.
- `src/NatsDistributedCache/Internal/NotificationListener.cs`: `NatsNotificationTransport` (ordered consumer with `FilterSubjects`, `DeliverPolicy` New / ByStartSequence, `MaxResetAttempts = int.MaxValue`, msg-id publishes, `ConnectionOpened` → `Reconnected`) and `NotificationListener` (one consumer per node, the rule-7 gap check).
- `src/NatsDistributedCache/Internal/OutageJournal.cs`: keys and tag prefixes written L1-only in Open mode, with the first outage write time.
- `src/NatsDistributedCache.Extensions/NatsCacheHealthCheck.cs`: `IHealthCheck` plus `AddNatsCache()` (tag "ready").
- `tests/NatsDistributedCache.UnitTests/FakeNotificationTransport.cs`, `InvalidationTests.cs` (18 tests).

Changed (uncommitted): `NatsCache.cs` (most of the work), `CacheTypes.cs` (`FactoryOutcome`, `FactoryCompletion`, `CacheHealth`), `INatsCache.cs` (`ClearAsync`), `NatsCacheOptions.cs` (`OnFactoryCompleted`, `OutageJournalCapacity`), `L2.cs` / `NatsL2Store.cs` (`LastSequenceAsync`), `L1Store.cs` (`Invalidate`, `RaiseFloor`, eviction-callback fix), `CacheMetrics.cs` (event, flush, outage, recovered counters), the Extensions csproj (`Microsoft.Extensions.Diagnostics.HealthChecks` 10.0.12), and the test harness (`NewCache` passes a shared `FakeNotificationTransport`; the fake L2 has `LastSequenceAsync`).

What is implemented, mapped to the design:
- **Events** (§7): `set` from SetAsync, UpdateAsync and committed factory writes; `del` per schema version from RemoveAsync; one `tag` per RemoveByTagAsync (rev = the highest tombstone, or the bucket's last sequence if no key matched); `clear` from the new `ClearAsync`; `fail` when a lock owner's result did not reach L2 (factory threw, fenced-del, fenced-exhausted, uncacheable). Publishing is best effort (logged and counted; L1Ttl bounds staleness) and is awaited by the writer.
- **Release before event** (§5): `RunOwnedAsync` does factory → fenced write → `EndLeaseAsync` → event → `OnFactoryCompleted`.
- **Consumer** (§7 rules 1–7): `HandleEvent`. `set` = evict only if older, then raise the high-water revision to `rev` (`L1Store.Invalidate`); `del` = evict + floor; `tag` = prefix evict + prefix floor; `clear` = flush. Gap check: a stream-sequence jump > 1 or a reconnect reads StreamInfo; a recreated stream, `FirstSeq > lastSeen + 1` or `LastSeq < lastSeen` = lost, so it flushes L1 and resumes after the stream's current `LastSeq` (not from "new", so no window); otherwise it resumes from `lastSeen + 1`. Malformed events are skipped.
- **HWM on serve** (§7, I5): `TryUse` and `FillL1` call `L1Store.RaiseFloor` with the served revision.
- **Waiters** (§5): `WaitForOwnerAsync` parks on `KeySignals.Next(ik)` (pulsed by set/del/fail/tag/clear) with the 250 ms poll as fallback; the signal is re-armed before each read so an event during the reads isn't missed.
- **Outage journal** (Q2, §8 step 2): `EnterOutage` (any `L2UnavailableException`) marks the outage and starts `RecoveryLoopAsync`, which probes `LastSequenceAsync` every 1 s. `RecoverAsync` always flushes L1, then for each journaled key leader-reads it and deletes it at that revision (plus a `del` event) if its `Created` is older than the first outage write minus 1 s. If NATS drops mid-replay, the journal is restored.
- **Outcome hook** (§9 Origin ledger): `NatsCacheOptions.OnFactoryCompleted(FactoryCompletion)` with outcome Cached / FencedNewer / FencedDel / FencedExhausted / UncachedL2Full / Uncached / LocalOnly / Failed, the revision, lock token, attempt and timestamps.
- **Health** (§8): `NatsCache.Health` / `HealthDescription` (stores unusable, connection not Open, outage, not provisioned) → Degraded in Open mode, Unavailable in Closed mode.
- **Connection settings** (§8) in `Build`: ReconnectWaitMin 100 ms, Max 2 s, jitter 100 ms, ConnectTimeout 2 s, RequestTimeout 2 s, PingInterval 10 s, MaxPingOut 2.

Bug found and fixed along the way (milestone 2): `L1Store`'s key index removed a key whenever its MemoryCache entry was *replaced* (the post-eviction callback fires with `EvictionReason.Replaced`), so `Clear()` and tag eviction missed refilled keys. The callback now ignores `Replaced`. This edit was lost once when the owner's editor saved `L1Store.cs` over it and has been re-applied (around line 103). Check it is still there.

Done since the first handoff: the owner's five `//TODO-AI` comments (`L1Item<T>` stores values unboxed; more `Decode` tests; the rest answered: lock vs semaphore, cache vs object store, single-flight threads), the `Eventually` fix (it asserts its last evaluation), seven timing-fragile tests fixed, a `SingleFlight` race fixed (a caller woken by a failed load could rejoin that finished load and get its failure instead of a retry: the key now leaves the table before the task completes), and 103 new tests from the mutation and sabotage runs.

Open items, in order:
1. **Prefix floors and `L1Store.Clear()`** (warning, future work; see "Deviations and gaps"): decide whether `Clear()` should keep the prefix floors, and fix it with a test plus a sabotage check.
2. **Bound concurrent NATS calls** (owner's question on `SingleFlight`): a `MaxConcurrentL2Operations` limit (SemaphoreSlim) as part of the §8 resilience pipeline.
3. **Integration tests** against real NATS: set/del/tag events across two nodes; a waiter woken by the event; gap and resume after a reconnect (restart the container or drop the connection); outage replay; the health check through DI.
4. **Design doc**: record milestone 4 (and the prefix-floor decision) in DESIGN.md (subscribed sentinel, resume after `LastSeq` instead of "new", `ClearAsync` on the interface, `fail` on every uncached owner outcome, outcome hook, health, journal capacity, connection settings), then a **Fable review**, apply all findings, log it in §13 and bump to v1.4.
5. Then the v1.4 rig pass for `eriks` (below) and milestones 5–6.

## Remote load-test machine (`eriks`)

- `ssh eriks` (key auth). AMD Ryzen 9 9950X3D (16 cores / 32 threads; SMT siblings are n and n+16), 61 GB RAM, Ubuntu, kernel 7.0, Docker 29.7.2, Compose v5.5.0, .NET SDK 10.0.112.
- Project root on the remote: `~/Documents/nats-distributed-cache` (same name as on the laptop). Sync with `rsync -az --delete --exclude bin/ --exclude obj/ --exclude spike/results/ --exclude TestResults/ ./ eriks:Documents/nats-distributed-cache/`.
- **Use only the system drive** (`/`, a SATA Samsung 850 EVO, where Docker's data root is). The NVMe drives are NTFS Windows partitions: don't mount or use them (owner's instruction). No passwordless sudo.
- All Docker content on the remote was removed with the owner's permission (old FlowBase stack, 84.7 GB). Remove anything of ours that's left after each run.
- The laptop reaches it over Wi-Fi: deploy and run everything there (detached with `setsid nohup …`), and fetch only result files. Don't `pkill -f` with a pattern that matches your own SSH command line.
- Results from the Oct 1 run are in `spike/results/eriks/` on the laptop (gitignored). Build clean; 121 unit and 16 integration tests passed; spike suite status 0 (only `lock-throughput-kv` fails, by design, 2.5 % spurious CreateAsync failures).
- Helper lock sweep, zero errors in every run:

| Workers | Scaled 0.5 CPU/node: cycles/s · p99 | Full 1 CPU/node: cycles/s · p99 |
| --- | --- | --- |
| 8 | 4 566 · 9.2 ms | 9 066 · 5.9 ms |
| 16 | 5 792 · 17.7 ms | 11 819 · 7.4 ms |
| 32 | 7 420 · 21.8 ms | 14 913 · 9.2 ms |
| 64 | 8 683 · 30.7 ms | 17 662 · 16.4 ms |

  The stream leader averaged about 31 % of its 50 % cap (scaled) and about 67 % of 100 % (full), so the leader is not fully saturated. Re-baseline the K3 budget (§10) from these numbers instead of the laptop's 2 000 ops/s.
- In the first full run, the helper step inside `run-spike.sh` left no result (status 1); the re-run was clean. The cause is unknown.

## Planned v1.4 design pass (before milestones 5–6)

Agreed with the owner, not started:
- CPU layout for the 9950X3D: NATS + 5 API nodes on CCD0 (cores 0–7, siblings 16–23); Envoy, Origin, observability and k6 on CCD1. The §9 pinning is written for the 12-thread laptop.
- The full-size profile (3 × 1 CPU NATS + 5 × 0.5 CPU API = 5.5 CPU) fits; targets move to the full-size 5 000 RPS.
- K3 budget from the sweep; record the SATA disk in the rig description.
- Remote workflow: build images and run k6 (as a container) on `eriks`; fetch only the JSON summaries and the Validator report; Grafana through an SSH tunnel.

## Repo layout

```
docs/DESIGN.md        design v1.3 (frozen); §13 logs seven Fable reviews
docs/HANDOFF.md       this file
spike/                milestone-1 spike: Program.cs (14 checks, SPIKE_WORKERS), compose rig + full-size override, run-spike.sh, run-sweep.sh, RESULTS.md
src/NatsDistributedCache/             core library (netstandard2.1 + net10.0)
  NatsCache.cs                        facade: read path, lock, fenced writes, events, consumer handling, outage + recovery, health
  INatsCache.cs, CacheTypes.cs, CacheEntryOptions.cs, NatsCacheOptions.cs, Serialization.cs   public API
  Internal/NatsL2Store.cs             L2 over NATS: publish helper (Committed/Rejected/Unknown), Direct Get, leader read; used for _cache and _locks
  Internal/DistributedLock.cs         LockToken, DistributedLock (acquire/renew/release), LockLease (renewal loop, cap, best-effort release)
  Internal/Notifications.cs           CacheEvent, INotificationTransport, KeySignals (milestone 4)
  Internal/NotificationListener.cs    NATS transport + per-node consumer with the gap check (milestone 4)
  Internal/OutageJournal.cs           Open-mode outage writes (milestone 4)
  Internal/L2.cs                      IL2Store abstraction, L2Entry, WriteResult
  Internal/L1Store.cs                 MemoryCache wrapper: compare-on-revision, revision floors, prefix floors
  Internal/Expiration.cs              jitter, Nats-TTL grace, lease default/bounds
  Internal/CacheKeys.cs, Provisioning.cs, SingleFlight.cs, PayloadCodec.cs (Brotli), CacheMetrics.cs (Meter "NatsDistributedCache")
src/NatsDistributedCache.Extensions/  AddNatsDistributedCache, hosted startup, NatsCacheHealthCheck / AddNatsCache()
src/Shared/IsExternalInit.cs          polyfill for records on netstandard2.1
tests/NatsDistributedCache.UnitTests/         xUnit v3; FakeL2Store (KV), FakeNotificationTransport (stream); fake time
tests/NatsDistributedCache.IntegrationTests/  xUnit v3 + Testcontainers, single node nats:2.15.0-alpine, Replicas = 1
```

## How to run

```bash
dotnet build NatsDistributedCache.slnx                           # both TFMs, warnings as errors
dotnet run --project tests/NatsDistributedCache.UnitTests        # 242 tests, ~9 s
dotnet run --project tests/NatsDistributedCache.UnitTests -- -method "*InvalidationTests*"   # one class
dotnet run --project tests/NatsDistributedCache.IntegrationTests # 16 tests, ~13 s, needs Docker
spike/run-spike.sh                                               # full spike on nats:2.15.0-alpine, ~4 min, needs Docker
spike/run-sweep.sh scaled|full                                   # helper lock sweep 8/16/32/64 workers
tests/Sabotage/run.py [--only id,id] [--jobs N]                  # 41 deliberate design-rule bugs; each must fail its test; writes tests/Sabotage/REPORT.md (~25 min locally, 6 min on eriks with --jobs 8)
cd tests/NatsDistributedCache.UnitTests && DOTNET_ROOT=$HOME/.dotnet dotnet stryker --output <dir>   # mutation testing, ~30 min locally, 7 min on eriks (DOTNET_ROOT=/usr/lib/dotnet)
```

Proving the tests catch bugs:
- **Stryker.NET 5.0** (local tool in `dotnet-tools.json`, config in `tests/NatsDistributedCache.UnitTests/stryker-config.json`). It must use `test-runner: mtp`: under the VSTest runner, xUnit v3 runs the test exe out of process and every mutant falsely "survives". `DOTNET_ROOT` is needed because the SDK lives in `~/.dotnet`. Stryker cannot mutate some async methods (`LoadAsync`, `RunOwnedAsync`, `RefreshAsync`, `DegradedAsync`, `CheckGapAsync`, `ConsumeAsync`: their mutants do not compile, "safe mode"); the sabotage suite covers those.
- **Sabotage suite** (`tests/Sabotage/sabotage.json` + `run.py`): each entry breaks one design rule by find/replace in a temp copy of the repo and names the test(s) that must fail. Add an entry for every new rule.
- Timing-sensitive tests must survive a 4× parallel stress run (4 `dotnet run` processes at once). Background loops arm fake-clock timers late under load: advance until the effect is seen (`AdvanceUntil`), or wait for the timer with `CountingTimeProvider`.

## How the code maps to the design

- **Read path** (§4): `GetOrCreateAsync` → `TryL1` (+ `MaybeRefresh`) → `SingleFlight.RunAsync` → `LoadAsync`: Direct Get above the floor (`ReadAboveFloorAsync`), lock acquire, waiter or owner, leader double-check, `RunOwnedAsync`.
- **Distributed lock** (§5): `DistributedLock.TryAcquireAsync` (spike `HelperAcquire` port); `LockLease` renews every LeaseTtl / 3 and stops at FactoryTimeout + LeaseTtl; release is best effort within 5 s. Waiters: `WaitForOwnerAsync` (signal or 250 ms poll, leader read of the lock key, takeover when free, the v1.3 live-token rule, then FailureMode).
- **Fence rule** (§5): `WriteFencedAsync` returns `Fenced<T>(value, outcome, revision)`. Empty, marker, purge or logically expired head = retry with its revision; DEL = fenced-del; live newer = fenced-newer; Unknown = settled by the `Nats-Msg-Id` at the head; at most 3 attempts.
- **Early refresh** (§6): `L1Item.RefreshAt`, claimed once per L1 fill, `RefreshAsync` (lock or no-op) → `RunOwnedAsync`.
- **Shutdown**: `DisposeAsync` cancels waiters and background work, waits up to `ShutdownTimeout` for `_activeLeases`, stops the recovery loop and the listener; it never releases a lock under a running factory.
- **Logical expiry**: server `Created` + `x-cache-ttl` header (ms), never the writer's clock (`TryUse`).
- **Failure modes** (Q1/Q2): `L2UnavailableException` → Open runs the factory locally (`FactoryReason.Degraded`, outcome LocalOnly) and fills L1 only; Closed throws `CacheUnavailableException`. `UpdateAsync` (CAS) always throws when NATS is down.
- **Provisioning** (§3): `Provisioner.EnsureAsync` in a background loop with backoff; the notifications listener starts once the stores are ready. A store that exists with memory storage, History > 1, or no TTL/markers sets `ProvisioningError`.

## Deviations and gaps to know about

- **Direct Get uses `INatsJSStream.GetDirectAsync`**, not the KV API, because NATS.Net `NatsKVEntry` exposes no headers. Sequence and time come from the `Nats-Sequence` / `Nats-Time-Stamp` reply headers (nanoseconds trimmed to 7 digits). 404 = absent.
- **Leader reads use `INatsJSStream.GetAsync(StreamMsgGetRequest)`** (missing from the XML docs but present in 3.3.0). 10037 = absent; 10059 (stream not found) maps to `L2UnavailableException`.
- **NATS.Net XML docs omit record properties**; to check an API, reflect over the 3.3.0 assemblies (see the scratch probe approach: a tiny console project that prints `GetProperties()`).
- **All writes are RAFT-atomic per key**: our publish helper uses the same `Nats-Expected-Last-Subject-Sequence` check NATS.Net's KV `CreateAsync` / `UpdateAsync` use, without their TTL and Direct-Get problems (answered for the owner on Oct 1).
- **Not implemented yet**: large values in the Object Store (returned uncached, `cache.large_values_skipped`); `HybridCache` / `IDistributedCache` adapters (Q10); circuit breaker and resilience pipeline (§8; today the L2 store retries, then degrades).
- **Warning: `L1Store.Clear()` drops the prefix floors but keeps the per-key floors.** After a flush (a `clear` event, lost events, or recovery), a lagging replica's Direct Get could refill a key under a tag-deleted prefix with a value older than that tag delete, until the entry expires: an I5 regression window on one node. Per-key floors survive because they live in the MemoryCache under their own keys, outside the user-key index that `Clear()` walks. Likely fix: keep the prefix floors in `Clear()` (they expire on their own after `FloorTtl`). Deliberately not pinned by a test until decided; Stryker reports the `_prefixFloors.Clear()` line as a survivor for that reason.
- **Fenced-del and waiters**: an owner fenced by a delete publishes `fail`, and waiters take over and run the factory again. This is a fresh load after a delete, not an I1 overlap, because the owner's factory has finished.
- **Unit-test fakes**: `FakeL2Store` models History 1, a global sequence, 10071 with the last sequence in the text, msg-id dedup, TTL → MaxAge marker → empty, DEL tombstones, stale Direct Gets, lost put/delete replies, unknown puts that never apply, rejections, outages and `LastSequenceAsync`. `FakeNotificationTransport` models a global sequence, live delivery, start-by-sequence that silently skips to `FirstSeq`, `SkipDeliveries`, `DiscardUpTo`, `Recreate`, `Disconnect` + `RaiseReconnected`, and outages. Keep both in sync with real NATS behaviour; confirm anything new in the spike or the integration tests first.

## NATS facts that cost real time (all spike-verified on 2.15.0)

- NATS.Net `PutAsync` / `UpdateAsync` take no TTL, and an `UpdateAsync` turns a TTL'd entry into one that never expires. Every TTL write is a raw publish to `$KV.{bucket}.{key}` with `Nats-TTL`, `Nats-Expected-Last-Subject-Sequence` and `Nats-Msg-Id`.
- NATS.Net `CreateAsync` fails spuriously (2.5–20 %) on uncontended tombstoned keys because it re-reads through Direct Get. Never use it for locks.
- Direct Get is not read-after-write coherent: 0.05–0.3 % stale idle, about 5 % under lock load. Fences, lock polling and HWM checks re-read through the leader.
- 10164 = another write to the subject is in flight and nothing was stored: retry the identical publish. 10071 = wrong last sequence, and its text carries the sequence. 10158 = duplicate msg id still being applied. A duplicate ack (`ack.Duplicate` or `NatsJSDuplicateMessageException`) = success at the original sequence.
- An expected-sequence error never coexists with a commit; only a lost reply or timeout leaves the outcome unknown. Resend with the same msg id; a new expected revision needs a new msg id.
- TTL expiry writes a `Nats-Marker-Reason: MaxAge` marker, which expires after `LimitMarkerTTL`. Buckets must be History 1, or short TTLs are silently raised to the marker TTL.
- Never purge, roll up, truncate or compact TTL'd buckets (#8594, fixed only in 2.15.1-RC).
- An ordered consumer started below `FirstSeq` silently skips ahead, and NATS.Net has no recreate callback: detect gaps from `Metadata.Sequence.Stream`.
- Leader reads can fail with `NatsJSApiNoResponseException` during an election; retry them.
- `MemoryCache` post-eviction callbacks also fire with `EvictionReason.Replaced` when an entry is overwritten.

## Owner's working agreements

- Update only the local `docs/DESIGN.md`. The online Claude Docs draft was abandoned after v1.0.
- Every design change gets an adversarial review by a subagent on the Fable model. The owner then asks for all findings to be applied, each round is logged in DESIGN.md §13, and a frozen doc gets a version bump. Fixes must keep the premise: one factory execution per key, run only under the distributed lock (outside FailureMode).
- NATS: latest stable release only (currently 2.15.0; the pin moves after the spike passes; never an RC). NATS.Net pinned at 3.3.0.
- All NATS stores are file-backed; memory storage is never used.
- Git: remote `origin` (GitHub, `eriksimonic/NatsDistributedCache`). Commit messages carry no attribution lines. Docs and the spike are committed and pushed with the code; never rewrite history to strip them. Ask the owner before committing or pushing.
- The owner reviews code by adding `//TODO-AI` comments. While a review is in progress, don't edit the files being reviewed; afterwards, address every comment.
- Remote machine rules: see "Remote load-test machine" above (system drive only, deploy remotely, fetch only results).
