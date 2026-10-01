# Handoff: NATS distributed cache

Oct 1, 2026. Milestones 1 (spike) and 2 (core library) are done and committed on `main`. Next up is milestone 3, the distributed lock. The design is frozen at v1.2 in `docs/DESIGN.md`, the single source of truth.

## State at a glance

| Milestone (DESIGN.md §12) | Status | Where |
| --- | --- | --- |
| 1. Spike: NATS behaviour the design relies on | Done, committed (`74209b3`) | `spike/`, results in `spike/RESULTS.md` |
| 2. Core library: L1/L2, jitter, keys, provisioning, unit tests | Done, committed | `src/`, `tests/` |
| 3. Distributed single-flight: leases, renewal, fencing, integration tests | Next | — |
| 4. Invalidation stream, reconnect resume, degraded-mode journal | To do | — |
| 5. TestApi, Origin, compose rig with CPU limits, Prometheus/Grafana | To do | — |
| 6. k6 K1–K8 and Validator (I1–I8) | To do | — |
| 7. Chaos C1–C8, tuning, v1.0 | To do | — |

Test status: 82 unit tests and 8 integration tests (real NATS 2.15.0 via Testcontainers) pass. The solution builds for netstandard2.1 and net10.0 with warnings as errors.

## Repo layout

```
docs/DESIGN.md        design v1.2 (frozen); §13 logs six Fable reviews and the spike-driven changes
docs/HANDOFF.md       this file
spike/                milestone-1 spike: Program.cs (14 checks), compose rig, run-spike.sh, RESULTS.md
src/NatsDistributedCache/             core library (netstandard2.1 + net10.0)
  NatsCache.cs                        facade: read path, fenced writes, failure modes, background provisioning
  INatsCache.cs, CacheTypes.cs, CacheEntryOptions.cs, NatsCacheOptions.cs, Serialization.cs   public API
  Internal/NatsL2Store.cs             L2 over NATS: publish helper (Committed/Rejected/Unknown), Direct Get, leader read
  Internal/L2.cs                      IL2Store abstraction, L2Entry, WriteResult
  Internal/L1Store.cs                 MemoryCache wrapper: compare-on-revision, revision floors, prefix floors
  Internal/Expiration.cs              jitter, Nats-TTL grace, lease default/cap
  Internal/CacheKeys.cs               key rules, internal key {key}._s{n}
  Internal/Provisioning.cs            create/verify the 4 stores
  Internal/SingleFlight.cs, PayloadCodec.cs (Brotli), CacheMetrics.cs (Meter "NatsDistributedCache")
src/NatsDistributedCache.Extensions/  AddNatsDistributedCache + hosted service that starts provisioning
src/Shared/IsExternalInit.cs          polyfill for records on netstandard2.1
tests/NatsDistributedCache.UnitTests/         xUnit v3; FakeL2Store emulates the KV semantics the spike verified
tests/NatsDistributedCache.IntegrationTests/  xUnit v3 + Testcontainers, single node nats:2.15.0-alpine, Replicas = 1
```

## How to run

```bash
dotnet build NatsDistributedCache.slnx                       # both TFMs, warnings as errors
dotnet run --project tests/NatsDistributedCache.UnitTests        # 82 tests, < 1 s
dotnet run --project tests/NatsDistributedCache.IntegrationTests # 8 tests, ~6 s, needs Docker
spike/run-spike.sh                                            # full spike on nats:2.15.0-alpine, ~4 min, needs Docker
```

The xUnit v3 test projects are executables (`dotnet run`); `dotnet test` also works. The host has 12 logical CPUs; the spike rig uses the halved limits from DESIGN.md §9.

## How milestone 2 maps to the design

- **Read path** (§4): `NatsCache.GetOrCreateAsync` → `TryL1` → `SingleFlight.RunAsync` → `LoadAsync`: a Direct Get that re-reads through the leader when below the key's floor (`ReadAboveFloorAsync`), then a leader double-check, then the factory, then `WriteFencedAsync`.
- **Where milestone 3 plugs in**: `NatsCache.LoadAsync` has a comment "Milestone 3 takes the distributed lock here", between the Direct Get miss and the leader double-check. The lease protocol is in DESIGN.md §5 and the spike's `HelperAcquire` / `HelperRelease` (`spike/Spike/Program.cs`), which review 6 judged fit to carry over.
- **Fence rule** (§5): `WriteFencedAsync`. Empty, marker, purge or logically expired head = retry with its revision; DEL = return the result uncached; live newer = return it; Unknown = settle by the `Nats-Msg-Id` at the head of the subject; at most 3 attempts.
- **Logical expiry**: server `Created` + `x-cache-ttl` header (ms), never the writer's clock (`TryUse`).
- **Failure modes** (Q1/Q2): `L2UnavailableException` from the L2 store → Open runs the factory locally (`FactoryReason.Degraded`) and fills L1 only; Closed throws `CacheUnavailableException`. `UpdateAsync` (CAS) always throws when NATS is down.
- **Provisioning** (§3): `Provisioner.EnsureAsync` runs in a background loop with backoff (`NatsCache.Start`). A store that exists with memory storage, History > 1, or no TTL/markers sets `ProvisioningError`, and the cache runs degraded without crashing.

## Deviations and gaps to know about

- **Direct Get uses `INatsJSStream.GetDirectAsync`**, not the KV API, because NATS.Net `NatsKVEntry` exposes no headers and the design keeps metadata in headers. Sequence and time come from the `Nats-Sequence` / `Nats-Time-Stamp` reply headers (nanosecond timestamps are trimmed to 7 digits). A 404 status means absent.
- **Leader reads use `INatsJSStream.GetAsync(StreamMsgGetRequest)`**, which exists in NATS.Net 3.3.0 even though it is missing from the XML docs. Error 10037 means absent.
- **Not implemented yet, by milestone**:
  - Milestone 3: distributed lock, renewal, waiters, early refresh. `EarlyRefreshRatio`, `LockWaitTimeout` and `LeaseTtl` are accepted but unused, apart from the grace and lease math.
  - Milestone 4: invalidation events (publish and consume), the HWM from events, the Open-mode outage journal with leader-read replay, and the StreamInfo / sequence-gap check.
  - Large values: values above `LargeValueThresholdBytes` are returned uncached and counted in `cache.large_values_skipped`. The `_objects` store is provisioned but unused.
  - `HybridCache` / `IDistributedCache` adapters (Q10), including the `d.` key namespace and the AddHybridCache warning.
  - Circuit breaker and resilience pipeline (§8). Today every call retries inside the L2 store, then degrades.
- **Unit-test fake**: `FakeL2Store` models History 1, a global sequence, 10071 with the last sequence in the text, msg-id dedup, TTL → MaxAge marker → empty, DEL tombstones, stale Direct Gets, lost replies and outages. Keep it in sync with real NATS behaviour; anything new should be confirmed in the spike or the integration tests first.

## NATS facts that cost real time (all spike-verified on 2.15.0)

- NATS.Net `PutAsync` / `UpdateAsync` take no TTL, and an `UpdateAsync` turns a TTL'd entry into one that never expires. Every TTL write is a raw publish to `$KV.{bucket}.{key}` with `Nats-TTL`, `Nats-Expected-Last-Subject-Sequence` and `Nats-Msg-Id`.
- NATS.Net `CreateAsync` fails spuriously (2.7–20 %) on uncontended tombstoned keys, because it re-reads through Direct Get. Never use it for locks.
- Direct Get is not read-after-write coherent: about 0.3 % stale idle, about 5 % under lock load. Fences and HWM checks re-read through the leader.
- 10164 = another write to the subject is in flight and nothing was stored: retry the identical publish. 10071 = wrong last sequence, and its text carries the sequence. 10158 = duplicate msg id still being applied. A duplicate ack (`ack.Duplicate` or `NatsJSDuplicateMessageException`) = success at the original sequence.
- An expected-sequence error never coexists with a commit; only a lost reply or timeout leaves the outcome unknown. Resend with the same msg id; a new expected revision needs a new msg id.
- TTL expiry writes a `Nats-Marker-Reason: MaxAge` marker, which expires after `LimitMarkerTTL`. Buckets must be History 1, or short TTLs are silently raised to the marker TTL.
- Never purge, roll up, truncate or compact TTL'd buckets (#8594, fixed only in 2.15.1-RC).
- An ordered consumer started below `FirstSeq` silently skips ahead, and NATS.Net has no recreate callback: detect gaps from `Metadata.Sequence.Stream`.
- Leader reads can fail with `NatsJSApiNoResponseException` during an election; retry them.

## Owner's working agreements

- Update only the local `docs/DESIGN.md`. The online Claude Docs draft was abandoned after v1.0.
- Every design change gets an adversarial review by a subagent on the Fable model. The owner then asks for all findings to be applied, each round is logged in DESIGN.md §13, and a frozen doc gets a version bump.
- NATS: latest stable release only (currently 2.15.0; the pin moves after the spike passes; never an RC). NATS.Net pinned at 3.3.0.
- All NATS stores are file-backed; memory storage is never used.
- Commits go straight to `main` (no branches so far), when the owner asks or after a design freeze.

## Next steps (milestone 3)

1. Port `HelperAcquire` / `HelperRelease` from the spike into `src/NatsDistributedCache/Internal/` as a `DistributedLock` over the `_locks` bucket, on `NatsL2Store`'s publish helper. Include the owner's LeaseTtl and FactoryTimeout in the token, renewal every LeaseTtl / 3, and release before the event.
2. Wire the lock into `NatsCache.LoadAsync` at the marked spot. Waiters: poll L2 and the lock key every 250 ms until milestone 4 adds event wake-ups; bounded wait (owner's FactoryTimeout + LeaseTtl), then FailureMode. Add `FactoryReason.Takeover` when a waiter takes over an expired lease.
3. Early refresh (§6): at 10 % remaining life, take the lock in the background; losing it is a no-op.
4. Tests: unit (extend `FakeL2Store` with a locks bucket) and integration (multiple `NatsCache` instances racing on one key: exactly one factory call; owner crash → takeover after the lease).
5. Run a Fable review of milestone 3 before freezing it, per the working agreement.
