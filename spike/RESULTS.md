# Milestone-1 spike results

Sep 30, 2026. Rig: 3-node NATS cluster in docker compose, file storage on named volumes, R3, 0.5 CPU per node (`cpu_period` 10 ms, `cpu_quota` 5 ms), 1 GB each, `sync_interval: 2m`. Client: NATS.Net 3.3.0 on .NET 10, running on the host (loopback, so latencies are a lower bound). Buckets: History 1, `LimitMarkerTTL` 2 s (short, to keep the tests fast).

Run it with `./run-spike.sh [image]` (default: `nats:2.15.0-alpine`, the latest stable release and, from design v1.2, the only supported version). Raw results land in `results/` (not committed). Final runs: 2.12.15 and 2.14.7, all tests pass; `lock-throughput-kv` is kept as evidence and is expected to fail. 2.11.2 was run once and is no longer supported (finding 1).

## NATS 2.15.0 (required version from design v1.2)

All checks pass: markers (expiry 3.1 s), CAS on markers, helper DEL, leader read p50 0.54 ms vs Direct Get 0.38 ms, Direct Get 0.2 % stale idle, helper contention (500 acquisitions, max 1 owner, 0 errors, wait p99 239 ms), TTL overwrite and renewals, ordered-consumer silent skip, restart (lock gone after 45.2 s, data survived). Helper lock throughput about 1 690 ops/s (cycle p50 19 ms, p99 40 ms), leader at 49 % of one CPU (its quota). `CreateAsync` still fails 2.8 % on uncontended keys.

## Summary (earlier versions)

| Assumption (DESIGN.md) | 2.12.15 | 2.14.7 | 2.11.2 (first run) | Verdict |
| --- | --- | --- | --- | --- |
| Per-key TTL writes a `MaxAge` delete marker on expiry; the marker expires after `LimitMarkerTTL` | Pass | Pass | Pass | Holds. Expiry after 3.1 s for a 3 s TTL; watch sees `Put` then `Purge` |
| Fence: expected 0 fails on a marker (10071), expected = marker revision succeeds, expected 0 succeeds once the marker is gone | Pass | Pass | Pass | Holds as the fence rule assumes |
| DEL via the publish helper: PubAck sequence = tombstone revision; a stale fenced delete gets 10071 | Pass | Pass | Pass | Holds |
| Direct Get vs leader read latency (p50 / p99) | 0.39 / 5.7 ms vs 0.36 / 4.6 ms | 0.37 / 2.9 ms vs 0.50 / 4.3 ms | 0.35 / 1.3 ms vs 0.48 / 1.3 ms | Leader read costs at most about +0.15 ms at p50 |
| Direct Get is not read-after-write coherent | 0.05–0.45 % stale (idle) | 0.2–0.45 % | 0.05 % | Confirmed. Under lock load about 5 % (the `CreateAsync` failures below are that stale read). Leader read: 0 stale |
| Five nodes racing NATS.Net `Create` on one key | 300/300 one winner | 300/300 | 300/300 | Holds, but the winner released after all losers finished, so this does not test #5162 |
| Helper acquire under contention: 5 nodes × 100 acquisitions, release racing the losers | 0 errors, max 1 owner, 0 starved | 0 errors, max 1 owner, 0 starved | not run | Holds after two fixes (findings 4 and 5). Acquire wait p50 3–4 ms, p99 200–270 ms |
| Overwriting a TTL'd message; lease renewals | Pass | Pass | not run | The replacement survives the old 5 s timer; 10 renewals keep a 5 s lease alive for 15 s; it expires 5.1 s after the last renewal |
| Ordered consumer asked to start below `FirstSeq` | Silently starts at `FirstSeq` | Same | Same | Silent skip confirmed; the sequence-gap check in section 7 is required and works |
| Full cluster restart: data survives, TTL timers resume | Pass (gone after 45.2 s for 45 s) | Pass (45.2 s) | **Fail**: never expired within TTL + 60 s | Minimum version 2.12.1 (finding 1) |
| Lock throughput, helper acquire, 16 workers, one key each | about 2 000 lock ops/s, cycle p50 15 ms, p99 39 ms | about 1 730 ops/s, p50 19 ms, p99 42 ms | about 2 760 ops/s, p50 11 ms, p99 22 ms | One closed-loop point, CPU sampled during the helper run: the busiest node (the leader) is at 49 % of one CPU, i.e. its 0.5 CPU quota. Provisional; a worker sweep replaces it before milestone 6 |

## Findings that changed the design (v1.1)

1. **Minimum server version is 2.12.1, not 2.11.2.** On 2.11.2 a TTL'd key written before a full cluster restart stayed live for 105 s after its 45 s TTL. The timer is not recovered; this was fixed in 2.11.10 and 2.12.1 (nats-server #7344). A lock held during a restart would never expire. Pin 2.12.15; CI also tests 2.12.1 and the latest 2.14.x.
2. **NATS.Net `CreateAsync` must not be used for lock acquire.** On keys nobody else touches it failed 3.8–5.3 % of the time on 2.12.15 and 2.14.7 (0.2 % on 2.11.2) with `NatsKVCreateException` or `NatsKVWrongLastRevisionException`. When the subject holds a tombstone it re-reads the entry through Direct Get (NATS.Net 3.3.0 `NatsKVStore` create path), and a lagging replica returns the old live lock or an old revision. The design's publish helper with a leader read replaces it.
3. **Lock operations saturate 0.5 CPU NATS nodes** at about 1 700–2 000 ops/s (850–1 000 cold-miss cycles/s). K3's budget is derived from this, and it stays provisional.
4. **Error 10164 is transient and must be retried** (found by the helper tests). On back-to-back writes to one subject (acquire → release), 2.14.7 answers "wrong last sequence" with code 10164 and no sequence: the leader has not applied the previous write yet. Before the fix, one failed release left a live lock that blocked its key for the full LeaseTtl. The helper now retries the identical publish (same expected revision, same `Nats-Msg-Id`) up to 5 times with 2–64 ms backoff; 10071 is never retried blindly.
5. **An acquire must recognise its own token.** A publish can commit while the client sees an error (lost ack or resend). Without a unique token per acquire, the node saw its own lock as "held by someone" and locked itself out for LeaseTtl: 16 % failed cycles on 2.14.7. With a fresh token and `Nats-Msg-Id` per attempt, and "live token = mine → owned", failures dropped to 0.

## Answered, no change needed

- Delete markers, CAS against a marker revision, DEL through the helper and the text of error 10071 behave as sections 3 and 5 describe.
- The leader read is cheap enough for every fence, HWM and acquire retry.
- Overwrites and renewals of TTL'd messages keep the new TTL; the old timer does not fire on the replacement.
- The ordered consumer's silent skip is real, and the stream-sequence gap check catches it.

## Not covered yet

- Chaos faults (C1–C8), CPU-limited .NET nodes, and throughput at more than one concurrency level (milestones 5–6).
- The host has 12 logical CPUs (6 cores with SMT). The scaled rig fits; the full-size profile must be measured before its numbers are trusted.
