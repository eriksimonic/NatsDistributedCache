# Milestone-1 spike results

Sep 30, 2026. Rig: 3-node NATS cluster in docker compose, file storage on named volumes, R3, 0.5 CPU per node (`cpu_period` 10 ms, `cpu_quota` 5 ms), 1 GB each, `sync_interval: 2m`. Client: NATS.Net 3.3.0 on .NET 10, running on the host. Buckets: History 1, `LimitMarkerTTL` 2 s (short, to keep the tests fast).

Run it with `./run-spike.sh [image]`. Raw results land in `results/` (not committed).

## Summary

| Assumption (DESIGN.md v1.0) | 2.12.15 | 2.11.2 | Verdict |
| --- | --- | --- | --- |
| Per-key TTL writes a `MaxAge` delete marker on expiry, and the marker expires after `LimitMarkerTTL` | Pass | Pass | Holds. Expiry after 3.1 s for a 3 s TTL; watch sees `Put` then `Purge` |
| Fence: expected 0 fails while a marker exists (10071), expected = marker revision succeeds, expected 0 succeeds once the marker is gone | Pass | Pass | Holds exactly as the review-2/3 fence rule assumes |
| DEL via the publish helper: PubAck sequence = tombstone revision readers see; a stale fenced delete gets 10071 | Pass | Pass | Holds |
| Direct Get vs leader read latency | p50 0.37 / p99 3.1 ms vs p50 0.51 / p99 4.3 ms | p50 0.35 / p99 1.3 ms vs p50 0.48 / p99 1.3 ms | Leader read costs about +0.15 ms at p50; fine for fence and HWM re-reads |
| Direct Get is not read-after-write coherent | 1–5 stale reads in 2 000 (0.05–0.25 %) | 1 in 2 000 | Confirmed on both. Leader read: 0 stale |
| Five nodes racing KV `Create` on one key: exactly one winner | 300/300 rounds | 300/300 rounds | Holds; no zero-winner rounds seen (nats-server #5162 not hit) |
| Ordered consumer asked to start below `FirstSeq` | Silently starts at `FirstSeq` (31), no error | Same | Silent skip confirmed; the design's sequence-gap check is required and works |
| Full cluster restart: data survives, TTL timers resume | Pass (lock gone after 45.2 s for a 45 s TTL) | **Fail**: lock never expired within TTL + 60 s | **2.11.2 is not a valid minimum** |
| Lock throughput, 16 workers, file-backed R3 at 0.5 CPU | about 2 000 lock ops/s, cycle p50 12 ms, p99 40 ms | about 2 750 ops/s, p50 11 ms, p99 22 ms | NATS nodes at 39–51 % of one CPU, i.e. at their 0.5 CPU quota. 2.11.2 was about 35 % faster with half the p99 |

## Findings that change the design

1. **Minimum server version is 2.12.1, not 2.11.2.** On 2.11.2 a TTL'd key written before a full cluster restart never expires afterwards (the timer is not recovered; fixed in 2.12.1, nats-server #7344). With file-backed locks this means a lock held during a restart stays held forever (only the `_locks` MaxAge backstop would clear it). Keep the 2.12.15 pin; make the documented and CI-tested minimum 2.12.1.
2. **NATS.Net `CreateAsync` must not be used for lock acquire.** With each worker using its own key (no contention at all), `CreateAsync` failed 4.4–5.3 % of the time on 2.12.15 and 0.2 % on 2.11.2 (`NatsKVCreateException: Can't create entry` and `NatsKVWrongLastRevisionException`). When the subject already holds a tombstone it re-reads the entry through Direct Get, and a lagging replica returns the old live lock or an old revision. In the library, each such failure would look like "someone else holds the lock", so an idle node would wait for a factory that no one is running. The design's publish helper fixes it: publish with expected 0 → on 10071 leader-read the last message → if it is a tombstone or TTL marker, publish again expecting that sequence. That had **0 errors** in 30 000+ cycles on both versions and was slightly faster.
3. **Lock operations saturate the NATS nodes at 0.5 CPU** at about 2 000 ops/s (1 000 miss cycles/s). This is the ceiling for cold misses on the scaled rig; K3 thresholds should be derived from it.

## Answered, no change needed

- Delete markers, CAS against a marker revision, DEL through the helper and error 10071 behave as sections 3 and 5 describe.
- The leader read is cheap enough to use on every fence and HWM retry.
- The ordered consumer's silent skip is real, and the stream-sequence gap check in section 7 catches it.

## Not covered yet

- Behaviour under the chaos faults (C1–C8) and with the .NET nodes CPU-limited; that is milestone 5+.
- The host has 12 logical CPUs, not 6 as assumed in section 9. The scaled rig still fits; the full-size profile (3 × 1 + 5 × 0.5 = 5.5 CPU plus k6) may fit too, but test that before relying on it.
