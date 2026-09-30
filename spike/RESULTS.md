# Milestone-1 spike results

Sep 30, 2026. Rig: 3-node NATS cluster in docker compose, file storage on named volumes, R3, 0.5 CPU per node (`cpu_period` 10 ms, `cpu_quota` 5 ms), 1 GB each, `sync_interval: 2m`. Client: NATS.Net 3.3.0 on .NET 10 on the host (loopback, so latencies are a lower bound), `RequestTimeout` 2 s. Buckets: History 1, `LimitMarkerTTL` 2 s (short, to keep the tests fast).

Run it with `./run-spike.sh [image]`. The default is `nats:2.15.0-alpine`, the latest stable release and, from design v1.2, the only supported version. The script refuses release-candidate images, checks the server version via `/varz`, clears old results for the version, and always tears the stack down. Raw results land in `results/` (not committed).

## Current result: NATS 2.15.0, after the review-6 fixes

All 14 checks pass. `lock-throughput-kv` fails by design: it is the evidence for finding 2.

| Check | Result |
| --- | --- |
| TTL → `MaxAge` delete marker; marker expires | Expiry after 3.04 s for a 3 s TTL; watch sees `Put@1` then `Purge@2` (asserted); marker gone after the 2 s marker TTL |
| Fence on markers | Expected 0 on a marker → `10071 wrong last sequence: 4`; expected = marker revision → committed; expected 0 after the marker expired → committed |
| DEL through the helper | Tombstone revision = PubAck sequence; resending the same msg id → duplicate ack at the same sequence; stale fenced delete → 10071 |
| Read latency (p50 / p99) | Direct Get 0.41 / 5.3 ms, leader read 0.42 / 5.3 ms |
| Direct Get staleness | 0.3 % stale right after a write (a lower bound: some reads are answered by the leader); leader read 0 stale |
| NATS.Net `CreateAsync` race, 5 nodes | 300/300 rounds with one winner (baseline only; the winner releases after the losers finish) |
| Helper acquire under contention, 5 nodes × 100 | 500 acquisitions, 0 overlapping ownership intervals (checked on server sequences), 0 errors, 0 starved; 724 transient 10164s retried; wait p50 2.2 ms, p99 298 ms |
| TTL overwrite and renewals | Replacement survives the old 5 s timer; 10 renewals with the lease token keep a 5 s lease alive for 15 s; it expires 5.04 s after the last renewal |
| Ordered consumer below `FirstSeq` | Silently starts at `FirstSeq` (31) with no error; the stream-sequence gap is detectable |
| Helper lock throughput, 16 workers, one key each | 835 cycles/s (about 1 670 lock ops/s), cycle p50 19 ms, p99 47 ms, 0 errors; the busiest node (the leader) at 45–46 % of one CPU, i.e. near its 0.5 CPU quota |
| Leader failover (new): 6 workers, locks stream leader restarted twice | 19 042 cycles, 10 unknown outcomes and 2 duplicate acks all resolved, 0 spurious losses, 0 failed releases, 0 stuck locks; longest cycle 2.3 s (the election); 2 leader reads failed with no-response during an election and were retried by the next cycle |
| Full cluster restart | Data survived; the 45 s lock expired at 45.0 s |
| `CreateAsync` on uncontended keys (evidence) | 19.9 % of cycles failed in this run (2.7–5.3 % in earlier runs) |

## Findings that changed the design

1. **Minimum server version.** On 2.11.2 a TTL'd key written before a full cluster restart stayed live 105 s after its 45 s TTL: the timer was not recovered (fixed in 2.11.10 / 2.12.1, nats-server #7344). This led to v1.1's minimum version; v1.2 now requires the latest stable release only (2.15.0).
2. **NATS.Net `CreateAsync` must not be used for lock acquire.** On keys nobody else touches it fails 2.7–20 % of the time on 2.12.15, 2.14.7 and 2.15.0 with `NatsKVCreateException` or `NatsKVWrongLastRevisionException`. On a tombstoned key it re-reads the entry through Direct Get (NATS.Net 3.3.0 create path), and a lagging replica returns the old lock or revision. The design's helper acquire with a leader read replaces it.
3. **Lock operations saturate 0.5 CPU NATS nodes** at about 1 700–2 000 lock ops/s (850–1 000 cycles/s). This is one closed-loop point; the K3 budget stays provisional until the milestone-6 worker sweep.
4. **10164 is transient.** nats-server 2.14/2.15 return it before proposal while another write to the same subject is in flight, so nothing was stored. The helper retries the identical publish (6 sends, about 124 ms at most); if it is still refused, the outcome is Unknown and is settled with a leader read. Helper contention retried 724 of them with 0 errors.
5. **Unknown outcomes must be handled, and an acquire must recognise its own token.** Corrected in review 6: expected-sequence errors never coexist with a commit. The only way a write commits while the client sees an error is a lost reply, timeout or reconnect. The helper now treats those as Unknown, resends with the same `Nats-Msg-Id` (the server dedups, returning the original sequence), and otherwise settles by a leader read, where finding its own token means it owns the lock. The failover test exercised this path: 10 unknown outcomes and 2 duplicate acks across two leader restarts, all resolved, with 0 stuck locks. The earlier "16 % self-lockout on 2.14.7" was most likely locks stuck after exceptions the old helper did not catch; that code no longer exists.

## Answered, no change needed

- Delete markers, CAS on a marker revision, DEL through the helper, the text of 10071 and msg-id dedup behave as sections 3 and 5 describe.
- The leader read (`INatsJSStream.GetAsync`, which exists in NATS.Net 3.3.0 although it is not in the XML docs) costs about the same as Direct Get at p50.
- Overwrites and renewals of TTL'd messages keep the new TTL on 2.15.0.
- The ordered consumer's silent skip is real, and the section-7 sequence-gap check catches it.

## Earlier runs (before design v1.2)

The same suite, without the review-6 fixes, was run on 2.12.15, 2.14.7 and 2.11.2. The server behaviour matched 2.15.0 except for the 2.11.2 restart failure (finding 1) and throughput: 2.11.2 was about 35 % faster, with about 2 750 lock ops/s and cycle p99 22 ms. These versions are no longer supported or tested.

## Not covered yet

- Chaos faults C1–C8 against the .NET nodes, CPU-limited .NET nodes, and throughput at more than one concurrency level (milestones 5–6).
- Dedup memory on the stream leader at sustained lock rates (about 240 000 msg ids in the 2-minute window at 2 000 lock ops/s).
- The host has 12 logical CPUs (6 cores with SMT). The scaled rig fits; the full-size profile must be measured before its numbers are trusted.
