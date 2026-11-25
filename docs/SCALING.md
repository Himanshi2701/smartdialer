# Scale: 100 -> 1,000 -> 10,000 agents. What breaks first, and how to fix it

Measured numbers are from `docs/reports/LOAD_TEST.md` (single machine, SQLite). They vary a little between runs and
machines, so they are rounded here; what matters is the *shape*. Production numbers would come from PostgreSQL over
a network, which is slower per transaction.

## Back-of-the-envelope load

At a 20-25% answer rate and ~2.5 min per handled call (talk + wrap), each agent needs roughly
1 answered call per 150 s, i.e. ~4-5 dial attempts per 150 s.

| Agents | Dials / s | Provider events / s (~4 per call, x1.3 duplicates) | DB writes / s (~10 per call) |
|---|---|---|---|
| 100 | ~3 | ~15 | ~30 |
| 1,000 | ~30-40 | ~150-200 | ~400 |
| 10,000 | ~300-400 | ~1,500-2,000 | ~4,000 |
| 100,000 | ~3,000-4,000 | ~20,000 | ~40,000 |

Measured (one pacing worker, predictive):

| Agents | Calls started / s | Tick p50 | Tick p95 | Biggest cost |
|---|---|---|---|---|
| 100 | ~3 | ~2 ms | ~5 ms | nothing |
| 1,000 | ~40 | ~20-25 ms | ~130 ms | allocation + safety math |
| 10,000 | ~370 | ~65-80 ms | ~300-400 ms | allocation (p95 ~250-350 ms) |

## What breaks, in order

### 1. Serial call setup inside one pacing loop  (breaks first: ~1,000-10,000 agents)

Each call is ~4 small transactions plus one **synchronous provider API call**. In the prototype the provider is
in-process, so ~370 calls/s still fit in a tick (allocation p95 ~250-350 ms). With a real provider each `place_call` is an HTTPS
request of ~100-300 ms: 400 calls x 200 ms = **80 s of work per 1-second tick**. The loop falls behind, so the
snapshot the pacer used is stale by the time calls go out - which also erodes safety margins.

**Fix:** split *decide* from *dial*. The pacing leader only commits intents (calls in RESERVED) in one batched
transaction; a pool of async dispatchers (per-provider concurrency and calls-per-second limits) does the HTTP calls.
The intent-first design already supports this - dispatchers are just more "allocator step 2-4" workers, and the
sweeper already handles one that dies.

### 2. Hot rows in allocation  (~1,000+ agents, many workers)

Every worker asks for "the longest-idle AVAILABLE agent" and "the highest-priority PENDING borrower" - the *same*
row. One wins, the others retry. The load test shows it: 8 processes over 5 agents made **~400,000 empty picks**
for ~16,000 successful reservations (still 0 double bookings - correctness holds, throughput doesn't).

**Fix:** `SELECT ... FOR UPDATE SKIP LOCKED LIMIT k` (PostgreSQL) so concurrent workers take *different* rows;
batch-reserve k agents and k borrowers in one statement with `RETURNING`; and partition agents into **pools**
(team/skill/language) with one pacing leader per pool, so contention is per pool, not global.

### 3. Single-writer database  (~10,000 agents)

~4,000 writes/s of small transactions is where one SQLite file stops (single writer) and where a single PostgreSQL
primary starts to need care (WAL volume, fsync, autovacuum on the hot `agents` and `calls` tables).

**Fix, in order of cost:** PostgreSQL with batched commits and `synchronous_commit` tuned for the inbox;
partition `calls`/`provider_events` by day and archive; then **shard by campaign/pool**. Sharding is natural here:
a call involves one agent and one borrower of the same pool, so there are **no cross-shard transactions**.

### 4. Snapshot queries that scan history  (~10,000 agents, long campaigns)

The snapshot does `COUNT(*)` over recent calls and agent transitions. As tables grow these get slower
(10k agents: ~10 ms p50 and rising with campaign length).

**Fix:** maintain per-pool counters (in-flight, waiting, answered/attempted per minute bucket) in the same
transaction as each transition, so a snapshot is O(1) reads.

### 5. Event ingestion  (~10,000 -> 100,000 agents)

Measured in-process: roughly 100k+ events/s ingest and ~15k events/s apply. At 100k agents (~20k events/s with duplicates)
the apply side is the limit, and ordering per call matters.

**Fix:** partition the inbox by `hash(call_id)` with one consumer per partition (keeps per-call order, parallel across
calls). This is the point where a log like Kafka starts paying for itself - not before.

### 6. Safety math per pool  (very large pools)

Exact Poisson-binomial is O(C) per call added, i.e. O(C x N) per tick. At C in the thousands with Python this is
tens of ms. **Fix:** normal approximation with a continuity correction above C ~ 500 (the error is tiny there), or
smaller pools.

### 7. The real world: telecom limits

Providers and carriers limit calls per second per trunk and per caller ID, and regulators may limit how often one
number can be called. At 10k agents you need several trunks/providers. The Safety Controller's per-tick cap
(`max_new_calls_per_tick`) and the router's per-provider breakers are where those limits plug in.

## "We just went from 1,000 to 100,000 agents. What breaks first?"

The single pacing loop doing call setup serially, then hot-row contention, then the single database primary.
The answer is not "add servers": it is *decide in batches, dial asynchronously, partition by agent pool so each
partition is independent*, and only then add machines - one per partition.

One caution: smaller pools are *less* efficient for predictive dialing. The binomial bound is tighter (relatively)
for big pools - 1,000 agents can run much closer to the limit than 10 pools of 100. So pools should be as large as
the database partition allows, not smaller.
