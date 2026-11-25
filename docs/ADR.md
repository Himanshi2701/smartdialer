# Architecture Decision Record (short)

Format for each decision: **what I chose - why - what problem it solves - what it makes harder.**

---

## ADR-1: One transactional database is the single source of truth

* **Chose:** All shared state (agents, borrowers, calls, inbox, outbox, permits, safety log) in one relational DB.
  SQLite (WAL mode) in the prototype; PostgreSQL in production. Workers are stateless.
* **Why:** The hard part of this problem is *correctness under concurrency* (no double-booked agent, no
  double-dialled borrower, consistent state after crashes). A relational DB gives atomic multi-row transactions,
  conditional updates and unique constraints for free. One store means there is never a "which copy is right?"
  question.
* **Solves:** agent/borrower allocation races, crash recovery (nothing lives in worker memory), auditability (every
  transition is a row).
* **Harder:** a single write path is the first scaling limit (see SCALING.md). SQLite allows one writer at a time -
  fine for a prototype and for proving correctness, not for production write volume.
* **Rejected:**
  * *Redis* for agent state - fast, but atomic multi-key logic needs Lua scripts, and durability/failover semantics
    are weaker; I would then need the DB anyway for calls, so I'd have two sources of truth.
  * *Kafka* for events - solves fan-out and replay at very high volume, but adds ordering/partitioning/consumer-offset
    problems that a DB inbox table solves more simply at this scale. It becomes reasonable around the 100k-agent
    mark, partitioned by call id.
  * *Microservices* - the pipeline stages have to share one consistent view of agents and calls; splitting them
    into services would add network hops and distributed transactions without buying anything at this size.

## ADR-2: Compare-and-set + leases, not distributed locks

* **Chose:** Every state change is `UPDATE ... WHERE id=? AND state=<expected>` (optionally `AND version=?`).
  Ownership of in-progress work is a lease (`lease_owner`, `lease_until`) on the row.
* **Why:** A lock held by a crashed worker blocks everyone until something notices; a lease simply expires. CAS
  never blocks across a network call (the provider API call happens *outside* any transaction).
* **Solves:** two workers reserving the same agent; crashed workers holding agents; stale workers acting late
  (their CAS fails because the row moved on; the pacing lease carries a fencing epoch).
* **Harder:** a lost race means retrying; under heavy contention on the same rows (everyone wants the longest-idle
  agent) throughput falls. Fix: `FOR UPDATE SKIP LOCKED` and agent pools (SCALING.md).

## ADR-3: Inbox and outbox tables

* **Chose:** Provider webhooks are inserted into `provider_events` with `UNIQUE(provider, event_id)` before being
  acknowledged, then applied idempotently in one transaction that also marks them processed. Commands to the
  provider (BRIDGE, HANGUP) are written to `outbox` in the *same* transaction as the state change that needs them,
  then delivered at-least-once; provider commands are idempotent.
* **Why:** "Don't assume external systems behave nicely": duplicates, re-ordering, and crashes between "received"
  and "applied" or between "decided" and "told the provider" are the normal case.
* **Solves:** duplicate events, crash after ANSWERED, crash after binding an agent but before bridging.
* **Harder:** more tables and a dispatcher loop; at-least-once means every command must be idempotent.

## ADR-4: The safety boundary is enforced by a capability, not by convention

* **Chose:** The pacer returns a `Proposal`. Only the `SafetyController` can create a `Permit`, signed with an HMAC
  key generated inside it. The allocator gets a verify-only object. Permits are single use (DB primary key) and
  expire after 5 s. The predictive cap is re-checked inside the transaction that creates each call.
* **Why:** "The predictive algorithm should not have a way to simply switch the safety mechanism off."
  There is no flag to switch off; a forged, tampered, replayed or expired permit is refused (tests in
  `test_safety.py`).
* **Harder / honest limit:** Python cannot truly hide a private attribute - code running in the same process could
  dig the key out with reflection. The production version runs the Safety Controller + Allocator as the only
  component holding provider credentials (separate process/service account), so the pacer physically has no path
  to the telecom API.

## ADR-5: Predictive calls are bound to an agent at answer time, with a 2-second hold

* **Chose:** Progressive calls carry their agent from the start (the provider bridges directly). Predictive calls are
  unbound; when ANSWERED arrives, a CAS grabs the longest-idle free agent. If none is free the borrower waits up to
  2 s; after that the call is ABANDONED, counted, hung up, and that borrower isn't re-dialled for 72 h.
* **Why:** Late binding is what gives predictive its benefit (any agent can take any answer). The hold turns a
  near-miss (agent frees up 1 s later) into a connected call.
* **Harder:** binding depends on webhook latency. Found in simulation: Provider B delivers webhooks up to ~4 s late,
  so by the time we bound an agent the provider had already given up. Fix: the router measures each provider's
  webhook lag and **predictive calls only go to providers with p95 lag <= 1.5 s**; if none qualifies the campaign
  is progressive. Progressive is immune because the provider bridges the pre-assigned agent itself.

## ADR-6: Python + a simulated clock

* **Chose:** Python 3.11+ standard library only (pytest/matplotlib for tests and charts). A `Clock` interface with
  `SimClock` so a 30-minute campaign runs in ~3 s and is reproducible from a seed.
* **Why:** readable for reviewers; easy to run (`pip install -r requirements.txt`); a deterministic simulator makes
  failure cases repeatable, which matters more here than raw speed.
* **Harder:** the GIL and SQLite limit absolute throughput; the 1-second simulation tick adds up to 1 s of
  webhook latency (accounted for in the 1.5 s lag threshold).

---

## Open risks / what I'm least confident about

1. **In-flight exposure during a sudden provider outage.** If a provider's webhooks suddenly slow down, calls that
   are *already ringing* can be answered after the provider's no-agent fallback. Scenario E shows 7 abandons out of
   426 answers this way. It is bounded (no new predictive calls once the lag/breaker reacts, and the in-flight count
   is capped at 3 per unit of capacity) but it is not zero. Better: per-provider lag measured over a short window,
   and a provider-side "answer webhook must be acknowledged within X ms or play the message" contract.
2. **Per-tick epsilon compounds.** P(overflow) <= 1% per decision is not 1% per hour. The abandon budget (R3b) is
   what bounds the hourly rate; the per-tick bound keeps it rare. A proper fix is to budget epsilon over a window.
3. **Statistical model assumptions.** Independence of answers (a whole region's network can fail together), and
   exponential talk times. The upper bounds and the hard caps are what protect against the model being wrong.
4. **Provider without lookup-by-idempotency-key** (Plivo has no client key): crash reconciliation then relies on
   webhooks carrying our call id plus ring timeouts.
5. **Breaker state is per worker** (in memory). Workers can briefly disagree on provider health; acceptable because
   breakers only affect *new* calls and every worker converges within its window. Could move to the DB.

## With another week

* PostgreSQL + `SKIP LOCKED`, batch allocation (reserve K agents and K borrowers in one statement) and an async
  provider dispatcher - the top items in SCALING.md.
* Run Plivo end-to-end with a real number and agent SIP endpoints; replace the BRIDGE command with the synchronous
  answer-XML bind.
* Calling-hours window per borrower time zone and DNC lists inside the Safety Controller.
* Property-based tests (Hypothesis) that generate random event orders/duplicates/crashes and assert invariants.
* Epsilon budgeting over a rolling window and a dashboard reading `safety_log`.

---

## Final question

> *How would you build a SmartDialer that gets as much of the utilization benefit of predictive dialing as possible,
> while retaining the deterministic safety characteristics of progressive dialing?*

**Separate "how many calls would be nice" from "how many calls are safe", and make the safe answer the only one that
can reach the phone network.**

1. **Deterministic core.** Every guarantee that progressive gives is enforced by the database, not by the pacing
   math: one active call per agent, one per borrower, every reservation a compare-and-set with a lease, intent
   persisted before dialing, events idempotent and monotonic. These hold in every mode.
2. **Predictive only spends a bounded, measured risk.** The pacer proposes; the Safety Controller approves only as
   many unbound calls as keep `P(more answers than free agents) <= 1%`, using a *pessimistic* answer rate (upper
   bound of the short and long windows), *age-aware* probabilities for calls already ringing, and counting as
   capacity only agents that are free now or whose forced wrap-up ends before an answer can arrive.
3. **Hard, deterministic fallbacks.** Abandon budget used, agents dropping, slow webhooks, unhealthy provider, not
   enough data -> the controller switches to progressive, which is safe even if every ringing call answers.
   Calls that became unsafe are cancelled *before* they are answered.
4. **The boundary is a capability.** Only signed, single-use permits move calls; the predictive code has no path
   to the provider and no switch to turn safety off.

The result in simulation (with modelled borrowers and providers, not real calls): no abandons in scenarios A-D,
progressive behaviour whenever conditions are uncertain, and between +2 and +25 points of agent utilization
(43% -> 68% at a 20% answer rate, 83% -> 85% at 70%).
