# SmartDialer

A working prototype of an outbound dialer for collections that supports **progressive** and **predictive** pacing,
with a **Safety Controller** between the pacing logic and the telecom provider that the predictive algorithm cannot
bypass.

```
Campaign > Pacing Engine (Progressive / Predictive) > Safety Controller > Call Allocator > Telecom Provider
```

**Headline results** (50 agents, 30 simulated minutes, messy provider as failover; full report in
[`docs/reports/SIMULATION_REPORT.md`](docs/reports/SIMULATION_REPORT.md)):

| Scenario | Progressive utilization | Predictive utilization | Predictive abandon rate |
|---|---|---|---|
| A: 20% answer, 120 s talk | 43.0% | **67.6%** | 0.00% |
| B: 50% answer, 90 s talk | 62.8% | **73.2%** | 0.00% |
| C: 70% answer, 180 s talk | 82.6% | **85.0%** | 0.00% |
| D: changing answer rate and talk time | 47.2% | **66.6%** | 0.00% |

Zero invariant violations (no agent or borrower ever on two calls) in every simulation, test and load test.

---

## Quick start

Requirements: **Python 3.11+**. No database server, no Docker - the prototype uses SQLite from the standard library.

```bash
git clone <this repo> && cd smartdialer
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt      # pytest + matplotlib (charts only)

python -m pytest                     # 51 tests, ~5 s
python sim/run_scenarios.py          # all scenarios, ~1 min  -> docs/reports/SIMULATION_REPORT.md + charts
python sim/failure_demos.py          # narrated failure cases -> docs/reports/FAILURE_DEMOS.md
python loadtest/loadtest.py          # load test, ~40 s       -> docs/reports/LOAD_TEST.md
```

`--quick` is available on `run_scenarios.py` and `loadtest.py` for shorter runs.

Run one custom simulation from Python:

```python
from sim.engine import Scenario, Simulation
s = Simulation(Scenario("mine", answer_rate=0.3, talk_mean_s=100, mode="PREDICTIVE", agents=40, duration_s=900)).run()
print(s["utilization"], s["abandon_rate"], s["safety"])
```

---

## What is where

| Deliverable asked for | Where |
|---|---|
| Working source code | `smartdialer/` |
| README with setup instructions | this file |
| Architecture diagram | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) (Mermaid + ASCII) |
| Agent state machine | [`docs/STATE_MACHINES.md`](docs/STATE_MACHINES.md), code: `smartdialer/states.py` |
| Call state machine | same |
| Progressive Dialer | `smartdialer/pacing.py` (`ProgressivePacer`) + `smartdialer/allocator.py` |
| Predictive Pacing Engine | `smartdialer/pacing.py` (`PredictivePacer`), explained in [`docs/PACING_AND_SAFETY.md`](docs/PACING_AND_SAFETY.md) |
| Safety Controller | `smartdialer/safety.py` |
| Mock telecom providers | `smartdialer/providers/mock.py` (A: fast/reliable, B: slow/timeouts/duplicates/out-of-order), interface `providers/base.py`, breaker/router `providers/health.py`, optional real adapter `providers/plivo.py` |
| Tests | `tests/` (51 tests) |
| Basic simulation | `sim/engine.py`, `sim/run_scenarios.py`, `sim/failure_demos.py` |
| Basic load test | `loadtest/loadtest.py` -> [`docs/reports/LOAD_TEST.md`](docs/reports/LOAD_TEST.md) |
| Short architecture decision document | [`docs/ADR.md`](docs/ADR.md) |
| Scale analysis | [`docs/SCALING.md`](docs/SCALING.md) |
| Answer to the final question | end of [`docs/ADR.md`](docs/ADR.md) and below |

```
smartdialer/
  states.py       agent / call / borrower state machines (transition tables + monotonic call rank)
  store.py        SQLite schema, transactions, compare-and-set, DB-enforced invariants
  lifecycle.py    multi-entity transitions in one transaction (call end, bind on answer, agent disappears)
  snapshot.py     frozen read-only campaign view for pacer and safety
  pacing.py       ProgressivePacer, PredictivePacer -> Proposal
  stats.py        age-aware answer probability, Poisson-binomial overflow
  safety.py       SafetyController -> Decision + HMAC-signed single-use Permit
  allocator.py    CallAllocator: verifies permit, reserves agent+borrower, intent first, calls provider
  events.py       inbox (dedupe) + idempotent apply, hold manager, outbox dispatcher
  recovery.py     lease expiry -> roll back or reconcile with provider.lookup()
  worker.py       DialerWorker: one tick of everything
  providers/      interface, mock A & B, circuit breaker + router, Plivo adapter
sim/              simulator, scenario runner, failure demos
loadtest/         load test
tests/            pytest suite
docs/             architecture, state machines, pacing & safety, ADR, scaling, generated reports
```

---

## How it works, in one page

**Single source of truth.** All state lives in one transactional database (SQLite here, PostgreSQL in production).
Workers are stateless; run as many as you like.

**No double booking.** Reserving an agent is `UPDATE agents SET state='RESERVED' ... WHERE id=? AND state='AVAILABLE'`.
If two workers race, the database applies one update; the other matches 0 rows and moves on. Partial unique indexes
make "one active call per agent" and "one active call per borrower" impossible to violate even with buggy code.

**Progressive:** reserve agent (CAS) -> claim borrower (CAS) -> persist call intent -> `place_call` with an
idempotency key. With N available agents, at most N agent-bound calls exist.

**Predictive:** the pacer estimates how many calls to start so agents are busy when people answer. The Safety
Controller then allows only as many as keep `P(more answers than free agents) <= 1%`, using a pessimistic answer rate
and age-aware probabilities for calls already ringing. The agent is bound when the borrower answers; if no agent is
free within 2 s the call counts as abandoned.

**Safety Controller** can approve, reduce, reject, fall back to progressive, or cancel ("shed") ringing calls before
they are answered. It falls back automatically on: agent drop, abandon budget, slow provider webhooks, unhealthy
providers, cold start. Its permits are HMAC-signed, single use and expire in 5 s; the allocator refuses anything else.
The pacer has no reference to the allocator or any provider.

**Bad providers are expected.** Webhooks go into an inbox with a unique key (duplicates dropped), then are applied
through a state machine that only moves forward (out-of-order events become no-ops). Commands to providers go through
an outbox (at-least-once, idempotent). Timeouts are reconciled with `lookup(idempotency_key)`, never blindly retried.

**Crashes are expected.** Every in-progress item has a lease. When a worker dies, its leases expire and any other
worker's sweeper rolls the work back (release agent, reschedule borrower) or forward (the provider says the call
exists - adopt it).

---

## Final question

> How would you build a SmartDialer that gets as much of the utilization benefit of predictive dialing as possible,
> while retaining the deterministic safety characteristics of progressive dialing?

Separate "how many calls would be nice" from "how many calls are safe", and make the safe number the only one that
can reach the phone network:

1. Keep progressive's guarantees in the **database**, not in the pacing math - CAS reservations, one active call per
   agent/borrower, intent before dialing, idempotent monotonic events. They hold in every mode.
2. Let predictive spend only a **bounded, measured risk**: unbound calls only up to `P(overflow) <= 1%`, computed with
   a pessimistic answer rate, age-aware ringing probabilities, and only capacity that is certain (free now, or forced
   wrap-up ending before an answer can arrive).
3. **Fall back to progressive deterministically** the moment the assumptions are in doubt (abandons, agent drops, slow
   webhooks, unhealthy provider, too little data), and cancel unsafe calls *before* they are answered.
4. Make the boundary a **capability** (signed single-use permits), so the predictive code has no path around it.

Result: zero abandons in progressive and in every uncertain condition, and between +2 and +25 points of utilization in
the simulated scenarios (biggest gain at low answer rates).

---

## Known limitations

See "Open risks" in [`docs/ADR.md`](docs/ADR.md). In short: a provider outage can still strand calls that were
already ringing (bounded, seen in scenario E: 7 of 426 answered calls); the 1% bound is per decision, the abandon
budget bounds the rate over time; SQLite is a single writer (PostgreSQL for production); the Plivo adapter is
written against the docs but not exercised end to end.

## Use of AI tools

Built with help from an AI coding assistant, which the assignment explicitly allows.

## What is real and what is simulated

To be clear about what the numbers in this repo mean:

- **Real:** the code, the database transactions and constraints, the concurrency tests (real threads and real OS
  processes on a real SQLite file), and the load test.
- **Simulated:** the telecom providers and the borrowers. Answer rates, ring times, talk times and provider
  misbehaviour come from mock models I wrote (`providers/mock.py`). No real phone call was placed.
- So the utilization and abandon figures show how the *logic* behaves under those modelled conditions. They are
  not predictions of real-world performance; real borrower behaviour would need real data.
- The Plivo adapter is a sketch written from Plivo's documentation and has not been run against a real account.
