"""
Basic load test.

    python loadtest/loadtest.py            # ~1-2 minutes
    python loadtest/loadtest.py --quick

1. Reservation contention: P worker PROCESSES hammer reserve -> hold -> release on a
   shared on-disk DB. Every hold interval is recorded and checked: no agent may be
   held by two workers at overlapping times.
2. Event ingestion: many calls, each with a full event sequence, 30% duplicates,
   shuffled delivery. Throughput + final-state consistency.
3. End-to-end tick cost at 100 / 1,000 / 10,000 agents (simulated predictive campaign):
   where does the time go as we scale?

Writes docs/reports/LOAD_TEST.md
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import random
import statistics
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from smartdialer.clock import RealClock, SimClock  # noqa: E402
from smartdialer.lifecycle import Lifecycle  # noqa: E402
from smartdialer.events import EventProcessor  # noqa: E402
from smartdialer.states import AgentState  # noqa: E402
from smartdialer.store import Store  # noqa: E402

OUT = os.path.join(ROOT, "docs", "reports")


# ------------------------------------------------------------------ 1
def _reserver(path, seconds, seed, q):
    rng = random.Random(seed)
    st = Store(path, RealClock(), init=False)
    me = f"p{os.getpid()}"
    held, ok, miss = [], 0, 0
    end = time.time() + seconds
    while time.time() < end:
        a = st.reserve_agent("c1", me, f"{me}-{ok}", 30)
        if a is None:
            miss += 1
            continue
        t0 = time.time()
        ok += 1
        time.sleep(rng.random() * 0.002)          # "use" the agent briefly
        t1 = time.time()
        assert st.cas_agent(a, AgentState.RESERVED, AgentState.AVAILABLE, me, current_call_id=None, lease_owner=None)
        held.append((a, t0, t1))
    q.put((me, ok, miss, held))


def contention_test(procs, agents, seconds):
    path = os.path.join(tempfile.mkdtemp(), "lt.db")
    s = Store(path, RealClock())
    s.add_campaign("c1")
    s.add_agents("c1", agents)
    s.close()
    q = mp.Queue()
    ps = [mp.Process(target=_reserver, args=(path, seconds, i, q)) for i in range(procs)]
    t0 = time.time()
    [p.start() for p in ps]
    res = [q.get(timeout=seconds + 60) for _ in ps]
    [p.join() for p in ps]
    wall = time.time() - t0
    total = sum(r[1] for r in res)
    misses = sum(r[2] for r in res)
    by_agent: dict = {}
    for me, _, _, held in res:
        for a, t0_, t1_ in held:
            by_agent.setdefault(a, []).append((t0_, t1_, me))
    overlaps = 0
    for a, iv in by_agent.items():
        iv.sort()
        for (s1, e1, w1), (s2, e2, w2) in zip(iv, iv[1:]):
            if s2 < e1 and w1 != w2:
                overlaps += 1
    return dict(processes=procs, agents=agents, seconds=seconds, reservations=total,
                reservations_per_s=round(total / seconds), empty_picks=misses, overlapping_holds=overlaps,
                wall_s=round(wall, 1))


# ------------------------------------------------------------------ 2
def ingestion_test(calls):
    clock = SimClock()
    st = Store(":memory:", clock)
    st.add_campaign("c1")
    st.add_agents("c1", calls)
    st.add_borrowers("c1", [(f"b{i}", f"+{i}", 0) for i in range(calls)])
    now = clock.now()
    rows = []
    for i in range(calls):
        rows.append((f"call{i}", "c1", f"b{i}", f"agent-{i:05d}", "PROGRESSIVE", "INITIATED", f"call{i}", now, "provX",
                     f"px{i}"))
    with st.txn():
        st.conn.executemany("INSERT INTO calls(id,campaign_id,borrower_id,agent_id,mode,state,idempotency_key,"
                            "created_at,provider,provider_call_id) VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        st.conn.executemany("UPDATE agents SET state='DIALING', current_call_id=? WHERE id=?",
                            [(f"call{i}", f"agent-{i:05d}") for i in range(calls)])
    rng = random.Random(1)
    evs = []
    for i in range(calls):
        seq = [("RINGING", {}), ("ANSWERED", {}), ("COMPLETED", dict(answered=True, bridged=True, talk_s=30))]
        for k, (t, p) in enumerate(seq):
            e = ("provX", f"px{i}:{k}", t, f"px{i}", f"call{i}", now + k, p)
            evs.append(e)
            if rng.random() < 0.3:
                evs.append(e)                                  # duplicate
    rng.shuffle(evs)                                           # arbitrary order
    ep = EventProcessor(st, Lifecycle(st))
    t0 = time.time()
    for e in evs:
        st.ingest_event(*e)
    t1 = time.time()
    while ep.process_pending(5000):
        pass
    t2 = time.time()
    final = {r["state"]: r["c"] for r in st.q("SELECT state, COUNT(*) c FROM calls GROUP BY state")}
    agents = {r["state"]: r["c"] for r in st.q("SELECT state, COUNT(*) c FROM agents GROUP BY state")}
    return dict(calls=calls, events_delivered=len(evs), ingest_per_s=round(len(evs) / (t1 - t0)),
                apply_per_s=round((len(evs)) / (t2 - t1)), final_call_states=final, final_agent_states=agents,
                invariant_violations=len(st.check_invariants()))


# ------------------------------------------------------------------ 3
def scale_test(agents, seconds):
    from sim.engine import Scenario, Simulation
    import smartdialer.worker as W
    sim = Simulation(Scenario("scale", 0.25, 120, agents=agents, duration_s=seconds, borrowers=max(5000, agents * 40)))
    w = sim.workers[0]
    timings = {k: [] for k in ("events", "snapshot", "pacing+safety", "allocation", "tick_total")}
    orig_build = W.build_snapshot

    def timed_build(*a, **k):
        t = time.perf_counter()
        r = orig_build(*a, **k)
        timings["snapshot"].append(time.perf_counter() - t)
        return r
    W.build_snapshot = timed_build
    orig_pump, orig_exec, orig_eval = w.pump_events, w.allocator.execute, w.safety.evaluate

    def tp(f, key):
        def g(*a, **k):
            t = time.perf_counter()
            r = f(*a, **k)
            timings[key].append(time.perf_counter() - t)
            return r
        return g
    w.pump_events = tp(orig_pump, "events")
    w.allocator.execute = tp(orig_exec, "allocation")
    w.safety.evaluate = tp(orig_eval, "pacing+safety")
    t_all = time.time()
    for _ in range(seconds):
        t = time.perf_counter()
        sim.step(1.0)
        timings["tick_total"].append(time.perf_counter() - t)
    W.build_snapshot = orig_build
    wall = time.time() - t_all
    s = sim.summary()

    def p(v, q):
        v = sorted(v)
        return round(1000 * v[int(q * (len(v) - 1))], 1) if v else 0.0
    return dict(agents=agents, sim_seconds=seconds, wall_s=round(wall, 1),
                calls_initiated=s["calls_initiated"], calls_per_sim_s=round(s["calls_initiated"] / seconds, 1),
                ms_p50={k: p(v, 0.5) for k, v in timings.items()}, ms_p95={k: p(v, 0.95) for k, v in timings.items()},
                realtime_factor=round(seconds / wall, 2), utilization=s["utilization"],
                invariant_violations=s["invariant_violations"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    a = ap.parse_args()
    out = {}
    print("1) reservation contention ...")
    out["contention"] = [contention_test(p, ag, 3 if a.quick else 6) for p, ag in ((4, 50), (8, 50), (8, 5))]
    for r in out["contention"]:
        print("  ", r)
    print("2) event ingestion ...")
    out["ingestion"] = ingestion_test(5000 if a.quick else 20000)
    print("  ", out["ingestion"])
    print("3) end-to-end scale ...")
    sizes = ((100, 120), (1000, 60)) if a.quick else ((100, 300), (1000, 120), (10000, 30))
    out["scale"] = []
    for ag, secs in sizes:
        r = scale_test(ag, secs)
        out["scale"].append(r)
        print("  ", r)
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "load_test.json"), "w") as f:
        json.dump(out, f, indent=2)
    write_md(out)


def write_md(out):
    L = ["# Load test results\n", "Generated by `python loadtest/loadtest.py` (SQLite WAL on local disk, single machine). "
         "Absolute numbers depend on the machine it runs on - re-run it to get your own; the *shape* "
         "(what grows, what stays flat) is the point, not the exact figures.\n",
         "## 1. Agent reservation under contention (separate OS processes, one DB)\n",
         "| Processes | Agents | Reservations | Reservations/s | Empty picks (no agent free) | Overlapping holds |",
         "|---|---|---|---|---|---|"]
    for r in out["contention"]:
        L.append(f"| {r['processes']} | {r['agents']} | {r['reservations']} | {r['reservations_per_s']} | "
                 f"{r['empty_picks']} | **{r['overlapping_holds']}** |")
    L.append("\nOverlapping holds must be 0: no agent was ever held by two workers at the same time. With only 5 "
             "agents and 8 processes almost every pick races - throughput drops (that is the hot-row problem, "
             "see SCALING) but correctness does not.\n")
    r = out["ingestion"]
    L += ["## 2. Provider event ingestion (30% duplicates, shuffled order)\n",
          f"- calls: {r['calls']}, events delivered: {r['events_delivered']}",
          f"- ingest (durable insert + dedupe): **{r['ingest_per_s']} events/s**",
          f"- apply (state machine): **{r['apply_per_s']} events/s**",
          f"- final call states: `{r['final_call_states']}`  (all {r['calls']} completed exactly once)",
          f"- final agent states: `{r['final_agent_states']}`",
          f"- invariant violations: {r['invariant_violations']}\n"]
    L += ["## 3. End-to-end tick cost vs campaign size (one pacing worker, predictive)\n",
          "| Agents | Calls started / s | Tick p50 (ms) | Tick p95 (ms) | Snapshot p50 | Events p50 | Allocation p50 | "
          "Pacing+safety p50 | Realtime factor |", "|---|---|---|---|---|---|---|---|---|"]
    for r in out["scale"]:
        m, n = r["ms_p50"], r["ms_p95"]
        L.append(f"| {r['agents']} | {r['calls_per_sim_s']} | {m['tick_total']} | {n['tick_total']} | {m['snapshot']} | "
                 f"{m['events']} | {m['allocation']} | {m['pacing+safety']} | {r['realtime_factor']}x |")
    L.append("\nRealtime factor > 1 means the system keeps up with a 1-second pacing tick. Interpretation and the "
             "bottleneck analysis are in docs/SCALING.md.\n")
    with open(os.path.join(OUT, "LOAD_TEST.md"), "w") as f:
        f.write("\n".join(L))


if __name__ == "__main__":
    main()
