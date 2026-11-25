"""
Narrated demonstrations of the five failure cases in the assignment.

    python sim/failure_demos.py          # prints the story and writes docs/reports/FAILURE_DEMOS.md

Each demo builds a tiny world, breaks something on purpose, and prints the
state of agents/calls/borrowers before and after recovery.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

from helpers import (advance, call_states, make_env, predictive_permit_whitebox, progressive_permit,  # noqa: E402
                     snap, temp_db)
from smartdialer.lifecycle import Lifecycle  # noqa: E402
from smartdialer.pacing import PREDICTIVE, Proposal  # noqa: E402
from smartdialer.providers import PROFILE_B  # noqa: E402
from smartdialer.states import AgentState  # noqa: E402
from smartdialer.store import Store  # noqa: E402
from smartdialer.worker import DialerWorker  # noqa: E402

LINES: list[str] = []


def say(msg=""):
    print(msg)
    LINES.append(msg)


def show(st, call_id=None, agent="agent-00000"):
    a = st.get_agent(agent)
    say(f"    agent {agent}: {a['state']}  (current_call={a['current_call_id']})")
    if call_id:
        c = st.get_call(call_id)
        b = st.get_borrower(c["borrower_id"])
        say(f"    call  {call_id}: {c['state']}  provider_call_id={c['provider_call_id']}  reason={c['end_reason']}")
        say(f"    borrower {b['id']}: {b['state']}  attempts={b['attempts']}  provider_failures={b['provider_failures']}")


def demo_worker_crash():
    say("## 1. Worker crash: agent reserved > borrower reserved > call initiated > worker crashes\n")
    for placed in (True, False):
        say(f"### Variant: the provider {'DID' if placed else 'did NOT'} create the call before the crash\n```")
        path = temp_db()
        env = make_env(agents=1, path=path)
        alloc = env.worker.allocator
        permit = progressive_permit(env, 1)
        alloc._consume(permit)
        cid = alloc._prepare_progressive(permit)
        real = env.prov.place_call

        def die(req, real=real, placed=placed):
            if placed:
                real(req)
            raise SystemExit("worker w1 crashed")
        env.prov.place_call = die
        try:
            alloc._initiate(cid)
        except SystemExit as e:
            say(f"  !! {e} (INITIATED was committed before calling the provider; provider id never stored)")
        env.prov.place_call = real
        show(env.store, cid)
        say("  ...worker w2 starts with its own DB connection; w1's setup lease (8s) expires...")
        st = Store(path, env.clock, init=False)
        w2 = DialerWorker("w2", st, "c1", env.router, env.worker.pacer)
        env.clock.advance(9)
        env.prov.advance(env.clock.now())
        w2.recovery.sweep("c1")
        say(f"  recovery: provider.lookup(idempotency_key={cid}) -> "
            f"{'found -> ADOPT the call' if placed else 'not found -> roll back'}")
        if placed:
            for _ in range(10):
                env.clock.advance(1)
                env.prov.advance(env.clock.now())
                w2.pump_events()
        show(st, cid)
        say(f"  invariant violations: {st.check_invariants()}")
        say("```\n")


def demo_provider_outage():
    say("## 2. Provider outage\n```")
    env = make_env(agents=20, borrowers=2000, answer_rate=0.3, talk=15, n_providers=2)
    for _ in range(40):
        env.worker.tick()
        advance(env, 1.0)
    say(f"  before: breakers={env.router.health()['states']}")
    inflight = [r["id"] for r in env.store.q("SELECT id FROM calls WHERE state IN ('INITIATED','RINGING','CONNECTED')")]
    env.provs[0].set_outage("timeout")
    say(f"  !! provA starts timing out. Calls already in flight at this moment: {len(inflight)}")
    for i in range(40):
        rep = env.worker.tick()
        advance(env, 1.0)
        if i in (0, 1, 2, 5, 20, 39):
            a = rep.allocation
            by = {r["provider"]: r["c"] for r in env.store.q("SELECT provider, COUNT(*) c FROM calls WHERE "
                                                             "initiated_at>=? GROUP BY provider", (env.clock.now() - 1,))}
            say(f"  t+{i+1:>2}s breakers={env.router.health()['states']} decision={rep.decision.action if rep.decision else '-'}"
                f" placed={a.placed if a else 0} timeouts={a.uncertain if a else 0} new calls by provider={by}")
    env.provs[1].set_outage("timeout")
    say("  !! provB ALSO times out")
    for i in range(25):
        rep = env.worker.tick()
        advance(env, 1.0)
    say(f"  breakers={env.router.health()['states']} decision={rep.decision.action}  rule: {rep.decision.reasons.get('rule')}")
    unc = env.store.q1("SELECT COUNT(*) c FROM calls WHERE setup_uncertain=1")["c"]
    say(f"  calls whose place_call timed out (outcome unknown): {unc} -> reconciled via provider.lookup(idempotency_key);")
    say("    while the provider can't answer, the agent stays held; after 30s we give up, release the agent and queue a HANGUP")
    say(f"  reconcile give-ups so far: {env.worker.recovery.counters['unreconcilable']}")
    b = env.store.q("SELECT id, attempts, provider_failures, round(next_attempt_at - ?) wait_s FROM borrowers "
                    "WHERE provider_failures>0 ORDER BY provider_failures DESC LIMIT 3", (env.clock.now(),))
    say(f"  retries: provider faults don't burn borrower attempts; backoff grows: {[dict(r) for r in b]}")
    env.provs[0].set_outage(None)
    env.provs[1].set_outage(None)
    say("  .. both providers come back ..")
    for i in range(40):
        env.worker.tick()
        advance(env, 1.0)
        if i in (0, 20, 39):
            say(f"  t+{i+1:>2}s breakers={env.router.health()['states']}  (OPEN -> HALF_OPEN: 1 probe call -> CLOSED)")
    st = {}
    for c in inflight:
        k = env.store.get_call(c)["state"]
        st[k] = st.get(k, 0) + 1
    say(f"  the {len(inflight)} calls that were in flight when the outage began ended as: {st}")
    pending = env.store.q1("SELECT COUNT(*) c FROM outbox WHERE status='PENDING'")["c"]
    say(f"  pending outbox commands: {pending}")
    say(f"  invariant violations: {env.store.check_invariants()}")
    say("```\n")


def demo_agent_drop():
    import json
    say("## 3. Agent availability suddenly drops (100 agents, 40 vanish within 3 s)\n")
    say("Same setup as scenario F in SIMULATION_REPORT.md (drop at t=600 s). The safety controller's own reasoning per tick:\n```")
    from sim.engine import Scenario, Simulation
    sim = Simulation(Scenario("F", 0.25, 120, agents=100, duration_s=612,
                              script=[(600, lambda s: s.drop_agents(14)), (601, lambda s: s.drop_agents(14)),
                                      (602, lambda s: s.drop_agents(12))]))
    sim.run()
    logs = {round(r["at"] - sim.t0): r for r in sim.store.q("SELECT * FROM safety_log")}
    for r in sim.samples[597:610]:
        lg = logs.get(round(r["t"]))
        d = json.loads(lg["reasons"]) if lg else {}
        say(f"  t={r['t']:>4.0f}s logged_in={r['logged_in']:>3} free={r['available']:>3} ringing={r['unbound']:>3} "
            f"waiting={r['waiting']} {r['action']:<21} approved={r['approved']:>2} cancelled={r['shed']:>2} | "
            f"{(d.get('rule') or '')[:60]}")
    say(f"  abandoned in the whole run: {sim.summary()['abandoned']}   invariant violations: {len(sim.violations)}")
    say("```\nReaction time: one pacing tick (1 s). The drop window sees agents going OFFLINE, the controller "
        "switches to FALLBACK_PROGRESSIVE (60 s cool-down) and cancels the newest ringing calls that the "
        "remaining agents could not absorb - before those borrowers answer. Calls that have been ringing for "
        "a long time are kept: the age-aware model knows they are unlikely to be answered.\n")


def demo_duplicates_and_out_of_order():
    say("## 4 + 5. Duplicate and out-of-order events\n```")
    env = make_env(agents=1)
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    c = env.store.get_call(res.call_ids[0])

    def inj(t, eid, dt=0.0, **p):
        env.store.ingest_event(c["provider"], eid, t, c["provider_call_id"], c["id"], env.clock.now() + dt, p)
    say("  provider sends: ANSWERED, ANSWERED (same id), ANSWERED (new id), COMPLETED")
    for t, e in (("ANSWERED", "e2"), ("ANSWERED", "e2"), ("ANSWERED", "e2b")):
        inj(t, e)
    inj("COMPLETED", "e3", 1, answered=True, bridged=True, talk_s=30)
    env.worker.events.process_pending()
    say(f"  call transitions: {call_states(env.store, c['id'])}")
    say(f"  inbox outcomes:   {[r['outcome'] for r in env.store.q('SELECT outcome FROM provider_events ORDER BY id')]}")
    say(f"  agent: {env.store.get_agent(c['agent_id'])['state']}")

    env = make_env(agents=1)
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    c = env.store.get_call(res.call_ids[0])
    say("\n  provider sends: COMPLETED, ANSWERED, RINGING (reverse order)")
    inj("COMPLETED", "x3", 0, answered=True, bridged=True, talk_s=30)
    env.worker.events.process_pending()          # COMPLETED is processed before the others even arrive
    inj("ANSWERED", "x2", -20)
    inj("RINGING", "x1", -25)
    env.worker.events.process_pending()
    say(f"  call transitions: {call_states(env.store, c['id'])}")
    say(f"  inbox outcomes:   {[r['outcome'] for r in env.store.q('SELECT outcome FROM provider_events ORDER BY id')]}")
    say(f"  agent: {env.store.get_agent(c['agent_id'])['state']}")
    say("  (if several events are already waiting in the inbox, they are also applied in provider-timestamp")
    say("   order; but correctness never depends on that - the monotonic state machine handles any order)")

    env = make_env(agents=10, borrowers=400, profile=PROFILE_B, answer_rate=0.5, talk=20)
    for _ in range(300):
        env.worker.tick()
        advance(env, 1.0)
    say(f"\n  5 minutes on Provider B (25% duplicates, 5% re-sent with new ids, up to 2.5s reordering):")
    say(f"  event counters: {env.worker.events.counters}")
    say(f"  invariant violations: {env.store.check_invariants()}")
    say("```\n")


def demo_crash_after_answered():
    say("## Bonus: the provider sends ANSWERED, the worker crashes, then COMPLETED arrives\n```")
    path = temp_db()
    env = make_env(agents=1, mode="PREDICTIVE", path=path)
    res = env.worker.allocator.execute(predictive_permit_whitebox(env, 1))
    cid = res.call_ids[0]
    advance(env, 6.0, tick=False)
    env.worker.events.ingest(env.prov.poll_events(env.clock.now()))
    say("  ANSWERED stored in the inbox; worker w1 crashes before processing it.")
    st = Store(path, env.clock, init=False)
    w2 = DialerWorker("w2", st, "c1", env.router, env.worker.pacer)
    w2.events.process_pending()
    w2.outbox.run()
    say(f"  w2 processes it: call={st.get_call(cid)['state']} agent={st.get_agent('agent-00000')['state']} "
        f"bridge sent={env.prov.stats['bridges']}")
    for _ in range(200):
        env.clock.advance(1)
        env.prov.advance(env.clock.now())
        w2.pump_events()
        if st.get_call(cid)["state"] == "COMPLETED":
            break
    say(f"  COMPLETED arrives: call={st.get_call(cid)['state']} agent={st.get_agent('agent-00000')['state']}")
    say(f"  invariant violations: {st.check_invariants()}")
    say("```\n")


if __name__ == "__main__":
    say("# Failure demonstrations\n")
    say("Generated by `python sim/failure_demos.py`.\n")
    demo_worker_crash()
    demo_provider_outage()
    demo_agent_drop()
    demo_duplicates_and_out_of_order()
    demo_crash_after_answered()
    out = os.path.join(ROOT, "docs", "reports", "FAILURE_DEMOS.md")
    with open(out, "w") as f:
        f.write("\n".join(LINES))
    print(f"\nwritten {out}")
