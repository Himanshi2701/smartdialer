"""
"Don't assume that external systems will behave nicely."
Duplicate events, out-of-order events, events for unknown calls, crash mid-processing.
Events are injected straight into the inbox so the exact order is controlled.
"""
from helpers import (advance, call_states, make_env, predictive_permit_whitebox, progressive_permit, temp_db)
from smartdialer.events import EventProcessor
from smartdialer.lifecycle import Lifecycle
from smartdialer.store import Store
from smartdialer.worker import DialerWorker


def _placed_progressive_call(env):
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    assert res.placed == 1
    call = env.store.get_call(res.call_ids[0])
    return call


def _inject(env, call, typ, eid, ts_offset=0.0, **payload):
    env.store.ingest_event(call["provider"], eid, typ, call["provider_call_id"], call["id"],
                           env.clock.now() + ts_offset, payload)


def test_answered_x3_then_completed():
    env = _env = make_env(agents=1)
    call = _placed_progressive_call(env)
    _inject(env, call, "RINGING", "e1")
    _inject(env, call, "ANSWERED", "e2")
    _inject(env, call, "ANSWERED", "e2")                    # exact duplicate (same id) -> dedup in inbox
    _inject(env, call, "ANSWERED", "e2-resent")             # same meaning, new id -> state machine no-op
    _inject(env, call, "COMPLETED", "e3", 1.0, answered=True, bridged=True, talk_s=42.0)
    env.worker.events.process_pending()

    c = env.store.get_call(call["id"])
    assert c["state"] == "COMPLETED" and c["talk_s"] == 42.0
    assert call_states(env.store, call["id"]) == ["QUEUED", "RESERVED", "INITIATED", "RINGING", "ANSWERED",
                                                  "CONNECTED", "COMPLETED"]       # each state exactly once
    a = env.store.get_agent(call["agent_id"])
    assert a["state"] == "WRAP_UP" and a["current_call_id"] is None
    assert env.store.get_borrower(call["borrower_id"])["state"] == "DONE"
    outcomes = [r["outcome"] for r in env.store.q("SELECT outcome FROM provider_events ORDER BY id")]
    assert outcomes == ["applied", "applied", "stale", "applied"]  # duplicate never even stored twice
    assert env.store.check_invariants() == []


def test_completed_answered_ringing_out_of_order():
    env = make_env(agents=1)
    call = _placed_progressive_call(env)
    _inject(env, call, "COMPLETED", "e3", 0, answered=True, bridged=True, talk_s=30.0)
    env.worker.events.process_pending()
    _inject(env, call, "ANSWERED", "e2", -20)
    _inject(env, call, "RINGING", "e1", -25)
    env.worker.events.process_pending()
    c = env.store.get_call(call["id"])
    assert c["state"] == "COMPLETED"
    assert env.store.get_agent(call["agent_id"])["state"] == "WRAP_UP"
    assert call_states(env.store, call["id"])[-1] == "COMPLETED"
    outcomes = [r["outcome"] for r in env.store.q("SELECT outcome FROM provider_events ORDER BY id")]
    assert outcomes == ["applied", "stale", "stale"]
    assert env.store.check_invariants() == []


def test_completed_not_answered_releases_agent():
    env = make_env(agents=1)
    call = _placed_progressive_call(env)
    _inject(env, call, "COMPLETED", "e9", answered=False, bridged=False, talk_s=0)
    _inject(env, call, "RINGING", "e1", -10)
    env.worker.events.process_pending()
    assert env.store.get_call(call["id"])["state"] == "NO_ANSWER"
    assert env.store.get_agent(call["agent_id"])["state"] == "AVAILABLE"
    b = env.store.get_borrower(call["borrower_id"])
    assert b["state"] == "PENDING" and b["attempts"] == 1


def test_crash_after_answered_ingested_before_processing():
    """Webhook stored in the inbox, worker dies before applying it. A new worker applies it."""
    path = temp_db()
    env = make_env(agents=1, path=path)
    call = _placed_progressive_call(env)
    _inject(env, call, "ANSWERED", "e2")
    # worker 1 "crashes": nothing processed. A brand-new worker with its own connection takes over.
    st2 = Store(path, env.clock, init=False)
    w2 = DialerWorker("w2", st2, "c1", env.router, env.worker.pacer)
    w2.events.process_pending()
    assert st2.get_call(call["id"])["state"] == "CONNECTED"
    assert st2.get_agent(call["agent_id"])["state"] == "CONNECTED"
    _inject(env, call, "COMPLETED", "e3", answered=True, bridged=True, talk_s=10)
    w2.events.process_pending()
    assert st2.get_call(call["id"])["state"] == "COMPLETED"


def test_crash_in_the_middle_of_applying_answered_rolls_back():
    """The worker dies half-way through the ANSWERED transaction (after marking the event and
    moving the call, before the agent). The transaction never commits -> nothing is half-applied."""
    path = temp_db()
    env = make_env(agents=1, path=path)
    call = _placed_progressive_call(env)
    _inject(env, call, "ANSWERED", "e2")
    doomed = Store(path, env.clock, init=False)
    doomed.conn.execute("BEGIN IMMEDIATE")
    doomed.conn.execute("UPDATE provider_events SET processed_at=1 WHERE provider_event_id='e2'")
    doomed.conn.execute("UPDATE calls SET state='ANSWERED' WHERE id=?", (call["id"],))
    doomed.conn.close()                       # process dies: no COMMIT -> rollback
    st = Store(path, env.clock, init=False)
    assert st.get_call(call["id"])["state"] == "INITIATED"
    assert st.q1("SELECT processed_at FROM provider_events WHERE provider_event_id='e2'")["processed_at"] is None
    w2 = DialerWorker("w2", st, "c1", env.router, env.worker.pacer)
    w2.events.process_pending()
    assert st.get_call(call["id"])["state"] == "CONNECTED"
    assert st.check_invariants() == []


def test_predictive_answer_binds_agent_and_bridge_survives_crash():
    """Predictive: ANSWERED -> agent bound + BRIDGE written to the outbox in one txn.
    Worker crashes before sending BRIDGE; another worker's outbox dispatcher sends it."""
    path = temp_db()
    env = make_env(agents=1, mode="PREDICTIVE", path=path)
    res = env.worker.allocator.execute(predictive_permit_whitebox(env, 1))
    call = env.store.get_call(res.call_ids[0])
    assert call["agent_id"] is None
    advance(env, 0.6, tick=False)                     # provider rings
    advance(env, 5.0, tick=False)                     # provider: answered (not bridged yet)
    env.worker.events.ingest(env.prov.poll_events(env.clock.now()))
    env.worker.events.process_pending()               # binds agent, enqueues BRIDGE ... then "crash"
    c = env.store.get_call(call["id"])
    assert c["state"] == "CONNECTED" and c["agent_id"] == "agent-00000"
    assert env.store.q1("SELECT status FROM outbox WHERE command='BRIDGE'")["status"] == "PENDING"
    st2 = Store(path, env.clock, init=False)
    w2 = DialerWorker("w2", st2, "c1", env.router, env.worker.pacer)
    w2.outbox.run()
    assert env.prov.stats["bridges"] == 1
    assert st2.q1("SELECT status FROM outbox WHERE command='BRIDGE'")["status"] == "DONE"


def test_predictive_answer_with_no_agent_holds_then_abandons():
    env = make_env(agents=1, mode="PREDICTIVE")
    Lifecycle(env.store).agent_set_state("agent-00000", __import__("smartdialer.states").states.AgentState.PAUSED)
    res = env.worker.allocator.execute(predictive_permit_whitebox(env, 1))
    cid = res.call_ids[0]
    advance(env, 6.0)                                 # rings, answers; nobody free -> hold
    c = env.store.get_call(cid)
    assert c["state"] in ("ANSWERED", "ABANDONED")
    advance(env, 3.0)                                 # hold (2s) expires
    c = env.store.get_call(cid)
    assert c["state"] == "ABANDONED"
    assert env.store.get_borrower(c["borrower_id"])["next_attempt_at"] > env.clock.now() + 3600  # cooldown


def test_predictive_hold_gets_agent_when_one_frees_up():
    from smartdialer.states import AgentState
    env = make_env(agents=1, mode="PREDICTIVE")
    lc = Lifecycle(env.store)
    lc.agent_set_state("agent-00000", AgentState.PAUSED)
    res = env.worker.allocator.execute(predictive_permit_whitebox(env, 1))
    cid = res.call_ids[0]
    advance(env, 5.5)
    assert env.store.get_call(cid)["state"] == "ANSWERED"
    lc.agent_set_state("agent-00000", AgentState.AVAILABLE)   # agent comes back within the hold window
    env.worker.holds.run("c1")
    env.worker.outbox.run()
    assert env.store.get_call(cid)["state"] == "CONNECTED"


def test_event_for_unknown_call_is_hung_up():
    env = make_env()
    env.store.ingest_event("provA", "zz", "ANSWERED", "provA-9999999", None, env.clock.now(), {})
    env.worker.events.process_pending()
    row = env.store.q1("SELECT * FROM outbox")
    assert row["command"] == "HANGUP"


def test_provider_b_chaos_stays_consistent():
    """Duplicates + resends + reordering from a noisy provider over many calls."""
    from smartdialer.providers import PROFILE_B
    env = make_env(agents=10, borrowers=300, profile=PROFILE_B, answer_rate=0.5, talk=20)
    for _ in range(240):
        env.worker.tick()
        advance(env, 1.0)
        assert env.store.check_invariants() == []
    c = env.worker.events.counters
    assert c["duplicates"] > 0 and c["stale"] > 0         # the chaos really happened
    # every call that the provider finished is terminal on our side too
    open_calls = env.store.q1("SELECT COUNT(*) c FROM calls WHERE state NOT IN "
                              "('COMPLETED','FAILED','CANCELLED','NO_ANSWER','ABANDONED')")["c"]
    assert open_calls <= 10                                 # only the ones still genuinely in progress
