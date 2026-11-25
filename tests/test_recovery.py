"""
Failure case 1 from the assignment:
    Agent reserved > Borrower reserved > Call initiated > Worker crashes
What happens when the system comes back?
"""
from helpers import advance, make_env, progressive_permit, temp_db
from smartdialer.providers import ProviderProfile
from smartdialer.store import Store
from smartdialer.worker import DialerWorker


def _crash_after_initiated(env, provider_places_call: bool):
    """Run the allocator up to 'INITIATED is committed' (+ optionally the provider accepting the
    call), then stop: the worker process dies before it can record the provider call id."""
    alloc = env.worker.allocator
    permit = progressive_permit(env, 1)
    alloc._consume(permit)
    call_id = alloc._prepare_progressive(permit)           # agent RESERVED, borrower claimed, call RESERVED
    real_place = env.prov.place_call

    class Crash(Exception):
        pass

    def place_then_die(req):
        if provider_places_call:
            real_place(req)                                 # the provider DID create the call
        raise Crash()                                       # ...and we never see the response

    env.prov.place_call = place_then_die
    try:
        alloc._initiate(call_id)
    except Crash:
        pass
    env.prov.place_call = real_place
    return call_id


def test_crash_after_initiate_call_exists_is_adopted():
    path = temp_db()
    env = make_env(agents=1, path=path)
    cid = _crash_after_initiated(env, provider_places_call=True)
    c = env.store.get_call(cid)
    assert c["state"] == "INITIATED" and c["provider_call_id"] is None
    assert env.store.get_agent("agent-00000")["state"] == "DIALING"

    # --- system comes back: a new worker with a new connection ---
    st = Store(path, env.clock, init=False)
    w2 = DialerWorker("w2", st, "c1", env.router, env.worker.pacer)
    env.clock.advance(env.policy.setup_lease_s + 1)          # dead worker's lease expires
    env.prov.advance(env.clock.now())
    w2.recovery.sweep("c1")
    c = st.get_call(cid)
    assert c["provider_call_id"] is not None                 # adopted via provider.lookup(idempotency_key)
    assert w2.recovery.counters["adopted"] == 1
    # the call now continues normally with the new worker
    for _ in range(20):
        env.clock.advance(1)
        env.prov.advance(env.clock.now())
        w2.pump_events()
    assert st.get_call(cid)["state"] == "CONNECTED"
    assert st.get_agent("agent-00000")["state"] == "CONNECTED"
    assert st.check_invariants() == []


def test_crash_after_initiate_call_never_created_is_rolled_back():
    path = temp_db()
    env = make_env(agents=1, path=path)
    cid = _crash_after_initiated(env, provider_places_call=False)
    st = Store(path, env.clock, init=False)
    w2 = DialerWorker("w2", st, "c1", env.router, env.worker.pacer)
    env.clock.advance(env.policy.setup_lease_s + 1)
    w2.recovery.sweep("c1")
    c = st.get_call(cid)
    assert c["state"] == "FAILED" and "no record" in c["end_reason"]
    assert st.get_agent("agent-00000")["state"] == "AVAILABLE"
    b = st.get_borrower(c["borrower_id"])
    assert b["state"] == "PENDING" and b["attempts"] == 0     # not the borrower's fault
    assert env.prov.stats["placed"] == 0                      # no ghost call anywhere


def test_crash_before_initiate_rolls_back():
    env = make_env(agents=1)
    alloc = env.worker.allocator
    permit = progressive_permit(env, 1)
    alloc._consume(permit)
    cid = alloc._prepare_progressive(permit)                  # crash right here
    env.clock.advance(env.policy.setup_lease_s + 1)
    env.worker.recovery.sweep("c1")
    assert env.store.get_call(cid)["state"] == "CANCELLED"
    assert env.store.get_agent("agent-00000")["state"] == "AVAILABLE"
    assert env.store.check_invariants() == []


def test_reconcile_during_outage_waits_then_gives_up_with_hangup():
    env = make_env(agents=1)
    cid = _crash_after_initiated(env, provider_places_call=True)
    env.prov.set_outage("timeout")
    env.clock.advance(env.policy.setup_lease_s + 1)
    env.worker.recovery.sweep("c1")
    assert env.store.get_call(cid)["state"] == "INITIATED"    # can't tell yet -> keep waiting (agent held)
    env.clock.advance(env.policy.uncertain_giveup_s + 1)
    env.worker.recovery.sweep("c1")
    c = env.store.get_call(cid)
    assert c["state"] == "FAILED" and "gave up" in c["end_reason"]
    assert env.store.get_agent("agent-00000")["state"] == "AVAILABLE"
    assert env.store.q1("SELECT status FROM outbox WHERE command='HANGUP'")["status"] == "PENDING"
    env.prov.set_outage(None)
    env.worker.outbox.run()
    assert env.prov.stats["hangups"] == 1                     # the ghost is killed once the provider is back


def test_silent_call_is_reconciled():
    """Provider stops sending events for a ringing call (lost webhooks)."""
    env = make_env(agents=1, profile=ProviderProfile(setup_latency_s=(0.5, 0.5), event_delay_s=(10_000, 10_000),
                                                     network_fail_prob=0, busy_prob=0))
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    cid = res.call_ids[0]
    advance(env, env.policy.ring_stale_s + 5)                 # call answered+... but no webhooks arrive
    env.worker.recovery.sweep("c1")
    assert env.store.get_call(cid)["state"] in ("CONNECTED", "COMPLETED")
    assert env.worker.recovery.counters["stale_reconciled"] == 1
