"""Progressive mode: never more agent-bound calls than agents; failure and agent-loss handling."""
from helpers import advance, make_env, progressive_permit
from smartdialer.lifecycle import Lifecycle
from smartdialer.providers import ProviderProfile
from smartdialer.states import AgentState
from smartdialer.store import TERMINAL_SQL


def test_50_agents_never_more_than_50_calls():
    env = make_env(agents=50, borrowers=2000, answer_rate=0.3, talk=30)
    peak = 0
    for _ in range(300):
        env.worker.tick()
        active = env.store.q1(f"SELECT COUNT(*) c FROM calls WHERE state NOT IN {TERMINAL_SQL}")["c"]
        peak = max(peak, active)
        assert active <= 50
        advance(env, 1.0)
    assert peak == 50              # and it actually uses all of them
    assert env.store.check_invariants() == []


def test_progressive_permit_larger_than_agents_is_still_bounded():
    env = make_env(agents=5, borrowers=100)
    from smartdialer.pacing import PROGRESSIVE, Proposal
    from helpers import snap
    d = env.worker.safety.evaluate(Proposal(PROGRESSIVE, 500), snap(env))
    assert d.approved == 5
    res = env.worker.allocator.execute(d.permit)
    assert res.placed == 5


def test_which_borrower_and_which_agent():
    env = make_env(agents=3, borrowers=10)
    lc = Lifecycle(env.store)
    # make agent-00002 the longest idle
    env.clock.advance(1)
    lc.agent_set_state("agent-00000", AgentState.PAUSED)
    lc.agent_set_state("agent-00000", AgentState.AVAILABLE)
    lc.agent_set_state("agent-00001", AgentState.PAUSED)
    lc.agent_set_state("agent-00001", AgentState.AVAILABLE)
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    c = env.store.get_call(res.call_ids[0])
    assert c["agent_id"] == "agent-00002"         # longest idle agent first (fairness)
    assert c["borrower_id"] == "b000"             # highest priority borrower first


def test_call_rejected_by_provider_releases_agent_and_does_not_burn_attempt():
    env = make_env(agents=1, profile=ProviderProfile(reject_prob=1.0))
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    assert res.rejected == 1
    c = env.store.q1("SELECT * FROM calls")
    assert c["state"] == "FAILED"
    assert env.store.get_agent("agent-00000")["state"] == "AVAILABLE"
    b = env.store.get_borrower(c["borrower_id"])
    assert b["state"] == "PENDING" and b["attempts"] == 0 and b["provider_failures"] == 1
    assert b["next_attempt_at"] > env.clock.now()           # exponential backoff


def test_agent_disappears_before_dialing():
    """Agent reserved, then logs out before the call is sent: the call must not be placed."""
    env = make_env(agents=1)
    alloc = env.worker.allocator
    permit = progressive_permit(env, 1)
    alloc._consume(permit)
    call_id = alloc._prepare_progressive(permit)
    Lifecycle(env.store).agent_set_state("agent-00000", AgentState.OFFLINE)
    assert alloc._initiate(call_id) == "cancelled"
    assert env.prov.stats["placed"] == 0
    assert env.store.get_call(call_id)["state"] == "CANCELLED"
    assert env.store.get_borrower(env.store.get_call(call_id)["borrower_id"])["state"] == "PENDING"


def test_agent_disappears_while_ringing_is_swapped():
    env = make_env(agents=2)
    lc = Lifecycle(env.store)
    lc.agent_set_state("agent-00001", AgentState.PAUSED)
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    cid = res.call_ids[0]
    lc.agent_set_state("agent-00001", AgentState.AVAILABLE)
    lc.agent_set_state("agent-00000", AgentState.OFFLINE)          # vanishes mid-setup
    c = env.store.get_call(cid)
    assert c["agent_id"] == "agent-00001" and c["state"] == "INITIATED"
    assert env.store.get_agent("agent-00001")["state"] == "DIALING"
    advance(env, 6.0)                                              # borrower answers -> goes to the new agent
    assert env.store.get_call(cid)["state"] == "CONNECTED"
    assert env.store.check_invariants() == []


def test_agent_disappears_while_ringing_no_replacement_cancels_and_hangs_up():
    env = make_env(agents=1)
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    cid = res.call_ids[0]
    advance(env, 1.0)
    Lifecycle(env.store).agent_set_state("agent-00000", AgentState.OFFLINE)
    assert env.store.get_call(cid)["state"] == "CANCELLED"
    env.worker.outbox.run()
    assert env.prov.stats["hangups"] == 1                          # borrower's phone stops ringing
    advance(env, 10.0)                                             # late events are harmless
    assert env.store.get_call(cid)["state"] == "CANCELLED"


def test_timeout_is_not_blindly_retried():
    """ProviderTimeout = unknown outcome. Keep the agent, don't re-dial, reconcile later."""
    env = make_env(agents=1, profile=ProviderProfile(place_timeout_prob=1.0, ghost_prob=1.0, network_fail_prob=0,
                                                     busy_prob=0, setup_latency_s=(0.5, 0.5)))
    res = env.worker.allocator.execute(progressive_permit(env, 1))
    assert res.uncertain == 1
    c = env.store.q1("SELECT * FROM calls")
    assert c["setup_uncertain"] == 1 and c["state"] == "INITIATED"
    assert env.store.get_agent("agent-00000")["state"] == "DIALING"   # still held for that call
    assert env.prov.stats["placed"] == 1                                # the ghost exists exactly once
