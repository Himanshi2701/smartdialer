import os
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smartdialer.clock import SimClock
from smartdialer.config import DialerPolicy, SafetyConfig
from smartdialer.pacing import PREDICTIVE, PROGRESSIVE, PredictivePacer, ProgressivePacer, Proposal
from smartdialer.providers import CalleeWorld, MockProvider, ProviderProfile, ProviderRouter
from smartdialer.snapshot import build_snapshot
from smartdialer.store import Store
from smartdialer.worker import DialerWorker

# A boring, deterministic provider: no failures, no busy, fast, in-order.
CLEAN = ProviderProfile(setup_latency_s=(0.5, 0.5), event_delay_s=(0.0, 0.0), network_fail_prob=0.0, busy_prob=0.0)


def temp_db():
    d = tempfile.mkdtemp()
    return os.path.join(d, "test.db")


def make_env(agents=3, borrowers=20, mode="PROGRESSIVE", answer_rate=1.0, talk=60.0, path=":memory:",
             profile=CLEAN, policy=DialerPolicy(), safety_cfg=SafetyConfig(), worker_id="w1", n_providers=1):
    clock = SimClock()
    store = Store(path, clock)
    store.add_campaign("c1", mode)
    store.add_agents("c1", agents)
    store.add_borrowers("c1", [(f"b{i:03d}", f"+91-0000-{i:06d}", 100 - i) for i in range(borrowers)])
    world = CalleeWorld(answer_rate, talk, ring_to_answer_s=(5.0, 5.0))
    world.t0 = clock.now()
    provs = [MockProvider(f"prov{n}", clock, world, profile, seed=i) for i, n in enumerate("AB"[:n_providers])]
    router = ProviderRouter(provs, clock, min_lag_samples=5, assume_fast_until_measured=True)
    pacer = PredictivePacer() if mode == "PREDICTIVE" else ProgressivePacer()
    w = DialerWorker(worker_id, store, "c1", router, pacer, safety_cfg, policy)
    return SimpleNamespace(clock=clock, store=store, world=world, prov=provs[0], provs=provs, router=router,
                           worker=w, policy=policy)


def snap(env):
    return build_snapshot(env.store, "c1")


def progressive_permit(env, n):
    d = env.worker.safety.evaluate(Proposal(PROGRESSIVE, n), snap(env))
    return d.permit


def predictive_permit_whitebox(env, n):
    """White-box helper: mint a predictive permit directly (bypassing the warm-up statistics).
    Only tests may do this; production code has no access to _permit's key."""
    s = snap(env)
    return env.worker.safety._permit(s, PREDICTIVE, n, s.unbound_inflight + n)


def advance(env, seconds, step=0.5, tick=True):
    t = 0.0
    while t < seconds:
        env.clock.advance(step)
        for p in env.provs:
            p.advance(env.clock.now())
        if tick:
            env.worker.pump_events()
            env.worker.outbox.run()
            env.worker.holds.run("c1")
        t += step


def call_states(store, call_id):
    return [r["to_state"] for r in store.q("SELECT to_state FROM call_transitions WHERE call_id=? ORDER BY id",
                                           (call_id,))]
