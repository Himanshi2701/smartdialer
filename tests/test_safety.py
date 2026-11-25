"""The safety boundary: can't be bypassed, and does the right thing in each situation."""
import dataclasses
import inspect
import itertools
import math

import pytest

from helpers import make_env, snap
from smartdialer.allocator import PermitRefused
from smartdialer.pacing import PREDICTIVE, PROGRESSIVE, PredictivePacer, Proposal
from smartdialer.safety import (APPROVE, FALLBACK, REDUCE, REJECT, Permit, binom_tail_gt, max_safe_inflight,
                                wilson_upper)
from smartdialer.snapshot import Snapshot, WindowStats
from smartdialer.stats import OverflowDist, overflow_prob, p_remaining


def fake_snap(**kw):
    base = dict(now=1000.0, campaign_id="c1", active=True, logged_in=20, available=10, reserved=0, dialing=0,
                connected=10, wrap_up=0, paused=0, wrap_remaining_s=(), connected_elapsed_s=(), agent_drops_recent=0,
                unbound_inflight=0, unbound_ages_s=(), waiting_answered=0, bound_inflight=0,
                short=WindowStats(120, 40, 160), long=WindowStats(900, 200, 800), avg_talk_s=120, avg_wrap_s=20,
                avg_time_to_answer_s=10, p05_time_to_answer_s=5, tta_sorted=tuple(range(5, 16)),
                answered_abandon_window=200, abandoned_abandon_window=0, hist_answer_rate=0.2, hist_talk_s=120,
                fallback_until=0.0, fallback_reason="")
    base.update(kw)
    return Snapshot(**base)


# ------------------------------------------------------------ bypass attempts
def test_forged_permit_is_refused():
    env = make_env()
    forged = Permit("p-1", "c1", PREDICTIVE, 1000, 1000, env.clock.now(), env.clock.now() + 60, "deadbeef")
    with pytest.raises(PermitRefused, match="signature"):
        env.worker.allocator.execute(forged)
    assert env.store.q1("SELECT COUNT(*) c FROM calls")["c"] == 0


def test_tampered_permit_is_refused():
    env = make_env(agents=3)
    d = env.worker.safety.evaluate(Proposal(PROGRESSIVE, 1), snap(env))
    bigger = dataclasses.replace(d.permit, count=500)          # keep signature, change count
    with pytest.raises(PermitRefused, match="signature"):
        env.worker.allocator.execute(bigger)


def test_permit_cannot_be_replayed_or_used_late():
    env = make_env(agents=3)
    d = env.worker.safety.evaluate(Proposal(PROGRESSIVE, 1), snap(env))
    env.worker.allocator.execute(d.permit)
    with pytest.raises(PermitRefused, match="already used"):
        env.worker.allocator.execute(d.permit)
    d2 = env.worker.safety.evaluate(Proposal(PROGRESSIVE, 1), snap(env))
    env.clock.advance(60)
    with pytest.raises(PermitRefused, match="expired"):
        env.worker.allocator.execute(d2.permit)


def test_pacer_has_no_path_to_the_provider():
    pacer = PredictivePacer()
    assert set(vars(pacer)) == {"cfg"}                       # holds nothing but its config
    src = inspect.getsource(__import__("smartdialer.pacing", fromlist=["x"]))
    for forbidden in ("allocator", "provider", "safety", "place_call"):
        assert f"import {forbidden}" not in src and f".{forbidden}" not in src


def test_absurd_proposal_is_clipped_to_the_bound():
    env = make_env()
    s = fake_snap()
    d = env.worker.safety.evaluate(Proposal(PREDICTIVE, 10 ** 6), s)
    assert d.action == REDUCE
    assert 0 < d.approved <= 30                               # 10 free agents
    p = float(d.reasons["p_new_call"])
    assert overflow_prob(10, [p] * d.approved) <= 0.01 + 1e-9


def test_negative_or_garbage_proposal_is_harmless():
    env = make_env()
    for bad in (-5, 0):
        d = env.worker.safety.evaluate(Proposal(PREDICTIVE, bad), fake_snap())
        assert d.approved == 0 and d.permit is None
    d = env.worker.safety.evaluate(Proposal("YOLO", 5), fake_snap())     # unknown mode -> progressive rules
    assert d.mode == PROGRESSIVE and d.approved <= 10


# ------------------------------------------------------------ rules
def test_no_healthy_provider_rejects_everything():
    env = make_env()
    br = env.router.breakers["provA"]
    for _ in range(10):
        br.record(False)
    d = env.worker.safety.evaluate(Proposal(PREDICTIVE, 10), fake_snap())
    assert d.action == REJECT and d.approved == 0 and "R2" in d.reasons["rule"]


def test_half_open_allows_a_single_probe():
    env = make_env()
    br = env.router.breakers["provA"]
    for _ in range(10):
        br.record(False)
    env.clock.advance(br.open_s + 1)
    d = env.worker.safety.evaluate(Proposal(PROGRESSIVE, 10), fake_snap())
    assert d.approved == 1


def test_cold_start_falls_back_to_progressive():
    env = make_env()
    d = env.worker.safety.evaluate(Proposal(PREDICTIVE, 40), fake_snap(long=WindowStats(900, 2, 3),
                                                                       short=WindowStats(120, 2, 3)))
    assert d.action == FALLBACK and d.mode == PROGRESSIVE and d.approved == 10


def test_agent_drop_triggers_fallback_and_shedding():
    env = make_env()
    ages = tuple([0.5] * 25)
    d = env.worker.safety.evaluate(Proposal(PREDICTIVE, 40), fake_snap(
        logged_in=60, available=5, agent_drops_recent=40, unbound_inflight=25, unbound_ages_s=ages))
    assert d.action == FALLBACK and d.mode == PROGRESSIVE
    assert "agent drop" in d.reasons["rule"]
    assert d.shed > 0
    assert d.approved == 0             # deterministic: 5 agents, still ringing calls could take them


def test_abandon_budget_triggers_fallback_and_cooldown_persists():
    env = make_env()
    d = env.worker.safety.evaluate(Proposal(PREDICTIVE, 40), fake_snap(abandoned_abandon_window=6))
    assert d.action == FALLBACK and "abandon budget" in d.reasons["rule"]
    camp = env.store.q1("SELECT * FROM campaigns")
    assert camp["safety_fallback_until"] > 1000.0


def test_answer_rate_spike_uses_short_window():
    """Model learned 20%, suddenly 70% answer. The short window's upper bound dominates."""
    env = make_env()
    calm = env.worker.safety.evaluate(Proposal(PREDICTIVE, 100), fake_snap())
    spike = env.worker.safety.evaluate(Proposal(PREDICTIVE, 100), fake_snap(short=WindowStats(120, 70, 30)))
    assert spike.reasons["p_safe"] > 0.7
    assert spike.approved < calm.approved
    assert spike.approved <= 12          # ~1 call per free agent when nearly everyone answers


def test_answer_rate_crash_is_handled_by_long_window():
    """Model learned 70%, it drops to 10%. Danger is the bounce-back, so p_safe stays high (long window)."""
    env = make_env()
    d = env.worker.safety.evaluate(Proposal(PREDICTIVE, 100), fake_snap(short=WindowStats(120, 5, 45),
                                                                       long=WindowStats(900, 600, 300)))
    assert d.reasons["p_safe"] >= 0.65


def test_progressive_is_deterministic_with_predictive_calls_in_flight():
    env = make_env()
    d = env.worker.safety.evaluate(Proposal(PROGRESSIVE, 50), fake_snap(available=10, unbound_inflight=7,
                                                                       unbound_ages_s=(20,) * 7, waiting_answered=1))
    assert d.approved == 2   # 10 free - 7 that might still answer - 1 already waiting


# ------------------------------------------------------------ math
def test_binomial_tail_matches_brute_force():
    for m, p, c in [(5, 0.3, 2), (8, 0.5, 4), (12, 0.2, 1), (3, 0.9, 3)]:
        brute = sum(math.comb(m, k) * p ** k * (1 - p) ** (m - k) for k in range(c + 1, m + 1))
        assert abs(binom_tail_gt(m, p, c) - brute) < 1e-12


def test_poisson_binomial_matches_brute_force():
    probs = [0.1, 0.5, 0.3, 0.8, 0.2]
    for C in range(0, 5):
        brute = 0.0
        for outcome in itertools.product([0, 1], repeat=len(probs)):
            pr = math.prod(p if o else 1 - p for p, o in zip(probs, outcome))
            if sum(outcome) > C:
                brute += pr
        assert abs(overflow_prob(C, probs) - brute) < 1e-12


def test_max_safe_inflight_and_wilson():
    assert max_safe_inflight(0, 0.3, 0.01, 100) == 0
    m = max_safe_inflight(10, 0.2, 0.01, 1000)
    assert binom_tail_gt(m, 0.2, 10) <= 0.01 < binom_tail_gt(m + 1, 0.2, 10)
    assert wilson_upper(20, 100, 1.645) > 0.2 and wilson_upper(0, 0, 1.645) == 1.0


def test_age_adjusted_probability_drops_with_ringing_time():
    tta = tuple(range(4, 16))
    assert p_remaining(0.5, 0, tta) > p_remaining(0.5, 10, tta) > p_remaining(0.5, 20, tta)
    assert p_remaining(0.5, 20, tta) < 0.05
