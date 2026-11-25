"""End-to-end smoke tests on the simulator (short runs)."""
from sim.engine import Scenario, Simulation


def _run(**kw):
    sim = Simulation(Scenario(**{"name": "t", "duration_s": 420, **kw}))
    return sim, sim.run(check_every=5)


def test_progressive_never_abandons():
    sim, s = _run(answer_rate=0.5, talk_mean_s=60, mode="PROGRESSIVE", agents=20)
    assert s["abandoned"] == 0 and s["invariant_violations"] == 0


def test_predictive_beats_progressive_at_low_answer_rate_safely():
    _, prog = _run(answer_rate=0.2, talk_mean_s=90, mode="PROGRESSIVE", agents=30)
    _, pred = _run(answer_rate=0.2, talk_mean_s=90, mode="PREDICTIVE", agents=30)
    assert pred["utilization"] > prog["utilization"]
    assert pred["abandon_rate"] <= 0.03
    assert pred["invariant_violations"] == 0


def test_three_workers_one_crashes_midway():
    def crash(sim):
        sim.crashed_workers.add("w2")
    sim, s = _run(answer_rate=0.3, talk_mean_s=60, mode="PREDICTIVE", agents=20, workers=3,
                  script=[(150, crash)])
    assert s["invariant_violations"] == 0
    assert s["connected"] > 20
    leaders = {r["owner"] for r in sim.store.q("SELECT DISTINCT used_by owner FROM permits")}
    assert len(leaders) >= 1


def test_messy_provider_only_disables_predictive():
    """Only Provider B (slow webhooks): predictive bridging would abandon calls, so the safety
    controller keeps the campaign in progressive mode."""
    sim, s = _run(answer_rate=0.3, talk_mean_s=60, mode="PREDICTIVE", agents=20, providers=("B",))
    assert set(s["safety"]) == {"FALLBACK_PROGRESSIVE"}
    assert s["abandoned"] == 0
