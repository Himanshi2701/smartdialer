"""
Pacing engines. They only PROPOSE. They have no reference to the allocator,
the providers or the safety controller, so they physically cannot place a call.

    ProgressivePacer : one free agent -> one call.
    PredictivePacer  : estimates how many calls to start now so that, by the time
                       they are answered, there is (on average) an agent free for each.

Output is a Proposal with a human-readable `rationale` - this answers the
question "why did the system decide to make this many calls right now?".
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .config import PacerConfig
from .snapshot import Snapshot
from .stats import p_remaining

PROGRESSIVE = "PROGRESSIVE"
PREDICTIVE = "PREDICTIVE"


@dataclass(frozen=True)
class Proposal:
    mode: str
    count: int
    rationale: dict = field(default_factory=dict)


class PacingEngine:
    mode: str

    def propose(self, snap: Snapshot) -> Proposal:
        raise NotImplementedError


class ProgressivePacer(PacingEngine):
    mode = PROGRESSIVE

    def propose(self, snap: Snapshot) -> Proposal:
        # Agents already RESERVED/DIALING are not AVAILABLE, so this can never exceed the agent count.
        n = max(0, snap.available - snap.waiting_answered)
        return Proposal(PROGRESSIVE, n, {"available_agents": snap.available,
                                         "formula": "calls = available agents"})


class PredictivePacer(PacingEngine):
    """
    Rule-based / statistical pacing.

      p        = Bayesian-smoothed answer rate:
                 (answered_recent + k * historical_rate) / (attempts_recent + k)
                 (k pseudo-observations stop the estimate jumping around on small samples
                  and give a sane value at campaign start)
      T        = expected time from dialing to answer (measured)
      free(T)  = agents free now
               + WRAP_UP agents whose forced wrap ends within T
               + CONNECTED agents expected to finish talk+wrap within T
                 (exponential talk-time assumption: P = 1 - exp(-(T - wrap)/avg_talk))
      owed     = answered calls waiting for an agent
               + expected answers from calls already ringing (sum of age-adjusted p_i,
                 see stats.p_remaining: a call ringing for 20s is unlikely to answer)
      need     = target_occupancy * free(T) - owed
      calls    = floor(need / p)       capped at max_dial_ratio * free(T)
    """
    mode = PREDICTIVE

    def __init__(self, cfg: PacerConfig = PacerConfig()):
        self.cfg = cfg

    def answer_rate(self, snap: Snapshot) -> float:
        k = self.cfg.prior_strength
        w = snap.short if snap.short.attempts >= 50 else snap.long
        return (w.answered + k * snap.hist_answer_rate) / (w.attempts + k)

    def propose(self, snap: Snapshot) -> Proposal:
        c = self.cfg
        p = max(0.02, min(1.0, self.answer_rate(snap)))
        T = c.lookahead_s or snap.avg_time_to_answer_s
        free_now = max(0, snap.available)
        from_wrap = sum(1 for r in snap.wrap_remaining_s if r <= T)
        q = 1 - math.exp(-max(0.0, T - snap.avg_wrap_s) / max(1.0, snap.avg_talk_s))
        from_talk = snap.connected * q
        free_T = free_now + from_wrap + from_talk
        # calls already ringing: each counts with its age-adjusted chance of still being answered
        exp_ringing = sum(p_remaining(p, a, snap.tta_sorted) for a in snap.unbound_ages_s)
        owed = snap.waiting_answered + exp_ringing
        need = c.target_occupancy * free_T - owed
        raw = need / p if need > 0 else 0.0
        cap = c.max_dial_ratio * free_T
        n = int(max(0, math.floor(min(raw, cap))))
        return Proposal(PREDICTIVE, n, {
            "answer_rate_est": round(p, 3),
            "window_used": "short" if snap.short.attempts >= 50 else "long",
            "time_to_answer_s": round(T, 1),
            "free_now": free_now, "free_from_wrap": from_wrap, "free_from_talk": round(from_talk, 2),
            "free_within_T": round(free_T, 2),
            "owed_waiting": snap.waiting_answered, "ringing_unbound": snap.unbound_inflight,
            "expected_answers_from_ringing": round(exp_ringing, 2),
            "need": round(need, 2), "raw_calls": round(raw, 2), "pacer_cap": round(cap, 1),
            "formula": "calls = floor((target_occ*free_within_T - owed) / p)",
        })
