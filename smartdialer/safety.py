"""
Safety Controller - the only component that can authorise calls.

    Pacing Engine --Proposal--> SafetyController --signed Permit--> CallAllocator --> Provider

Why the pacer cannot bypass it:
  1. The pacer gets no reference to the allocator or to any provider.
  2. The allocator only accepts a Permit carrying an HMAC signature made with a
     secret key that exists only inside the SafetyController instance. A pacer
     (or any other code) constructing its own Permit fails verification.
  3. Permits are single use (stored in the `permits` table with a PRIMARY KEY)
     and expire after a few seconds, so a permit cannot be replayed or hoarded.
  4. SafetyConfig is a frozen dataclass and there is no "disable" flag anywhere.
     A Proposal contains only (mode, count, rationale) - nothing in it can
     switch a check off. The rationale is logged but never read for decisions.
  5. Even with a valid permit, the allocator re-checks the predictive in-flight
     cap inside the same DB transaction that creates the call, so two workers
     holding permits at the same time cannot add up to more than the cap.

Decision rules (in order):
  R1 campaign inactive                       -> REJECT
  R2 no healthy provider                     -> REJECT  (half-open -> 1 probe call)
  R3 fallback triggers -> FALLBACK_PROGRESSIVE (deterministic mode):
       - agent availability dropped sharply (also: shed excess ringing calls)
       - abandon budget used up in the rolling window
       - not enough data (cold start)
       - an earlier trigger is still cooling down
  R4 predictive probability bound:
       choose the largest total M of unbound in-flight calls such that
           P( Binomial(M, p_safe) > C ) <= epsilon
       where p_safe = pessimistic (upper-confidence) answer rate over short AND
       long windows, and C = agents free now (minus callers already waiting)
       + WRAP_UP agents whose forced wrap ends before the earliest plausible answer.
  R5 hard caps: max lines per free agent, max new calls per tick (provider CPS)
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import secrets
import uuid
from dataclasses import asdict, dataclass, field
from typing import Optional

from .config import DialerPolicy, SafetyConfig
from .pacing import PREDICTIVE, PROGRESSIVE, Proposal
from .snapshot import Snapshot
from .stats import OverflowDist, p_remaining
from .store import Store

APPROVE, REDUCE, REJECT, FALLBACK = "APPROVE", "REDUCE", "REJECT", "FALLBACK_PROGRESSIVE"


@dataclass(frozen=True)
class Permit:
    permit_id: str
    campaign_id: str
    mode: str
    count: int
    max_unbound_inflight: int
    issued_at: float
    expires_at: float
    signature: str = ""

    def payload(self) -> bytes:
        d = asdict(self)
        d.pop("signature")
        return json.dumps(d, sort_keys=True).encode()


@dataclass(frozen=True)
class Decision:
    action: str
    proposed: int
    approved: int
    mode: str
    permit: Optional[Permit]
    shed: int = 0
    reasons: dict = field(default_factory=dict)


class PermitVerifier:
    """Handed to the allocator. Can verify signatures, cannot create them."""

    def __init__(self, key: bytes):
        self.__key = key

    def verify(self, permit: Permit, now: float) -> Optional[str]:
        expected = hmac.new(self.__key, permit.payload(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, permit.signature):
            return "bad signature (permit not issued by the safety controller)"
        if now > permit.expires_at:
            return "permit expired"
        if permit.count < 0:
            return "negative count"
        return None


# ----------------------------------------------------------------- statistics
def wilson_upper(successes: int, n: int, z: float) -> float:
    """Upper bound of the Wilson score interval for a proportion."""
    if n == 0:
        return 1.0
    p = successes / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return min(1.0, (centre + margin) / denom)


def binom_tail_gt(m: int, p: float, c: int) -> float:
    """P(X > c) for X ~ Binomial(m, p). Exact, computed in log space."""
    if c < 0:
        return 1.0
    if m <= c:
        return 0.0
    if p <= 0:
        return 0.0
    if p >= 1:
        return 1.0
    # cdf = sum_{k=0..c} C(m,k) p^k (1-p)^(m-k)
    logp, logq = math.log(p), math.log1p(-p)
    log_pmf = m * logq  # k = 0
    cdf = math.exp(log_pmf)
    for k in range(1, c + 1):
        log_pmf += math.log(m - k + 1) - math.log(k) + logp - logq
        cdf += math.exp(log_pmf)
    return max(0.0, 1.0 - cdf)


def max_safe_inflight(capacity: int, p: float, eps: float, hard_max: int) -> int:
    """Largest M <= hard_max with P(Bin(M, p) > capacity) <= eps. Binary search (tail grows with M)."""
    if capacity <= 0:
        return 0
    lo, hi = capacity, max(capacity, hard_max)  # M = capacity is always safe
    if binom_tail_gt(hi, p, capacity) <= eps:
        return hi
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if binom_tail_gt(mid, p, capacity) <= eps:
            lo = mid
        else:
            hi = mid - 1
    return lo


# ------------------------------------------------------------------ controller
class SafetyController:
    def __init__(self, store: Store, router_health, cfg: SafetyConfig = SafetyConfig(),
                 policy: DialerPolicy = DialerPolicy()):
        self.s = store
        self._router_health = router_health      # callable returning a health dict (read-only)
        self.cfg = cfg
        self.policy = policy
        self.__key = secrets.token_bytes(32)
        self.verifier = PermitVerifier(self.__key)

    def _sign(self, permit: Permit) -> Permit:
        sig = hmac.new(self.__key, permit.payload(), hashlib.sha256).hexdigest()
        return Permit(**{**asdict(permit), "signature": sig})

    def _permit(self, snap: Snapshot, mode: str, count: int, max_unbound: int) -> Permit:
        return self._sign(Permit(permit_id=str(uuid.uuid4()), campaign_id=snap.campaign_id, mode=mode,
                                 count=int(count), max_unbound_inflight=int(max_unbound), issued_at=snap.now,
                                 expires_at=snap.now + self.cfg.permit_ttl_s))

    def _set_fallback(self, snap: Snapshot, reason: str):
        with self.s.txn():
            self.s.x("UPDATE campaigns SET safety_fallback_until=?, safety_fallback_reason=? WHERE id=?",
                     (snap.now + self.cfg.fallback_cooldown_s, reason, snap.campaign_id))

    def p_safe(self, snap: Snapshot) -> tuple[float, dict]:
        z = self.cfg.z_upper
        ps = wilson_upper(snap.short.answered, snap.short.attempts, z) if snap.short.attempts else 0.0
        pl = wilson_upper(snap.long.answered, snap.long.attempts, z) if snap.long.attempts else 0.0
        p = max(ps, pl)  # pessimistic: if the answer rate just jumped, the short window catches it
        return p, {"p_ucb_short": round(ps, 3), "p_ucb_long": round(pl, 3), "p_safe": round(p, 3),
                   "n_short": snap.short.attempts, "n_long": snap.long.attempts}

    # -------------------------------------------------------------- evaluate
    def evaluate(self, proposal: Proposal, snap: Snapshot) -> Decision:
        c = self.cfg
        requested = proposal.count if isinstance(proposal.count, int) and proposal.count > 0 else 0
        mode = proposal.mode if proposal.mode in (PROGRESSIVE, PREDICTIVE) else PROGRESSIVE
        reasons: dict = {"proposed_mode": mode}

        # R1
        if not snap.active:
            return self._log(snap, Decision(REJECT, requested, 0, mode, None, reasons={**reasons,
                                                                                       "rule": "R1 campaign inactive"}))
        # R2
        health = self._router_health()
        reasons["providers"] = health["states"]
        probe_only = False
        if health["closed"] == 0:
            if health["half_open"] == 0:
                return self._log(snap, Decision(REJECT, requested, 0, mode, None,
                                                reasons={**reasons, "rule": "R2 no healthy provider"}))
            probe_only = True

        # Deterministic capacity: agents that are free now and not owed to a waiting caller.
        capacity_now = snap.available - snap.waiting_answered
        # R3 fallback triggers (only matter for predictive)
        trigger = None
        shed = 0
        base = snap.logged_in + snap.agent_drops_recent
        if base and snap.agent_drops_recent >= max(2, c.agent_drop_fraction * base):
            trigger = f"agent drop: {snap.agent_drops_recent} of {base} agents left in {c.agent_drop_window_s:.0f}s"
        abandon_rate = (snap.abandoned_abandon_window / snap.answered_abandon_window
                        if snap.answered_abandon_window else 0.0)
        reasons["abandon_rate_window"] = round(abandon_rate, 4)
        reasons["abandons_window"] = snap.abandoned_abandon_window
        if trigger is None and (snap.abandoned_abandon_window >= c.abandon_hard_count or
                                (snap.answered_abandon_window >= c.abandon_min_answered
                                 and abandon_rate >= c.abandon_rate_limit)):
            trigger = (f"abandon budget: {snap.abandoned_abandon_window} abandons / "
                       f"{snap.answered_abandon_window} answered in {c.abandon_window_s:.0f}s")
        if trigger:
            self._set_fallback(snap, trigger)
        if mode == PREDICTIVE and trigger is None:
            if not health.get("predictive_ok", []):
                trigger = (f"no provider with webhook lag fast enough for predictive bridging "
                           f"(p95 lag: {health.get('event_lag_p95')})")
            elif snap.fallback_until > snap.now:
                trigger = f"cooling down after: {snap.fallback_reason}"
            elif snap.long.attempts < c.min_samples:
                trigger = f"cold start: only {snap.long.attempts} resolved attempts (< {c.min_samples})"

        # Capacity C for the probability bound: agents free now (not owed to a waiting caller)
        # + WRAP_UP agents whose FORCED wrap end comes before the earliest plausible answer.
        p_safe, preasons = self.p_safe(snap)
        reasons.update(preasons)
        p_floor = max(p_safe, 0.05)
        guaranteed_soon = sum(1 for r in snap.wrap_remaining_s
                              if r <= snap.p05_time_to_answer_s + self.policy.max_hold_s)
        C = max(0, capacity_now + guaranteed_soon)
        # Each ringing call's chance of still answering depends on how long it has been ringing.
        ring_probs = sorted(p_remaining(p_floor, a, snap.tta_sorted) for a in snap.unbound_ages_s)

        # Shedding: if the ringing pool is now unsafe for the capacity we have, cancel the excess
        # BEFORE those borrowers answer. Cancelling an unanswered call is harmless; abandoning is not.
        # Hysteresis: admit new calls at epsilon, but only evict ringing calls when the risk is
        # clearly higher (shed_epsilon), or at epsilon right after an agent drop. Otherwise normal
        # noise in the estimate would make us cancel and re-dial in a loop.
        kept = OverflowDist(C)
        if ring_probs:
            shed_eps = c.epsilon if (trigger and trigger.startswith("agent drop")) else c.shed_epsilon
            m = 0
            for pr in ring_probs:              # lowest risk first; the highest-risk ones get shed
                trial = kept.copy()
                trial.add(pr)
                if trial.overflow() > shed_eps:
                    break
                kept, m = trial, m + 1
            shed = len(ring_probs) - m
            if shed:
                reasons["shed"] = (f"P(overflow) with all {len(ring_probs)} ringing calls > {shed_eps}; "
                                   f"cancel {shed} newest (capacity {C})")

        if mode == PROGRESSIVE or trigger:
            # Deterministic: even if EVERY unbound call still ringing answers, there is an agent for it,
            # and every new call has its own agent reserved before dialing.
            allowed = max(0, snap.available - snap.waiting_answered - max(0, snap.unbound_inflight - shed))
            approved = allowed if mode == PREDICTIVE else min(requested, allowed)
            if probe_only:
                approved = min(approved, 1)
            action = FALLBACK if (mode == PREDICTIVE and trigger) else (
                APPROVE if approved == requested else (REDUCE if approved > 0 else REJECT))
            reasons.update(rule="R3 deterministic (progressive)" if not trigger else f"R3 fallback: {trigger}",
                           deterministic_allowed=allowed)
            permit = self._permit(snap, PROGRESSIVE, approved, 0) if approved > 0 else None
            return self._log(snap, Decision(action, requested, approved, PROGRESSIVE, permit, shed, reasons))

        # R4 predictive probability bound (Poisson-binomial over every in-flight call + new ones)
        hard_total = int(c.max_lines_per_agent * C)
        p_new = p_remaining(p_floor, 0.0, snap.tta_sorted)
        od = kept
        bound_allowed = 0
        in_flight = len(ring_probs) - shed
        limit = min(requested, c.max_new_calls_per_tick, max(0, hard_total - in_flight))
        while bound_allowed < limit:
            trial = od.copy()
            trial.add(p_new)
            if trial.overflow() > c.epsilon:
                break
            od, bound_allowed = trial, bound_allowed + 1
        approved = bound_allowed
        if probe_only:
            approved = min(approved, 1)
        reasons.update(rule="R4 Poisson-binomial bound + R5 caps", capacity_now=capacity_now,
                       guaranteed_from_wrap=guaranteed_soon, capacity_C=C, in_flight_unbound=in_flight,
                       p_new_call=round(p_new, 3), expected_answers_in_flight=round(sum(ring_probs), 2),
                       hard_cap_total=hard_total, epsilon=c.epsilon,
                       p_overflow_after=round(od.overflow(), 4))
        if probe_only:
            reasons["probe_only"] = True
        action = APPROVE if approved == requested else (REDUCE if approved > 0 else REJECT)
        permit = self._permit(snap, PREDICTIVE, approved, in_flight + approved) if approved > 0 else None
        return self._log(snap, Decision(action, requested, approved, PREDICTIVE, permit, shed, reasons))

    def _log(self, snap: Snapshot, d: Decision) -> Decision:
        with self.s.txn():
            self.s.x("INSERT INTO safety_log(at, campaign_id, proposed, approved, action, mode, reasons) "
                     "VALUES (?,?,?,?,?,?,?)", (snap.now, snap.campaign_id, d.proposed, d.approved, d.action, d.mode,
                                                json.dumps(d.reasons, default=str)))
        return d
