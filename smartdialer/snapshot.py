"""
A frozen, read-only view of a campaign at one instant, built straight from the DB.

Both the pacing engine and the safety controller receive the same snapshot.
It is a frozen dataclass: the pacer cannot "edit" what the safety controller sees.
The safety controller also never reads the pacer's own numbers (Proposal.rationale);
it re-derives its own conservative statistics from the raw counts in here.
"""
from __future__ import annotations

from dataclasses import dataclass

from .store import TERMINAL_SQL, Store


@dataclass(frozen=True)
class WindowStats:
    window_s: float
    answered: int          # calls answered within the window
    not_answered: int      # calls that rang out / were busy within the window

    @property
    def attempts(self) -> int:
        return self.answered + self.not_answered

    @property
    def rate(self) -> float:
        return self.answered / self.attempts if self.attempts else 0.0


@dataclass(frozen=True)
class Snapshot:
    now: float
    campaign_id: str
    active: bool
    # agents
    logged_in: int
    available: int
    reserved: int
    dialing: int
    connected: int
    wrap_up: int
    paused: int
    wrap_remaining_s: tuple            # forced remaining wrap time of each WRAP_UP agent
    connected_elapsed_s: tuple
    agent_drops_recent: int            # agents that went OFFLINE/PAUSED within the drop window
    # calls
    unbound_inflight: int              # predictive calls dialing/ringing with no agent yet
    unbound_ages_s: tuple              # age (s since dial) of each of those calls, newest first
    waiting_answered: int              # predictive calls answered and waiting (hold) for an agent
    bound_inflight: int                # progressive calls not yet connected (agent already held)
    # history
    short: WindowStats
    long: WindowStats
    avg_talk_s: float
    avg_wrap_s: float
    avg_time_to_answer_s: float
    p05_time_to_answer_s: float
    tta_sorted: tuple                  # observed dial->answer times (long window), sorted
    answered_abandon_window: int
    abandoned_abandon_window: int
    hist_answer_rate: float
    hist_talk_s: float
    fallback_until: float
    fallback_reason: str


def _window(store: Store, cid: str, now: float, w: float) -> WindowStats:
    a = store.q1("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND answered_at>=?", (cid, now - w))["c"]
    na = store.q1("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND state='NO_ANSWER' AND ended_at>=?",
                  (cid, now - w))["c"]
    return WindowStats(w, a, na)


def build_snapshot(store: Store, campaign_id: str, short_window_s=120.0, long_window_s=900.0,
                   abandon_window_s=900.0, drop_window_s=10.0, max_wrap_s=20.0) -> Snapshot:
    s, cid = store, campaign_id
    now = s.now()
    camp = s.q1("SELECT * FROM campaigns WHERE id=?", (cid,))
    counts = {r["state"]: r["c"] for r in s.q("SELECT state, COUNT(*) c FROM agents WHERE campaign_id=? "
                                               "GROUP BY state", (cid,))}
    wrap_rem = tuple(max(0.0, (r["wrap_deadline"] or now) - now) for r in
                     s.q("SELECT wrap_deadline FROM agents WHERE campaign_id=? AND state='WRAP_UP'", (cid,)))
    conn_el = tuple(now - r["state_since"] for r in
                    s.q("SELECT state_since FROM agents WHERE campaign_id=? AND state='CONNECTED'", (cid,)))
    drops = s.q1("SELECT COUNT(*) c FROM agent_transitions t JOIN agents a ON a.id=t.agent_id WHERE a.campaign_id=? "
                 "AND t.at>=? AND t.to_state IN ('OFFLINE','PAUSED') AND t.from_state NOT IN ('OFFLINE','PAUSED')",
                 (cid, now - drop_window_s))["c"]
    ages = tuple(now - (r["initiated_at"] or r["created_at"]) for r in
                 s.q("SELECT initiated_at, created_at FROM calls WHERE campaign_id=? AND agent_id IS NULL AND "
                     "state IN ('QUEUED','RESERVED','INITIATED','RINGING') ORDER BY created_at DESC", (cid,)))
    unbound = len(ages)
    waiting = s.q1("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND agent_id IS NULL AND state='ANSWERED'",
                   (cid,))["c"]
    bound = s.q1("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND agent_id IS NOT NULL AND "
                 "state IN ('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED')", (cid,))["c"]
    talk = s.q1("SELECT AVG(talk_s) a FROM calls WHERE campaign_id=? AND state='COMPLETED' AND ended_at>=? "
                "AND talk_s>0", (cid, now - long_window_s))["a"]
    tta = [r["d"] for r in s.q("SELECT answered_at - initiated_at d FROM calls WHERE campaign_id=? AND "
                               "answered_at>=? AND initiated_at IS NOT NULL ORDER BY d", (cid, now - long_window_s))]
    ans_w = s.q1("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND answered_at>=?", (cid, now - abandon_window_s))["c"]
    ab_w = s.q1("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND state='ABANDONED' AND ended_at>=?",
                (cid, now - abandon_window_s))["c"]
    logged_in = sum(v for k, v in counts.items() if k != "OFFLINE")
    return Snapshot(
        now=now, campaign_id=cid, active=bool(camp["active"]),
        logged_in=logged_in, available=counts.get("AVAILABLE", 0), reserved=counts.get("RESERVED", 0),
        dialing=counts.get("DIALING", 0), connected=counts.get("CONNECTED", 0), wrap_up=counts.get("WRAP_UP", 0),
        paused=counts.get("PAUSED", 0), wrap_remaining_s=wrap_rem, connected_elapsed_s=conn_el,
        agent_drops_recent=drops,
        unbound_inflight=unbound, unbound_ages_s=ages, waiting_answered=waiting, bound_inflight=bound,
        short=_window(s, cid, now, short_window_s), long=_window(s, cid, now, long_window_s),
        avg_talk_s=talk or camp["hist_talk_s"], avg_wrap_s=max_wrap_s,   # conservative: assume the full (forced) wrap-up
        avg_time_to_answer_s=(sum(tta) / len(tta)) if tta else 10.0,
        p05_time_to_answer_s=tta[int(len(tta) * 0.05)] if len(tta) >= 20 else 3.0,
        tta_sorted=tuple(tta),
        answered_abandon_window=ans_w, abandoned_abandon_window=ab_w,
        hist_answer_rate=camp["hist_answer_rate"], hist_talk_s=camp["hist_talk_s"],
        fallback_until=camp["safety_fallback_until"] or 0.0, fallback_reason=camp["safety_fallback_reason"] or "",
    )
