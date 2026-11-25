"""
Provider event handling: inbox -> idempotent apply. Plus the outbox dispatcher
and the hold manager for answered predictive calls.

Defence in depth against misbehaving providers:
  Layer 1 - dedupe: UNIQUE(provider, provider_event_id) in the inbox. An exact
            duplicate is never applied twice.
  Layer 2 - monotonic state machine: a call only moves forward (CALL_RANK).
            A re-sent event with a new id, or an old event arriving late, maps to
            a transition that is not allowed -> recorded as 'stale', no effect.
  Layer 3 - terminal states are sticky: once COMPLETED, nothing changes it.
  Layer 4 - every apply is ONE transaction including "mark event processed".
            A crash mid-apply rolls back everything and the event is retried.

Examples:
  ANSWERED, ANSWERED, ANSWERED, COMPLETED -> CONNECTED once, then COMPLETED. Agent -> WRAP_UP once.
  COMPLETED, ANSWERED, RINGING            -> COMPLETED (or NO_ANSWER/ABANDONED by payload); the
                                             late ANSWERED and RINGING are stale no-ops.
"""
from __future__ import annotations

import json
from typing import Iterable

from .config import DialerPolicy
from .lifecycle import Lifecycle
from .providers.base import (ANSWERED, COMPLETED, FAILED, NO_ANSWER, RINGING, ProviderError, ProviderEvent)
from .providers.health import ProviderRouter
from .states import CALL_TERMINAL, AgentState, CallState
from .store import Store


class EventProcessor:
    def __init__(self, store: Store, lifecycle: Lifecycle, policy: DialerPolicy = DialerPolicy(),
                 worker_id: str = "w", on_lag=None):
        self.s = store
        self.lc = lifecycle
        self.p = policy
        self.worker_id = worker_id
        self.on_lag = on_lag            # callback(provider, lag_s) -> feeds provider health
        self.counters = dict(ingested=0, duplicates=0, applied=0, stale=0, unknown=0, late_answer_after_cancel=0,
                             errors=0)

    # ---------------------------------------------------------------- inbox
    def ingest(self, events: Iterable[ProviderEvent]) -> None:
        for e in events:
            new = self.s.ingest_event(e.provider, e.event_id, e.type, e.provider_call_id, e.call_id, e.ts, e.payload)
            self.counters["ingested" if new else "duplicates"] += 1
            if new and self.on_lag:
                self.on_lag(e.provider, self.s.now() - e.ts)

    def process_pending(self, limit: int = 5000) -> int:
        rows = self.s.q("SELECT * FROM provider_events WHERE processed_at IS NULL ORDER BY provider_ts, id LIMIT ?",
                        (limit,))
        for r in rows:
            try:
                with self.s.txn():
                    # claim the row inside the same txn: two workers can't both apply it
                    if self.s.x("UPDATE provider_events SET processed_at=? WHERE id=? AND processed_at IS NULL",
                                (self.s.now(), r["id"])) == 0:
                        continue
                    outcome = self._apply(r)
                    self.s.x("UPDATE provider_events SET outcome=? WHERE id=?", (outcome, r["id"]))
                self.counters[outcome if outcome in self.counters else "applied"] += 1
            except Exception as ex:  # poison event: record and move on, never block the queue
                self.counters["errors"] += 1
                with self.s.txn():
                    self.s.x("UPDATE provider_events SET processed_at=?, outcome=? WHERE id=?",
                             (self.s.now(), f"error: {ex}", r["id"]))
        return len(rows)

    # ---------------------------------------------------------------- apply
    def _find_call(self, r):
        if r["call_id"]:
            c = self.s.get_call(r["call_id"])
            if c:
                return c
        if r["provider_call_id"]:
            return self.s.q1("SELECT * FROM calls WHERE provider=? AND provider_call_id=?",
                             (r["provider"], r["provider_call_id"]))
        return None

    def _apply(self, r) -> str:
        s, lc = self.s, self.lc
        src = f"evt:{r['provider']}:{r['provider_event_id']}"
        call = self._find_call(r)
        if call is None:
            # An event for a call we have no record of. Safest action: make sure it is hung up.
            if r["provider_call_id"]:
                s.enqueue_command(f"hangup-unknown:{r['provider']}:{r['provider_call_id']}", r["provider_call_id"],
                                  r["provider"], "HANGUP", {"provider_call_id": r["provider_call_id"]})
            return "unknown"
        cid = call["id"]
        if not call["provider_call_id"] and r["provider_call_id"]:
            s.set_call_fields(cid, provider_call_id=r["provider_call_id"], provider=r["provider"])  # adopt
            call = s.get_call(cid)
        st = CallState(call["state"])
        typ = r["type"]
        payload = json.loads(r["payload"] or "{}")
        ts = r["provider_ts"]
        now = s.now()

        if st in CALL_TERMINAL:
            if typ == ANSWERED and st == CallState.CANCELLED:
                return "late_answer_after_cancel"   # tracked: we cancelled but the borrower picked up first
            return "stale"
        s.set_call_fields(cid, last_event_at=now)

        if typ == RINGING:
            return "applied" if s.cas_call(cid, CallState.RINGING, src, ringing_at=ts) else "stale"

        if typ == ANSWERED:
            if st in (CallState.ANSWERED, CallState.CONNECTED):
                return "stale"
            if call["agent_id"]:
                # progressive: agent already held; the provider bridges to it directly
                agent = s.get_agent(call["agent_id"])
                s.cas_call(cid, CallState.ANSWERED, src, answered_at=ts)
                if agent and agent["current_call_id"] == cid and agent["state"] in ("DIALING", "RESERVED"):
                    if agent["state"] == "RESERVED":
                        s.cas_agent(agent["id"], AgentState.RESERVED, AgentState.DIALING, src)
                    s.cas_agent(agent["id"], AgentState.DIALING, AgentState.CONNECTED, src)
                    s.cas_call(cid, CallState.CONNECTED, src, connected_at=now)
                    return "applied"
                # the held agent is gone (should have been swapped/cancelled) -> treat like predictive
                s.set_call_fields(cid, agent_id=None)
            else:
                s.cas_call(cid, CallState.ANSWERED, src, answered_at=ts, hold_until=now + self.p.max_hold_s)
            if lc.bind_free_agent(cid, src) is None:
                s.set_call_fields(cid, hold_until=now + self.p.max_hold_s)   # wait for an agent (hold)
            return "applied"

        if typ == COMPLETED:
            answered = bool(payload.get("answered"))
            bridged = bool(payload.get("bridged"))
            talk = float(payload.get("talk_s") or 0.0)
            if st == CallState.CONNECTED:
                if bridged:
                    lc.end_call(cid, CallState.COMPLETED, payload.get("cause", "normal"), src, talk_s=talk)
                else:
                    lc.end_call(cid, CallState.ABANDONED, "bridge never happened", src)
            elif st == CallState.ANSWERED:
                lc.end_call(cid, CallState.ABANDONED, "borrower hung up while waiting for an agent", src)
            else:  # QUEUED/RESERVED/INITIATED/RINGING: we missed ANSWERED (late / lost)
                if answered:
                    s.set_call_fields(cid, answered_at=call["answered_at"] or ts)
                    if call["agent_id"] and bridged:
                        lc.end_call(cid, CallState.COMPLETED, "completed (answer event late)", src, talk_s=talk)
                    else:
                        lc.end_call(cid, CallState.ABANDONED, "answered but never connected", src)
                else:
                    lc.end_call(cid, CallState.NO_ANSWER, payload.get("cause", "no_answer"), src)
            return "applied"

        if typ == NO_ANSWER:
            return "applied" if lc.end_call(cid, CallState.NO_ANSWER, payload.get("cause", "no_answer"), src) \
                else "stale"
        if typ == FAILED:
            return "applied" if lc.end_call(cid, CallState.FAILED, payload.get("cause", "failed"), src) else "stale"
        return "stale"


class HoldManager:
    """Predictive calls that were answered while no agent was free wait here for max_hold_s.
    Served before any new dialing. If nobody frees up in time -> ABANDONED (counted) + hangup."""

    def __init__(self, store: Store, lifecycle: Lifecycle, worker_id: str = "w"):
        self.s = store
        self.lc = lifecycle
        self.worker_id = worker_id

    def run(self, campaign_id: str) -> dict:
        now = self.s.now()
        out = dict(bound=0, abandoned=0)
        for r in self.s.q("SELECT id, hold_until FROM calls WHERE campaign_id=? AND state='ANSWERED' AND "
                          "agent_id IS NULL ORDER BY answered_at", (campaign_id,)):
            if self.lc.bind_free_agent(r["id"], f"hold:{self.worker_id}"):
                out["bound"] += 1
            elif r["hold_until"] is not None and now >= r["hold_until"]:
                if self.lc.end_call(r["id"], CallState.ABANDONED, "no agent within max hold", f"hold:{self.worker_id}",
                                    hangup=True):
                    out["abandoned"] += 1
        return out


class OutboxDispatcher:
    """Delivers BRIDGE / HANGUP commands at-least-once. Provider commands are idempotent."""

    def __init__(self, store: Store, router: ProviderRouter, max_attempts: int = 20):
        self.s = store
        self.router = router
        self.max_attempts = max_attempts

    def run(self, limit: int = 500) -> int:
        now = self.s.now()
        done = 0
        for r in self.s.q("SELECT * FROM outbox WHERE status='PENDING' AND next_at<=? ORDER BY id LIMIT ?",
                          (now, limit)):
            args = json.loads(r["args"] or "{}")
            names = [r["provider"]] if r["provider"] else list(self.router.order)
            try:
                for n in names:
                    prov = self.router.get(n)
                    if r["command"] == "BRIDGE":
                        prov.bridge(args.get("provider_call_id"), args["agent_id"], args.get("idempotency_key"))
                    elif r["command"] == "HANGUP":
                        prov.hangup(args.get("provider_call_id"), args.get("idempotency_key"))
                with self.s.txn():
                    self.s.x("UPDATE outbox SET status='DONE', attempts=attempts+1 WHERE id=?", (r["id"],))
                done += 1
            except ProviderError:
                att = r["attempts"] + 1
                with self.s.txn():
                    self.s.x("UPDATE outbox SET attempts=?, next_at=?, status=? WHERE id=?",
                             (att, now + min(30.0, 0.5 * 2 ** att), "DEAD" if att >= self.max_attempts else "PENDING",
                              r["id"]))
        return done
