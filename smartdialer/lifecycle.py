"""
Domain transitions that touch more than one entity (call + agent + borrower).

Every function here runs inside ONE database transaction, so either all of
"call ended, agent released, borrower rescheduled, hangup queued" happens, or
none of it does. A crash in the middle cannot leave an agent stuck RESERVED
for a call that is already COMPLETED.
"""
from __future__ import annotations

from typing import Optional

from .config import DialerPolicy
from .states import CALL_TERMINAL, AgentState, CallState
from .store import Store


class Lifecycle:
    def __init__(self, store: Store, policy: DialerPolicy = DialerPolicy()):
        self.s = store
        self.p = policy

    # ------------------------------------------------------------ call end
    def end_call(self, call_id: str, to: CallState, reason: str, source: str,
                 talk_s: Optional[float] = None, hangup: bool = False) -> bool:
        """Move a call to a terminal state and apply all side effects atomically."""
        assert to in CALL_TERMINAL
        s = self.s
        now = s.now()
        with s.txn():
            call = s.get_call(call_id)
            if call is None:
                return False
            fields = dict(ended_at=now, end_reason=reason, lease_owner=None, lease_until=None, hold_until=None)
            if talk_s is not None:
                fields["talk_s"] = talk_s
            if not s.cas_call(call_id, to, source, **fields):
                return False            # already terminal (duplicate / late event) -> nothing to do
            self._release_agent_after(call, to, source)
            self._reschedule_borrower(call, to)
            if hangup:
                # Kill the provider leg if it might exist. Idempotent: keyed by call id.
                s.enqueue_command(f"hangup:{call_id}", call_id, call["provider"], "HANGUP",
                                  {"provider_call_id": call["provider_call_id"],
                                   "idempotency_key": call["idempotency_key"]})
            return True

    def _release_agent_after(self, call, to: CallState, source: str):
        s = self.s
        agent_id = call["agent_id"]
        if not agent_id:
            return
        agent = s.get_agent(agent_id)
        if agent is None or agent["current_call_id"] != call["id"]:
            return  # agent already moved on (e.g. swapped out); never touch someone else's agent
        st = AgentState(agent["state"])
        talked = to == CallState.COMPLETED or st == AgentState.CONNECTED
        if st in (AgentState.CONNECTED,) or (st == AgentState.DIALING and talked):
            s.cas_agent(agent_id, st, AgentState.WRAP_UP, source, current_call_id=None,
                        lease_owner=None, lease_until=None, wrap_deadline=s.now() + self.p.max_wrap_s)
        elif st in (AgentState.RESERVED, AgentState.DIALING):
            s.cas_agent(agent_id, st, AgentState.AVAILABLE, source, current_call_id=None,
                        lease_owner=None, lease_until=None)
        else:  # OFFLINE / PAUSED / WRAP_UP: just detach
            s.x("UPDATE agents SET current_call_id=NULL, lease_owner=NULL, lease_until=NULL, "
                "version=version+1 WHERE id=? AND current_call_id=?", (agent_id, call["id"]))

    def _reschedule_borrower(self, call, to: CallState):
        s, p = self.s, self.p
        now = s.now()
        b = s.get_borrower(call["borrower_id"])
        if b is None:
            return
        attempts, pf = b["attempts"], b["provider_failures"]
        if to == CallState.COMPLETED:
            state, nxt = "DONE", now
        elif to == CallState.NO_ANSWER:
            attempts += 1
            state, nxt = ("EXHAUSTED" if attempts >= p.max_attempts else "PENDING"), now + p.no_answer_retry_s
        elif to == CallState.ABANDONED:
            attempts += 1
            state, nxt = "PENDING", now + p.abandon_cooldown_s
        elif to == CallState.FAILED:
            # Provider/our fault: do NOT burn one of the borrower's attempts.
            pf += 1
            state = "PENDING"
            nxt = now + min(p.provider_fail_retry_max_s, p.provider_fail_retry_base_s * (2 ** min(pf, 10)))
        else:  # CANCELLED by us
            rang = call["ringing_at"] is not None
            state, nxt = "PENDING", now + (60.0 if rang else 1.0)
        s.x("UPDATE borrowers SET state=?, attempts=?, provider_failures=?, next_attempt_at=?, last_outcome=?, "
            "lease_owner=NULL, lease_until=NULL, version=version+1 WHERE id=?",
            (state, attempts, pf, nxt, to.value, b["id"]))

    # ------------------------------------------------------------ answers
    def bind_free_agent(self, call_id: str, source: str) -> Optional[str]:
        """Predictive: borrower answered; atomically grab the longest-idle agent.

        Returns the agent id, or None if no agent is free (caller puts the call on hold).
        The BRIDGE command is written to the outbox in the same transaction, so
        a crash right after this commit still results in the bridge being sent.
        """
        s = self.s
        now = s.now()
        with s.txn():
            call = s.get_call(call_id)
            if call is None or CallState(call["state"]) not in (CallState.INITIATED, CallState.RINGING,
                                                                  CallState.ANSWERED):
                return None
            row = s.q1("SELECT id FROM agents WHERE campaign_id=? AND state='AVAILABLE' ORDER BY state_since LIMIT 1",
                       (call["campaign_id"],))
            if row is None:
                return None
            if not s.cas_agent(row["id"], AgentState.AVAILABLE, AgentState.CONNECTED, source,
                               current_call_id=call_id, lease_owner=None, lease_until=None):
                return None
            if CallState(call["state"]) != CallState.ANSWERED:
                s.cas_call(call_id, CallState.ANSWERED, source, answered_at=call["answered_at"] or now)
            ok = s.cas_call(call_id, CallState.CONNECTED, source, agent_id=row["id"], connected_at=now,
                            hold_until=None)
            if not ok:
                raise RuntimeError("bind failed after agent reserved")  # rolls back the agent CAS too
            s.enqueue_command(f"bridge:{call_id}", call_id, call["provider"], "BRIDGE",
                              {"provider_call_id": call["provider_call_id"], "agent_id": row["id"],
                               "idempotency_key": call["idempotency_key"]})
            return row["id"]

    # ------------------------------------------------------------ agents
    def agent_set_state(self, agent_id: str, to: AgentState, source: str = "agent") -> bool:
        """Agent-driven state change (login, pause, logout, ready).

        If an agent disappears (PAUSED/OFFLINE) while RESERVED/DIALING for a call
        that has not been answered yet, we first try to swap in another free
        agent; if none is free we cancel the call and hang it up BEFORE the
        borrower answers (cancelling a ringing call is safe; abandoning an
        answered one is not).
        """
        s = self.s
        with s.txn():
            agent = s.get_agent(agent_id)
            if agent is None:
                return False
            frm = AgentState(agent["state"])
            if frm == to:
                return True
            if to not in {AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE}:
                raise ValueError("agents can only set AVAILABLE / PAUSED / OFFLINE themselves")
            call_id = agent["current_call_id"]
            ok = s.cas_agent(agent_id, frm, to, source,
                             **({"current_call_id": None, "lease_owner": None, "lease_until": None}
                                if frm in (AgentState.RESERVED, AgentState.DIALING) else {}),
                             **({"wrap_deadline": None} if to == AgentState.AVAILABLE else {}))
            if not ok:
                return False
            if call_id and frm in (AgentState.RESERVED, AgentState.DIALING):
                self._agent_vanished_during_setup(call_id, agent_id, source)
            return True

    def _agent_vanished_during_setup(self, call_id: str, old_agent: str, source: str):
        s = self.s
        call = s.get_call(call_id)
        if call is None or CallState(call["state"]) in CALL_TERMINAL:
            return
        st = CallState(call["state"])
        if st in (CallState.ANSWERED, CallState.CONNECTED):
            return
        # try to swap in another agent (same state the old agent had for this call)
        target = AgentState.DIALING if st in (CallState.INITIATED, CallState.RINGING) else AgentState.RESERVED
        new_agent = s.reserve_agent(call["campaign_id"], f"swap:{source}", call_id, self.p.agent_reserve_lease_s)
        if new_agent:
            if target == AgentState.DIALING:
                s.cas_agent(new_agent, AgentState.RESERVED, AgentState.DIALING, f"swap:{source}")
            s.set_call_fields(call_id, agent_id=new_agent)
            if call["provider_call_id"]:
                s.enqueue_command(f"rebind:{call_id}:{new_agent}", call_id, call["provider"], "BRIDGE",
                                  {"provider_call_id": call["provider_call_id"], "agent_id": new_agent,
                                   "idempotency_key": call["idempotency_key"]})
            return
        self.end_call(call_id, CallState.CANCELLED, f"agent {old_agent} disappeared during setup", source,
                      hangup=st in (CallState.INITIATED, CallState.RINGING))

    def finish_expired_wrapups(self, campaign_id: str) -> int:
        """System-enforced end of WRAP_UP. This makes agent return deterministic."""
        s = self.s
        now = s.now()
        n = 0
        for r in s.q("SELECT id FROM agents WHERE campaign_id=? AND state='WRAP_UP' AND wrap_deadline<=?",
                     (campaign_id, now)):
            if s.cas_agent(r["id"], AgentState.WRAP_UP, AgentState.AVAILABLE, "wrap-timeout", wrap_deadline=None):
                n += 1
        return n
