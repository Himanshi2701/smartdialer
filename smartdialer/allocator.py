"""
Call Allocator - turns a signed Permit into actual calls.

Per call, the order of operations is chosen so that a crash at ANY point
leaves a state the recovery sweeper can repair:

  progressive:                         predictive:
  1. txn: reserve agent (CAS)          1. txn: check unbound cap, claim borrower,
          claim borrower (CAS)                 create call RESERVED (no agent)
          create call RESERVED
  2. txn: call INITIATED, agent DIALING, record provider name, setup lease
          ("intent" is durable BEFORE we talk to the provider)
  3. provider.place_call(idempotency_key = call id)
  4. txn: store provider_call_id

  crash after 1 -> lease expires -> call CANCELLED, agent AVAILABLE, borrower PENDING
  crash after 2/3 -> call INITIATED without provider id -> reconcile with provider.lookup(key)
  ProviderRejected -> definitely no call -> FAILED, agent released, borrower retried with backoff
  ProviderTimeout  -> MAYBE a call -> keep agent, mark setup_uncertain, reconcile (never blind-retry:
                      a blind retry could ring the borrower twice and double-book the agent)
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from .config import DialerPolicy
from .lifecycle import Lifecycle
from .pacing import PREDICTIVE, PROGRESSIVE
from .providers.base import PlaceCallRequest, ProviderRejected, ProviderTimeout
from .providers.health import ProviderRouter
from .safety import Permit, PermitVerifier
from .states import CALL_TERMINAL, AgentState, CallState
from .store import Store


class PermitRefused(Exception):
    pass


class _Abort(Exception):
    pass


@dataclass
class AllocationResult:
    requested: int = 0
    placed: int = 0
    rejected: int = 0
    uncertain: int = 0
    cancelled: int = 0
    stopped: str = ""
    call_ids: list = field(default_factory=list)


class CallAllocator:
    def __init__(self, store: Store, lifecycle: Lifecycle, router: ProviderRouter, verifier: PermitVerifier,
                 worker_id: str, policy: DialerPolicy = DialerPolicy()):
        self.s = store
        self.lc = lifecycle
        self.router = router
        self.verifier = verifier
        self.worker_id = worker_id
        self.p = policy

    def _consume(self, permit: Permit):
        err = self.verifier.verify(permit, self.s.now())
        if err:
            raise PermitRefused(err)
        try:
            with self.s.txn():
                self.s.x("INSERT INTO permits(id, campaign_id, mode, count, issued_at, used_by, used_at) "
                         "VALUES (?,?,?,?,?,?,?)", (permit.permit_id, permit.campaign_id, permit.mode, permit.count,
                                                     permit.issued_at, self.worker_id, self.s.now()))
        except Exception as e:  # sqlite3.IntegrityError -> replay
            raise PermitRefused(f"permit already used: {e}")

    def execute(self, permit: Permit) -> AllocationResult:
        self._consume(permit)
        res = AllocationResult(requested=permit.count)
        for _ in range(permit.count):
            try:
                if permit.mode == PROGRESSIVE:
                    call_id = self._prepare_progressive(permit)
                else:
                    call_id = self._prepare_predictive(permit)
            except _Abort as a:
                res.stopped = str(a)
                break
            outcome = self._initiate(call_id)
            setattr(res, outcome, getattr(res, outcome) + 1)
            res.call_ids.append(call_id)
            if outcome in ("rejected", "cancelled") and self.router.pick(predictive=permit.mode == PREDICTIVE) is None:
                res.stopped = "no healthy provider left"
                break
        return res

    # --------------------------------------------------------------- step 1
    def _new_call(self, cid, borrower, agent_id, mode, permit_id):
        s = self.s
        now = s.now()
        call_id = f"call-{uuid.uuid4().hex[:12]}"
        s.x("INSERT INTO calls(id, campaign_id, borrower_id, agent_id, mode, state, idempotency_key, permit_id, "
            "lease_owner, lease_until, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (call_id, cid, borrower["id"], agent_id, mode, CallState.QUEUED.value, call_id, permit_id,
             self.worker_id, now + self.p.setup_lease_s, now))
        s.x("INSERT INTO call_transitions(call_id, from_state, to_state, at, source) VALUES (?,?,?,?,?)",
            (call_id, None, "QUEUED", now, self.worker_id))
        return call_id

    def _prepare_progressive(self, permit: Permit) -> str:
        s = self.s
        with s.txn():
            placeholder = f"pending-{uuid.uuid4().hex[:8]}"
            agent_id = s.reserve_agent(permit.campaign_id, self.worker_id, placeholder, self.p.agent_reserve_lease_s)
            if agent_id is None:
                raise _Abort("no available agent")
            borrower = s.claim_borrower(permit.campaign_id, self.worker_id, self.p.borrower_lease_s)
            if borrower is None:
                raise _Abort("no borrower due")       # whole txn rolls back -> agent stays AVAILABLE
            call_id = self._new_call(permit.campaign_id, borrower, agent_id, PROGRESSIVE, permit.permit_id)
            s.x("UPDATE agents SET current_call_id=? WHERE id=?", (call_id, agent_id))
            s.cas_call(call_id, CallState.RESERVED, self.worker_id)
            return call_id

    def _prepare_predictive(self, permit: Permit) -> str:
        s = self.s
        with s.txn():
            # Re-check the cap INSIDE the transaction: this is what makes two concurrent permit
            # holders unable to exceed the safety bound together.
            n = s.q1("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND agent_id IS NULL AND "
                     "state IN ('QUEUED','RESERVED','INITIATED','RINGING')", (permit.campaign_id,))["c"]
            if n >= permit.max_unbound_inflight:
                raise _Abort(f"unbound in-flight cap reached ({n} >= {permit.max_unbound_inflight})")
            borrower = s.claim_borrower(permit.campaign_id, self.worker_id, self.p.borrower_lease_s)
            if borrower is None:
                raise _Abort("no borrower due")
            call_id = self._new_call(permit.campaign_id, borrower, None, PREDICTIVE, permit.permit_id)
            s.cas_call(call_id, CallState.RESERVED, self.worker_id)
            return call_id

    # --------------------------------------------------------------- step 2-4
    def _initiate(self, call_id: str) -> str:
        s = self.s
        predictive = s.get_call(call_id)["agent_id"] is None
        provider = self.router.pick(predictive=predictive)
        if provider is None:
            self.lc.end_call(call_id, CallState.CANCELLED, "no healthy provider", self.worker_id)
            return "cancelled"
        try:
            with s.txn():
                call = s.get_call(call_id)
                now = s.now()
                if not s.cas_call(call_id, CallState.INITIATED, self.worker_id, expected=[CallState.RESERVED],
                                  provider=provider.name, initiated_at=now, lease_owner=self.worker_id,
                                  lease_until=now + self.p.setup_lease_s, last_event_at=now):
                    raise _Abort("call no longer RESERVED")
                if call["agent_id"]:
                    a = s.get_agent(call["agent_id"])
                    if a["current_call_id"] != call_id or not s.cas_agent(
                            call["agent_id"], AgentState.RESERVED, AgentState.DIALING, self.worker_id):
                        raise _Abort("agent disappeared before dialing")
        except _Abort as e:
            self.lc.end_call(call_id, CallState.CANCELLED, str(e), self.worker_id)
            return "cancelled"

        call = s.get_call(call_id)
        borrower = s.get_borrower(call["borrower_id"])
        req = PlaceCallRequest(call_id=call_id, idempotency_key=call["idempotency_key"], to_number=borrower["phone"],
                               agent_id=call["agent_id"], metadata={"campaign": call["campaign_id"]})
        t0 = s.now()
        try:
            result = provider.place_call(req)
        except ProviderRejected as e:
            self.router.record(provider.name, False, s.now() - t0)
            self.lc.end_call(call_id, CallState.FAILED, f"provider rejected: {e}", self.worker_id)
            return "rejected"
        except ProviderTimeout:
            self.router.record(provider.name, False, s.now() - t0)
            # Unknown outcome. Keep the agent held; the sweeper reconciles once the setup lease expires.
            s.set_call_fields(call_id, setup_uncertain=1)
            return "uncertain"
        self.router.record(provider.name, True, s.now() - t0)
        with s.txn():
            cur = s.get_call(call_id)
            s.set_call_fields(call_id, provider_call_id=result.provider_call_id, lease_owner=None, lease_until=None)
            if CallState(cur["state"]) in CALL_TERMINAL and cur["state"] == CallState.CANCELLED.value:
                # cancelled while we were talking to the provider (agent vanished) -> make sure it is gone
                s.enqueue_command(f"hangup:{call_id}", call_id, provider.name, "HANGUP",
                                  {"provider_call_id": result.provider_call_id,
                                   "idempotency_key": call["idempotency_key"]})
        return "placed"
