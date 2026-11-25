"""
DialerWorker: one stateless worker. Run N of them against the same DB.

tick():
  1. heartbeat
  2. ingest provider webhooks into the inbox (durable), then process the inbox
  3. dispatch outbox commands (BRIDGE / HANGUP)
  4. serve answered callers waiting on hold (before any new dialing!)
  5. recovery sweep (every few ticks; safe to run on all workers)
  6. if this worker holds the campaign's pacing lease:
        snapshot -> pacer.propose -> safety.evaluate -> shed excess -> allocator.execute(permit)

Only step 6 is "single leader per campaign". Everything else is safe on any
number of workers because every write is a CAS / unique-constrained insert.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .allocator import AllocationResult, CallAllocator, PermitRefused
from .config import DialerPolicy, SafetyConfig
from .events import EventProcessor, HoldManager, OutboxDispatcher
from .lifecycle import Lifecycle
from .pacing import PacingEngine, Proposal
from .providers.health import ProviderRouter
from .recovery import RecoverySweeper
from .safety import Decision, SafetyController
from .snapshot import Snapshot, build_snapshot
from .states import CallState
from .store import Store


@dataclass
class TickReport:
    leader: bool = False
    snapshot: Optional[Snapshot] = None
    proposal: Optional[Proposal] = None
    decision: Optional[Decision] = None
    allocation: Optional[AllocationResult] = None
    shed: int = 0
    hold: dict = field(default_factory=dict)


class DialerWorker:
    def __init__(self, worker_id: str, store: Store, campaign_id: str, router: ProviderRouter, pacer: PacingEngine,
                 safety_cfg: SafetyConfig = SafetyConfig(), policy: DialerPolicy = DialerPolicy(),
                 sweep_every: int = 2, pacing_lease_s: float = 3.0):
        self.id = worker_id
        self.s = store
        self.campaign_id = campaign_id
        self.router = router
        self.pacer = pacer
        self.policy = policy
        self.safety_cfg = safety_cfg
        self.lc = Lifecycle(store, policy)
        self.safety = SafetyController(store, router.health, safety_cfg, policy)
        # The allocator receives only the verifier - not the pacer, not the signing key.
        self.allocator = CallAllocator(store, self.lc, router, self.safety.verifier, worker_id, policy)
        self.events = EventProcessor(store, self.lc, policy, worker_id, on_lag=router.record_event_lag)
        self.holds = HoldManager(store, self.lc, worker_id)
        self.outbox = OutboxDispatcher(store, router)
        self.recovery = RecoverySweeper(store, self.lc, router, self.events, policy, worker_id)
        self.sweep_every = sweep_every
        self.pacing_lease_s = pacing_lease_s
        self._ticks = 0

    def pump_events(self):
        now = self.s.now()
        for name in self.router.order:
            self.events.ingest(self.router.get(name).poll_events(now))
        self.events.process_pending()

    def tick(self) -> TickReport:
        rep = TickReport()
        self._ticks += 1
        self.s.heartbeat(self.id)
        self.pump_events()
        self.outbox.run()
        rep.hold = self.holds.run(self.campaign_id)
        if self._ticks % self.sweep_every == 0:
            self.recovery.sweep(self.campaign_id)
        else:
            self.lc.finish_expired_wrapups(self.campaign_id)

        epoch = self.s.acquire_pacing_lease(self.campaign_id, self.id, self.pacing_lease_s)
        if epoch is None:
            return rep
        rep.leader = True
        cfg = self.safety_cfg
        snap = build_snapshot(self.s, self.campaign_id, cfg.short_window_s, cfg.long_window_s, cfg.abandon_window_s,
                              cfg.agent_drop_window_s, self.policy.max_wrap_s)
        rep.snapshot = snap
        rep.proposal = self.pacer.propose(snap)
        rep.decision = self.safety.evaluate(rep.proposal, snap)
        if rep.decision.shed:
            rep.shed = self.shed(rep.decision.shed)
        if rep.decision.permit is not None:
            try:
                rep.allocation = self.allocator.execute(rep.decision.permit)
            except PermitRefused as e:
                rep.allocation = AllocationResult(stopped=f"permit refused: {e}")
        self.outbox.run()
        return rep

    def shed(self, n: int) -> int:
        """Cancel the newest unbound calls that have not been answered yet (they are least
        likely to have rung the borrower at all)."""
        rows = self.s.q("SELECT id FROM calls WHERE campaign_id=? AND agent_id IS NULL AND state IN "
                        "('RESERVED','INITIATED','RINGING') ORDER BY CASE state WHEN 'RINGING' THEN 1 ELSE 0 END, "
                        "created_at DESC LIMIT ?", (self.campaign_id, n))
        done = 0
        for r in rows:
            if self.lc.end_call(r["id"], CallState.CANCELLED, "shed by safety controller", f"safety:{self.id}",
                                hangup=True):
                done += 1
        return done
