"""
Recovery sweeper. Any worker may run it at any time; every step is a CAS so
running it concurrently on several workers is safe (the first one wins, the
others' updates match 0 rows).

The rule of thumb: state lives in the DB, ownership lives in leases.
A dead worker's leases expire, and whatever it was doing is either rolled
forward (the provider says the call exists -> adopt it) or rolled back
(release agent, reschedule borrower).
"""
from __future__ import annotations

from .config import DialerPolicy
from .events import EventProcessor
from .lifecycle import Lifecycle
from .providers.base import ProviderError
from .providers.health import ProviderRouter
from .states import AgentState, CallState
from .store import TERMINAL_SQL, Store


class RecoverySweeper:
    def __init__(self, store: Store, lifecycle: Lifecycle, router: ProviderRouter, events: EventProcessor,
                 policy: DialerPolicy = DialerPolicy(), worker_id: str = "sweeper"):
        self.s = store
        self.lc = lifecycle
        self.router = router
        self.events = events
        self.p = policy
        self.worker_id = worker_id
        self.counters = dict(setup_rolled_back=0, adopted=0, lost=0, unreconcilable=0, stale_reconciled=0,
                             agents_repaired=0, borrowers_released=0, wrapups_forced=0)

    def sweep(self, campaign_id: str) -> dict:
        s, now = self.s, self.s.now()
        src = f"recovery:{self.worker_id}"

        # (a) worker died before talking to the provider -> roll back
        for c in s.q("SELECT id FROM calls WHERE campaign_id=? AND state IN ('QUEUED','RESERVED') AND lease_until<?",
                     (campaign_id, now)):
            if self.lc.end_call(c["id"], CallState.CANCELLED, "setup lease expired before dialing", src):
                self.counters["setup_rolled_back"] += 1

        # (b) INITIATED but we never learned the provider call id (crash or timeout) -> reconcile
        for c in s.q("SELECT * FROM calls WHERE campaign_id=? AND state='INITIATED' AND provider_call_id IS NULL "
                     "AND lease_until<?", (campaign_id, now)):
            self._reconcile(c, src, uncertain=True)

        # (c) calls that have gone silent -> ask the provider what is going on
        for c in s.q("SELECT * FROM calls WHERE campaign_id=? AND provider_call_id IS NOT NULL AND "
                     "((state IN ('INITIATED','RINGING') AND last_event_at<?) OR "
                     " (state='CONNECTED' AND last_event_at<?))",
                     (campaign_id, now - self.p.ring_stale_s, now - self.p.connected_stale_s)):
            self._reconcile(c, src, uncertain=False)

        # (d) agents pointing at calls that are finished / missing -> repair
        for a in s.q(f"SELECT a.id, a.state, a.current_call_id, c.state cs FROM agents a LEFT JOIN calls c "
                     f"ON c.id=a.current_call_id WHERE a.campaign_id=? AND a.state IN ('RESERVED','DIALING','CONNECTED') "
                     f"AND (c.id IS NULL OR c.state IN {TERMINAL_SQL})", (campaign_id,)):
            if a["state"] == "RESERVED":
                # still inside its reservation lease? then the allocator may be mid-flight (placeholder id)
                lease = s.q1("SELECT lease_until FROM agents WHERE id=?", (a["id"],))["lease_until"] or 0
                if lease > now:
                    continue
            to = AgentState.WRAP_UP if a["state"] == "CONNECTED" else AgentState.AVAILABLE
            extra = {"wrap_deadline": now + self.p.max_wrap_s} if to == AgentState.WRAP_UP else {}
            if s.cas_agent(a["id"], AgentState(a["state"]), to, src + ":orphan", current_call_id=None,
                           lease_owner=None, lease_until=None, **extra):
                self.counters["agents_repaired"] += 1

        # (e) borrowers whose claim expired without an active call
        for b in s.q(f"SELECT b.id FROM borrowers b WHERE b.campaign_id=? AND b.state='IN_PROGRESS' AND b.lease_until<? "
                     f"AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.borrower_id=b.id AND c.state NOT IN {TERMINAL_SQL})",
                     (campaign_id, now)):
            with s.txn():
                if s.x("UPDATE borrowers SET state='PENDING', lease_owner=NULL, lease_until=NULL, version=version+1 "
                       "WHERE id=? AND state='IN_PROGRESS'", (b["id"],)):
                    self.counters["borrowers_released"] += 1

        # (f) system-enforced end of wrap-up
        self.counters["wrapups_forced"] += self.lc.finish_expired_wrapups(campaign_id)
        return dict(self.counters)

    def _reconcile(self, c, src: str, uncertain: bool):
        s, now = self.s, self.s.now()
        if not c["provider"]:
            self.lc.end_call(c["id"], CallState.FAILED, "no provider recorded", src)
            return
        prov = self.router.get(c["provider"])
        try:
            status = prov.lookup(c["idempotency_key"])
        except ProviderError:
            # Provider unreachable. Keep waiting a bounded time, then give up AND queue a hangup
            # (the outbox keeps trying it), so a ghost call can't later ring with nobody behind it.
            age = now - (c["initiated_at"] or now)
            if age > self.p.uncertain_giveup_s * (1 if uncertain else 4):
                self.lc.end_call(c["id"], CallState.FAILED, "provider unreachable; reconcile gave up", src, hangup=True)
                self.counters["unreconcilable"] += 1
            else:
                s.set_call_fields(c["id"], lease_until=now + self.p.setup_lease_s)
            return
        if status is None:
            # The provider has no call with our idempotency key: it was never created.
            self.lc.end_call(c["id"], CallState.FAILED, "provider has no record of the call", src)
            self.counters["lost"] += 1
            return
        # The call exists. Adopt it and feed its current status through the NORMAL event path,
        # so reconciliation can never do something an event could not.
        s.set_call_fields(c["id"], provider_call_id=status.provider_call_id, setup_uncertain=0, lease_owner=None,
                          lease_until=None, last_event_at=now)
        self.counters["adopted" if uncertain else "stale_reconciled"] += 1
        if status.status != "INITIATED":
            self.s.ingest_event(prov.name, f"reconcile:{c['id']}:{status.status}", status.status,
                                status.provider_call_id, c["id"], now, status.payload)
            self.events.process_pending()
