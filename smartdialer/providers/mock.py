"""
Mock telecom providers driven by a (simulated) clock.

MockProvider simulates a provider's own view of each call and the webhooks it
would send. Behaviour is controlled by a ProviderProfile, so "Provider A" and
"Provider B" are the same code with very different personalities:

  Provider A: fast, reliable, events in order, no duplicates.
  Provider B: slow setup, occasional place_call timeouts (some of which DID
              create the call - the dangerous "ghost call" case), duplicate
              webhooks, re-sent events with new ids, and out-of-order delivery.

An outage can be switched on at runtime (set_outage) to test failure handling.
"""
from __future__ import annotations

import heapq
import itertools
import random
import threading
from dataclasses import dataclass, replace
from typing import Callable, Optional

from ..clock import Clock
from .base import (ANSWERED, COMPLETED, FAILED, NO_ANSWER, RINGING, PlaceCallRequest, PlaceCallResult,
                   ProviderCallStatus, ProviderEvent, ProviderRejected, ProviderTimeout, TelecomProvider)


@dataclass(frozen=True)
class ProviderProfile:
    setup_latency_s: tuple[float, float] = (0.2, 0.6)    # place_call -> starts ringing
    event_delay_s: tuple[float, float] = (0.05, 0.2)     # webhook delivery delay
    reorder_jitter_s: float = 0.0                        # extra random delay -> out-of-order delivery
    duplicate_prob: float = 0.0                          # same event id delivered twice
    resend_new_id_prob: float = 0.0                      # same event re-sent with a NEW id
    place_timeout_prob: float = 0.0                      # place_call raises ProviderTimeout
    ghost_prob: float = 0.5                              # ...and the call was created anyway
    reject_prob: float = 0.0                             # place_call raises ProviderRejected
    network_fail_prob: float = 0.005                     # call fails after creation (FAILED event)
    busy_prob: float = 0.03
    ring_timeout_s: float = 30.0
    bridge_fallback_s: float = 3.0                       # provider-side: answered & not bridged -> message + hangup
    max_cps: int = 10_000                                # calls per second the trunk accepts


PROFILE_A = ProviderProfile()
PROFILE_B = ProviderProfile(setup_latency_s=(1.5, 4.0), event_delay_s=(0.2, 1.5), reorder_jitter_s=2.5,
                            duplicate_prob=0.25, resend_new_id_prob=0.05, place_timeout_prob=0.05,
                            ghost_prob=0.5, reject_prob=0.01, network_fail_prob=0.02)


class CalleeWorld:
    """How borrowers behave. Time-varying functions make scenario D possible."""

    def __init__(self, answer_rate: Callable[[float], float] | float, talk_mean_s: Callable[[float], float] | float,
                 ring_to_answer_s: tuple[float, float] = (4.0, 15.0)):
        self._ar = answer_rate if callable(answer_rate) else (lambda t, v=answer_rate: v)
        self._tt = talk_mean_s if callable(talk_mean_s) else (lambda t, v=talk_mean_s: v)
        self.ring_to_answer_s = ring_to_answer_s
        self.t0 = None

    def answer_rate(self, t: float) -> float:
        return self._ar(t - (self.t0 or t))

    def talk_mean(self, t: float) -> float:
        return self._tt(t - (self.t0 or t))


class MockProvider(TelecomProvider):
    def __init__(self, name: str, clock: Clock, world: CalleeWorld, profile: ProviderProfile = PROFILE_A,
                 seed: int = 0):
        self.name = name
        self.clock = clock
        self.world = world
        self.profile = profile
        self.rng = random.Random(seed)
        self._lock = threading.RLock()
        self._calls: dict[str, dict] = {}          # pcid -> call
        self._by_key: dict[str, str] = {}          # idempotency key -> pcid
        self._timeline: list = []                  # heap of (t, seq, fn)
        self._outbox: list = []                    # heap of (deliver_at, seq, event)
        self._seq = itertools.count()
        self._ids = itertools.count(1)
        self._cps_window = (0, 0)
        self.outage: Optional[str] = None          # None | "timeout" | "error"
        self.outage_event_delay_s = 0.0
        self.stats = dict(placed=0, timeouts=0, ghosts=0, rejected=0, events=0, duplicates=0, bridges=0,
                          hangups=0, fallback_hangups=0)

    # ------------------------------------------------------------ controls
    def set_outage(self, mode: Optional[str], event_delay_s: float = 0.0):
        with self._lock:
            self.outage = mode
            self.outage_event_delay_s = event_delay_s

    def set_profile(self, **changes):
        self.profile = replace(self.profile, **changes)

    # ------------------------------------------------------------ API
    def place_call(self, req: PlaceCallRequest) -> PlaceCallResult:
        with self._lock:
            now = self.clock.now()
            if req.idempotency_key in self._by_key:            # idempotent retry
                return PlaceCallResult(self._by_key[req.idempotency_key])
            if self.outage == "error":
                self.stats["rejected"] += 1
                raise ProviderRejected(f"{self.name}: 503 service unavailable")
            if self.outage == "timeout":
                self.stats["timeouts"] += 1
                if self.rng.random() < 0.3:                    # some requests still get through
                    self._create(req, now)
                    self.stats["ghosts"] += 1
                raise ProviderTimeout(f"{self.name}: request timed out")
            sec = int(now)
            cnt = self._cps_window[1] + 1 if self._cps_window[0] == sec else 1
            self._cps_window = (sec, cnt)
            if cnt > self.profile.max_cps:
                self.stats["rejected"] += 1
                raise ProviderRejected(f"{self.name}: 429 rate limited")
            r = self.rng.random()
            if r < self.profile.reject_prob:
                self.stats["rejected"] += 1
                raise ProviderRejected(f"{self.name}: 500 internal error")
            if r < self.profile.reject_prob + self.profile.place_timeout_prob:
                self.stats["timeouts"] += 1
                if self.rng.random() < self.profile.ghost_prob:
                    self._create(req, now)
                    self.stats["ghosts"] += 1
                raise ProviderTimeout(f"{self.name}: request timed out")
            pcid = self._create(req, now)
            return PlaceCallResult(pcid)

    def bridge(self, provider_call_id, agent_id, idempotency_key=None):
        with self._lock:
            self._check_api()
            c = self._find(provider_call_id, idempotency_key)
            if c is None or c["ended"]:
                return
            c["agent"] = agent_id
            if c["answered_at"] is not None and c["bridged_at"] is None:
                self._do_bridge(c, self.clock.now())

    def hangup(self, provider_call_id, idempotency_key=None):
        with self._lock:
            self._check_api()
            c = self._find(provider_call_id, idempotency_key)
            if c is None or c["ended"]:
                return
            self.stats["hangups"] += 1
            now = self.clock.now()
            talk = (now - c["bridged_at"]) if c["bridged_at"] else 0.0
            self._end(c, now, COMPLETED, answered=c["answered_at"] is not None,
                      bridged=c["bridged_at"] is not None, talk_s=talk, cause="hangup_by_us")

    def lookup(self, idempotency_key):
        with self._lock:
            self._check_api()
            pcid = self._by_key.get(idempotency_key)
            if pcid is None:
                return None
            c = self._calls[pcid]
            return ProviderCallStatus(pcid, c["call_id"], c["status"],
                                      dict(answered=c["answered_at"] is not None,
                                           bridged=c["bridged_at"] is not None, talk_s=c.get("talk_s", 0.0)))

    def poll_events(self, now: float) -> list[ProviderEvent]:
        with self._lock:
            self.advance(now)
            out = []
            while self._outbox and self._outbox[0][0] <= now:
                out.append(heapq.heappop(self._outbox)[2])
            return out

    # ------------------------------------------------------------ simulation
    def advance(self, now: float):
        """Run the provider's internal timeline up to `now`."""
        with self._lock:
            while self._timeline and self._timeline[0][0] <= now:
                t, _, fn = heapq.heappop(self._timeline)
                fn(t)

    def active_calls(self) -> int:
        return sum(1 for c in self._calls.values() if not c["ended"])

    # ------------------------------------------------------------ internals
    def _check_api(self):
        if self.outage == "timeout":
            raise ProviderTimeout(f"{self.name}: request timed out")
        if self.outage == "error":
            raise ProviderRejected(f"{self.name}: 503")

    def _find(self, pcid, key):
        if pcid and pcid in self._calls:
            return self._calls[pcid]
        if key and key in self._by_key:
            return self._calls[self._by_key[key]]
        return None

    def _at(self, t, fn):
        heapq.heappush(self._timeline, (t, next(self._seq), fn))

    def _create(self, req: PlaceCallRequest, now: float) -> str:
        pcid = f"{self.name}-{next(self._ids):07d}"
        c = dict(pcid=pcid, call_id=req.call_id, key=req.idempotency_key, agent=req.agent_id, status="INITIATED",
                 created=now, answered_at=None, bridged_at=None, ended=False, ev=0)
        self._calls[pcid] = c
        self._by_key[req.idempotency_key] = pcid
        self.stats["placed"] += 1
        p, rng = self.profile, self.rng
        t_ring = now + rng.uniform(*p.setup_latency_s)
        if rng.random() < p.network_fail_prob:
            self._at(t_ring, lambda t: self._end(c, t, FAILED, cause="network"))
            return pcid
        self._at(t_ring, lambda t: self._ring(c, t))
        return pcid

    def _ring(self, c, t):
        if c["ended"]:
            return
        c["status"] = RINGING
        self._emit(c, RINGING, t)
        p, rng = self.profile, self.rng
        if rng.random() < p.busy_prob:
            self._at(t + rng.uniform(1, 3), lambda tt: self._end(c, tt, NO_ANSWER, cause="busy"))
        elif rng.random() < self.world.answer_rate(t):
            self._at(t + rng.uniform(*self.world.ring_to_answer_s), lambda tt: self._answer(c, tt))
        else:
            self._at(t + p.ring_timeout_s, lambda tt: self._end(c, tt, NO_ANSWER, cause="no_answer"))

    def _answer(self, c, t):
        if c["ended"]:
            return
        c["status"] = ANSWERED
        c["answered_at"] = t
        self._emit(c, ANSWERED, t)
        if c["agent"]:
            self._do_bridge(c, t)
        else:
            # provider-side safety net: nobody bridged us -> play message & hang up
            self._at(t + self.profile.bridge_fallback_s, lambda tt: self._fallback(c, tt))

    def _fallback(self, c, t):
        if c["ended"] or c["bridged_at"] is not None:
            return
        self.stats["fallback_hangups"] += 1
        self._end(c, t, COMPLETED, answered=True, bridged=False, talk_s=0.0, cause="no_agent_message")

    def _do_bridge(self, c, t):
        c["bridged_at"] = t
        self.stats["bridges"] += 1
        mean = max(5.0, self.world.talk_mean(t))
        talk = max(5.0, self.rng.expovariate(1.0 / mean))
        self._at(t + talk, lambda tt: self._end(c, tt, COMPLETED, answered=True, bridged=True,
                                                talk_s=tt - c["bridged_at"], cause="normal"))

    def _end(self, c, t, kind, cause, **payload):
        if c["ended"]:
            return
        c["ended"] = True
        c["status"] = kind
        c["talk_s"] = payload.get("talk_s", 0.0)
        self._emit(c, kind, t, cause=cause, **payload)

    def _emit(self, c, kind, t, **payload):
        p, rng = self.profile, self.rng
        c["ev"] += 1
        eid = f"{c['pcid']}:{c['ev']}"
        ev = ProviderEvent(self.name, eid, c["pcid"], c["call_id"], kind, t, payload)
        self._deliver(ev, t)
        self.stats["events"] += 1
        if rng.random() < p.duplicate_prob:
            self.stats["duplicates"] += 1
            self._deliver(ev, t)                                        # exact duplicate
        if rng.random() < p.resend_new_id_prob:
            self.stats["duplicates"] += 1
            self._deliver(ProviderEvent(self.name, eid + "r", c["pcid"], c["call_id"], kind, t, payload), t)

    def _deliver(self, ev, t):
        p, rng = self.profile, self.rng
        at = t + rng.uniform(*p.event_delay_s) + rng.uniform(0, p.reorder_jitter_s) + self.outage_event_delay_s
        heapq.heappush(self._outbox, (at, next(self._seq), ev))


def provider_a(clock, world, seed=1) -> MockProvider:
    return MockProvider("provA", clock, world, PROFILE_A, seed)


def provider_b(clock, world, seed=2) -> MockProvider:
    return MockProvider("provB", clock, world, PROFILE_B, seed)
