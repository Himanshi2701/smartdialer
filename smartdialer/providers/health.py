"""
Provider health: a circuit breaker per provider + a router that fails over.

CLOSED     -> normal. Failure rate over a sliding window is tracked.
OPEN       -> too many failures/timeouts. No new calls go to this provider.
HALF_OPEN  -> after a cool-down, allow a single probe call. Success closes, failure re-opens.

The breaker answers "should we send NEW calls there?". It does not touch
calls already in flight - those keep being driven by provider events and,
if events stop, by the recovery sweeper (reconciliation).
"""
from __future__ import annotations

import collections
import threading
from typing import Optional

from ..clock import Clock
from .base import TelecomProvider

CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, clock: Clock, window_s: float = 30.0, min_samples: int = 5, failure_threshold: float = 0.5,
                 open_s: float = 15.0):
        self.clock = clock
        self.window_s = window_s
        self.min_samples = min_samples
        self.failure_threshold = failure_threshold
        self.open_s = open_s
        self.state = CLOSED
        self.opened_at = 0.0
        self.probe_in_flight = False
        self.samples: collections.deque = collections.deque()  # (t, ok, latency)
        self.transitions: list[tuple[float, str]] = []
        self._lock = threading.Lock()

    def _trim(self, now):
        while self.samples and self.samples[0][0] < now - self.window_s:
            self.samples.popleft()

    def _set(self, st, now):
        if st != self.state:
            self.state = st
            self.transitions.append((now, st))

    def record(self, ok: bool, latency_s: float = 0.0):
        with self._lock:
            now = self.clock.now()
            self.samples.append((now, ok, latency_s))
            self._trim(now)
            if self.state == HALF_OPEN:
                self.probe_in_flight = False
                if ok:
                    self._set(CLOSED, now)
                    self.samples.clear()
                else:
                    self._set(OPEN, now)
                    self.opened_at = now
                return
            if self.state == CLOSED and len(self.samples) >= self.min_samples \
                    and self.failure_rate() >= self.failure_threshold:
                self._set(OPEN, now)
                self.opened_at = now

    def failure_rate(self) -> float:
        if not self.samples:
            return 0.0
        return sum(1 for s in self.samples if not s[1]) / len(self.samples)

    def current(self) -> str:
        with self._lock:
            now = self.clock.now()
            if self.state == OPEN and now - self.opened_at >= self.open_s:
                self._set(HALF_OPEN, now)
                self.probe_in_flight = False
            return self.state

    def allow(self) -> bool:
        st = self.current()
        with self._lock:
            if st == CLOSED:
                return True
            if st == HALF_OPEN and not self.probe_in_flight:
                self.probe_in_flight = True
                return True
            return False


class ProviderRouter:
    """Chooses a provider for each new call. Ordered by preference; skips unhealthy ones."""

    def __init__(self, providers: list[TelecomProvider], clock: Clock, max_event_lag_p95_s: float = 1.5,
                 min_lag_samples: int = 20, assume_fast_until_measured: bool = False, **breaker_kw):
        self.providers = {p.name: p for p in providers}
        self.order = [p.name for p in providers]
        self.breakers = {p.name: CircuitBreaker(clock, **breaker_kw) for p in providers}
        # webhook lag = (time we received the event) - (time it happened at the provider)
        self.lags = {p.name: collections.deque(maxlen=200) for p in providers}
        self.max_event_lag_p95_s = max_event_lag_p95_s
        self.min_lag_samples = min_lag_samples
        self.assume_fast_until_measured = assume_fast_until_measured   # tests only

    def get(self, name: str) -> TelecomProvider:
        return self.providers[name]

    def record_event_lag(self, name: str, lag_s: float):
        if name in self.lags:
            self.lags[name].append(max(0.0, lag_s))

    def lag_p95(self, name: str) -> Optional[float]:
        d = sorted(self.lags[name])
        if len(d) < max(1, self.min_lag_samples):
            return None
        return d[int(0.95 * (len(d) - 1))]

    def predictive_ok(self, name: str) -> bool:
        """Predictive calls are bridged by US after we see ANSWERED, so the provider's webhooks
        must be fast. Progressive calls don't care: the provider bridges the pre-assigned agent itself."""
        lag = self.lag_p95(name)
        if lag is None:
            return self.assume_fast_until_measured   # default: unknown latency is NOT trusted
        return lag <= self.max_event_lag_p95_s

    def pick(self, predictive: bool = False) -> Optional[TelecomProvider]:
        names = [n for n in self.order if not predictive or self.predictive_ok(n)]
        for n in names:
            if self.breakers[n].current() == CLOSED:
                return self.providers[n]
        for n in names:
            if self.breakers[n].allow():          # half-open probe
                return self.providers[n]
        return None

    def record(self, name: str, ok: bool, latency_s: float = 0.0):
        self.breakers[name].record(ok, latency_s)

    def health(self) -> dict:
        """Read-only summary for the Safety Controller."""
        states = {n: self.breakers[n].current() for n in self.order}
        lags = {n: (round(self.lag_p95(n), 2) if self.lag_p95(n) is not None else None) for n in self.order}
        return dict(states=states,
                    closed=sum(1 for s in states.values() if s == CLOSED),
                    half_open=sum(1 for s in states.values() if s == HALF_OPEN),
                    predictive_ok=[n for n in self.order if states[n] == CLOSED and self.predictive_ok(n)],
                    event_lag_p95=lags,
                    failure_rates={n: round(self.breakers[n].failure_rate(), 3) for n in self.order})
