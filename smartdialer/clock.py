"""
Time source abstraction.

All components ask a Clock for "now" instead of calling time.time().
The simulator uses SimClock so a 30-minute campaign runs in seconds and
is reproducible (same seed -> same result). Production/load test uses RealClock.
"""
import threading
import time


class Clock:
    def now(self) -> float:
        raise NotImplementedError


class RealClock(Clock):
    def now(self) -> float:
        return time.time()


class SimClock(Clock):
    def __init__(self, start: float = 1_700_000_000.0):
        self._t = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._t

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._t += seconds

    def set(self, t: float) -> None:
        with self._lock:
            self._t = t
