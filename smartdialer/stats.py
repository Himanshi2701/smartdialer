"""
Small statistics helpers shared by the pacer and the safety controller.

Age-aware answer probability
----------------------------
A call that was dialed 1 second ago and a call that has been ringing for 25
seconds are NOT equally likely to be answered. From the observed distribution
of time-to-answer (dial -> answer) we compute, by Bayes:

    P(will answer | not answered after `age` s) =  p*S(age) / (p*S(age) + (1-p))

where S(age) = fraction of answered calls that took longer than `age`.
(The (1-p) term assumes unanswered calls keep ringing until the ring timeout.)

Poisson-binomial tail
---------------------
Each in-flight call i answers with its own probability p_i. The number of
answers is then Poisson-binomial. We keep its distribution truncated at the
capacity C (+1 overflow bucket), which costs O(C) per call added. The overflow
bucket is exactly P(answers > C) = P(somebody answers and no agent is free).
"""
from __future__ import annotations

import bisect
from typing import Iterable, Sequence


def survival(tta_sorted: Sequence[float], age: float) -> float:
    n = len(tta_sorted)
    if n == 0:
        return 1.0
    greater = n - bisect.bisect_right(tta_sorted, age)
    return (greater + 0.5) / (n + 0.5)        # smoothed: never exactly 0 (conservative)


def p_remaining(p: float, age: float, tta_sorted: Sequence[float]) -> float:
    if p <= 0:
        return 0.0
    s = survival(tta_sorted, age)
    return p * s / (p * s + (1 - p))


class OverflowDist:
    """Distribution of the number of answers, truncated at capacity C."""

    def __init__(self, capacity: int):
        self.C = max(0, capacity)
        self.d = [0.0] * (self.C + 2)
        self.d[0] = 1.0

    def add(self, p: float) -> None:
        d, C = self.d, self.C
        nd = [0.0] * (C + 2)
        q = 1 - p
        for k in range(C + 1):
            if d[k]:
                nd[k] += d[k] * q
                nd[k + 1] += d[k] * p
        nd[C + 1] += d[C + 1]          # once over capacity, stays over
        self.d = nd

    def overflow(self) -> float:
        return self.d[self.C + 1]

    def copy(self) -> "OverflowDist":
        o = OverflowDist(self.C)
        o.d = list(self.d)
        return o


def overflow_prob(capacity: int, probs: Iterable[float]) -> float:
    od = OverflowDist(capacity)
    for p in probs:
        od.add(p)
    return od.overflow()
