"""
Provider interface. The dialer only ever talks to this interface.

Every concrete provider (mock A, mock B, Plivo) translates its own API and its
own webhook format into these normalized types. Nothing outside the
`providers` package knows how a provider works internally.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


class ProviderError(Exception):
    """Base class."""


class ProviderTimeout(ProviderError):
    """We don't know what happened. The call MAY exist. Must be reconciled, never blindly retried."""


class ProviderRejected(ProviderError):
    """The provider definitely did not create the call (validation error, rate limit, 5xx with no side effect)."""


# Normalized event types
RINGING = "RINGING"
ANSWERED = "ANSWERED"
COMPLETED = "COMPLETED"      # payload: answered(bool), bridged(bool), talk_s(float)
NO_ANSWER = "NO_ANSWER"      # rang out or busy
FAILED = "FAILED"            # network failure / invalid number

EVENT_TYPES = {RINGING, ANSWERED, COMPLETED, NO_ANSWER, FAILED}


@dataclass(frozen=True)
class PlaceCallRequest:
    call_id: str
    idempotency_key: str
    to_number: str
    agent_id: Optional[str] = None   # progressive: provider bridges to this agent on answer
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class PlaceCallResult:
    provider_call_id: str


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    event_id: str            # unique per logical event; a true duplicate re-uses it
    provider_call_id: str
    call_id: Optional[str]   # our id, echoed back from metadata
    type: str
    ts: float                # provider-side timestamp (when it happened, not when we received it)
    payload: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderCallStatus:
    provider_call_id: str
    call_id: Optional[str]
    status: str              # one of EVENT_TYPES or "INITIATED"
    payload: dict = field(default_factory=dict)


class TelecomProvider(ABC):
    name: str

    @abstractmethod
    def place_call(self, req: PlaceCallRequest) -> PlaceCallResult:
        """Must be idempotent on req.idempotency_key. Raises ProviderTimeout / ProviderRejected."""

    @abstractmethod
    def bridge(self, provider_call_id: Optional[str], agent_id: str, idempotency_key: Optional[str] = None) -> None:
        """Connect the borrower leg to an agent (predictive) or change the target agent. Idempotent."""

    @abstractmethod
    def hangup(self, provider_call_id: Optional[str], idempotency_key: Optional[str] = None) -> None:
        """End the call if it exists. Idempotent; hanging up an unknown/finished call is not an error."""

    @abstractmethod
    def lookup(self, idempotency_key: str) -> Optional[ProviderCallStatus]:
        """Reconciliation: does a call with this key exist, and what state is it in?"""

    @abstractmethod
    def poll_events(self, now: float) -> list[ProviderEvent]:
        """Stand-in for the webhook receiver: returns events delivered since the last poll."""
