"""
Explicit state machines for agents, calls and borrowers.

Everything that changes state in the system goes through these tables.
If a transition is not listed here, it is not allowed - the store refuses it.

Why tables instead of if/else scattered around the code?
  * One place to read the whole lifecycle (and to review it in an interview).
  * Tests can enumerate every (from, to) pair.
  * Provider events become "requests" to move along the machine, never
    direct writes, so a misbehaving provider cannot push us into nonsense.
"""
from enum import Enum


class AgentState(str, Enum):
    OFFLINE = "OFFLINE"        # not logged in
    AVAILABLE = "AVAILABLE"    # logged in, idle, can take a call
    RESERVED = "RESERVED"      # a dialer worker has claimed this agent for a call (lease held)
    DIALING = "DIALING"        # call for this agent has been sent to the provider
    CONNECTED = "CONNECTED"    # talking to a borrower
    WRAP_UP = "WRAP_UP"        # after-call work (notes, disposition); has a forced max duration
    PAUSED = "PAUSED"          # logged in but on break


AGENT_TRANSITIONS: dict[AgentState, set[AgentState]] = {
    AgentState.OFFLINE:   {AgentState.AVAILABLE},
    AgentState.AVAILABLE: {AgentState.RESERVED,      # progressive: reserve before dialing
                           AgentState.CONNECTED,     # predictive: bind on answer
                           AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.RESERVED:  {AgentState.DIALING,
                           AgentState.AVAILABLE,     # setup failed / lease expired
                           AgentState.PAUSED, AgentState.OFFLINE},   # agent disappeared during setup
    AgentState.DIALING:   {AgentState.CONNECTED,
                           AgentState.WRAP_UP,       # COMPLETED(answered) arrived before ANSWERED
                           AgentState.AVAILABLE,     # no answer / failed
                           AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.CONNECTED: {AgentState.WRAP_UP,
                           AgentState.OFFLINE},      # agent's own line dropped mid call
    AgentState.WRAP_UP:   {AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.PAUSED:    {AgentState.AVAILABLE, AgentState.OFFLINE},
}

# Agent states that count as "logged in"
AGENT_LOGGED_IN = {s for s in AgentState if s != AgentState.OFFLINE}


class CallState(str, Enum):
    QUEUED = "QUEUED"          # borrower claimed, call row created
    RESERVED = "RESERVED"      # capacity reserved: an agent (progressive) or a safety slot (predictive)
    INITIATED = "INITIATED"    # we are asking / have asked the provider to dial (intent persisted first)
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"      # borrower picked up; predictive calls may wait here (hold) for an agent
    CONNECTED = "CONNECTED"    # borrower is talking to an agent
    COMPLETED = "COMPLETED"    # normal end after a conversation
    NO_ANSWER = "NO_ANSWER"    # rang out / busy
    FAILED = "FAILED"          # provider/setup failure
    CANCELLED = "CANCELLED"    # we cancelled it (agent disappeared, shed, recovery)
    ABANDONED = "ABANDONED"    # borrower answered but no agent was connected -> compliance metric


CALL_TERMINAL = {CallState.COMPLETED, CallState.NO_ANSWER, CallState.FAILED,
                 CallState.CANCELLED, CallState.ABANDONED}

# Rank makes the call machine monotonic: a call can only move "forward".
# This single rule is what makes duplicate and out-of-order provider
# events harmless: an event that would move a call backwards (RINGING after
# ANSWERED) or sideways (ANSWERED after ANSWERED) is a no-op.
CALL_RANK = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.CONNECTED: 5,
    **{s: 10 for s in CALL_TERMINAL},
}

CALL_TRANSITIONS: dict[CallState, set[CallState]] = {
    CallState.QUEUED:    {CallState.RESERVED, CallState.CANCELLED},
    CallState.RESERVED:  {CallState.INITIATED, CallState.CANCELLED, CallState.FAILED},
    CallState.INITIATED: {CallState.RINGING, CallState.ANSWERED, CallState.CONNECTED,
                          CallState.COMPLETED, CallState.NO_ANSWER, CallState.FAILED,
                          CallState.CANCELLED, CallState.ABANDONED},
    CallState.RINGING:   {CallState.ANSWERED, CallState.CONNECTED, CallState.COMPLETED,
                          CallState.NO_ANSWER, CallState.FAILED, CallState.CANCELLED,
                          CallState.ABANDONED},
    CallState.ANSWERED:  {CallState.CONNECTED, CallState.ABANDONED, CallState.COMPLETED,
                          CallState.FAILED},
    CallState.CONNECTED: {CallState.COMPLETED, CallState.FAILED,
                          CallState.ABANDONED},  # we "bound" an agent but the bridge never happened
    **{s: set() for s in CALL_TERMINAL},   # terminal states are sticky
}

ACTIVE_CALL_STATES = [s for s in CallState if s not in CALL_TERMINAL]


class BorrowerState(str, Enum):
    PENDING = "PENDING"          # ready to be called (subject to next_attempt_at)
    IN_PROGRESS = "IN_PROGRESS"  # claimed by a worker / has an active call
    DONE = "DONE"                # conversation happened
    EXHAUSTED = "EXHAUSTED"      # max attempts reached


def agent_can(frm: AgentState, to: AgentState) -> bool:
    return to in AGENT_TRANSITIONS[AgentState(frm)]


def call_can(frm: CallState, to: CallState) -> bool:
    frm, to = CallState(frm), CallState(to)
    return to in CALL_TRANSITIONS[frm] and CALL_RANK[to] > CALL_RANK[frm]
