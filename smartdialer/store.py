"""
The Store: the single source of truth.

Design decision (see docs/ADR.md, ADR-1 and ADR-2):
  * ALL shared state (agents, borrowers, calls, provider events, outbox) lives
    in one transactional database. Workers are stateless; if a worker dies, we
    lose nothing except in-flight work, which is recovered from leases.
  * SQLite in WAL mode is used for the prototype because it gives real ACID
    transactions and real cross-process locking with zero setup.
    The SQL is written so it maps 1:1 onto PostgreSQL (the production choice):
        BEGIN IMMEDIATE           ~ a transaction that takes the write lock up front
        conditional UPDATE ... WHERE state = 'AVAILABLE'  ~ same in Postgres
                                                           (+ FOR UPDATE SKIP LOCKED for the pick)
        partial UNIQUE indexes    ~ same in Postgres
  * Every state change is a compare-and-set (CAS): "UPDATE ... WHERE id=? AND
    state=<what I think it is>". If rowcount == 0 somebody else changed it first
    and I lost the race. No in-memory locks are trusted for correctness.
  * Two invariants are enforced by the database itself, not by application code:
        - an agent can have at most ONE active call      (partial unique index)
        - a borrower can have at most ONE active call    (partial unique index)
    So even a buggy worker cannot double-book an agent or double-dial a borrower.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterable, Optional

from .clock import Clock, RealClock
from .states import (AGENT_TRANSITIONS, CALL_TERMINAL, AgentState, BorrowerState,
                     CallState, agent_can, call_can)

TERMINAL_SQL = "(" + ",".join(f"'{s.value}'" for s in CALL_TERMINAL) + ")"

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS campaigns (
    id                TEXT PRIMARY KEY,
    mode              TEXT NOT NULL,            -- PROGRESSIVE | PREDICTIVE
    active            INTEGER NOT NULL DEFAULT 1,
    pacing_owner      TEXT,                     -- worker holding the pacing lease
    pacing_until      REAL,
    pacing_epoch      INTEGER NOT NULL DEFAULT 0, -- fencing token for the pacing lease
    hist_answer_rate  REAL NOT NULL DEFAULT 0.3,  -- prior from older campaigns
    hist_talk_s       REAL NOT NULL DEFAULT 120,
    safety_fallback_until  REAL,                -- safety controller forced progressive until...
    safety_fallback_reason TEXT
);

CREATE TABLE IF NOT EXISTS agents (
    id               TEXT PRIMARY KEY,
    campaign_id      TEXT NOT NULL,
    state            TEXT NOT NULL,
    version          INTEGER NOT NULL DEFAULT 0,
    current_call_id  TEXT,
    lease_owner      TEXT,
    lease_until      REAL,
    state_since      REAL NOT NULL,
    wrap_deadline    REAL                      -- forced end of WRAP_UP
);
CREATE INDEX IF NOT EXISTS ix_agents_pick ON agents(campaign_id, state, state_since);

CREATE TABLE IF NOT EXISTS borrowers (
    id                 TEXT PRIMARY KEY,
    campaign_id        TEXT NOT NULL,
    phone              TEXT NOT NULL,
    priority           REAL NOT NULL DEFAULT 0,  -- e.g. days-past-due x amount
    state              TEXT NOT NULL,
    version            INTEGER NOT NULL DEFAULT 0,
    attempts           INTEGER NOT NULL DEFAULT 0,  -- real attempts (rang the borrower)
    provider_failures  INTEGER NOT NULL DEFAULT 0,  -- our/provider fault, not counted as attempts
    next_attempt_at    REAL NOT NULL DEFAULT 0,
    lease_owner        TEXT,
    lease_until        REAL,
    last_outcome       TEXT
);
CREATE INDEX IF NOT EXISTS ix_borrowers_pick ON borrowers(campaign_id, state, priority DESC, next_attempt_at);

CREATE TABLE IF NOT EXISTS calls (
    id                TEXT PRIMARY KEY,
    campaign_id       TEXT NOT NULL,
    borrower_id       TEXT NOT NULL,
    agent_id          TEXT,                    -- NULL for predictive until answered
    mode              TEXT NOT NULL,           -- PROGRESSIVE (agent-bound) | PREDICTIVE (bind on answer)
    state             TEXT NOT NULL,
    version           INTEGER NOT NULL DEFAULT 0,
    provider          TEXT,
    provider_call_id  TEXT,
    idempotency_key   TEXT NOT NULL UNIQUE,    -- sent to the provider; dedupes retries of place_call
    permit_id         TEXT,
    lease_owner       TEXT,                    -- worker currently setting the call up
    lease_until       REAL,
    setup_uncertain   INTEGER NOT NULL DEFAULT 0,  -- place_call timed out: we do not know if it exists
    created_at        REAL NOT NULL,
    initiated_at      REAL,
    ringing_at        REAL,
    answered_at       REAL,
    connected_at      REAL,
    ended_at          REAL,
    hold_until        REAL,                    -- predictive: answered, waiting for an agent until
    end_reason        TEXT,
    talk_s            REAL,
    last_event_at     REAL
);
CREATE INDEX IF NOT EXISTS ix_calls_state ON calls(campaign_id, state);
CREATE INDEX IF NOT EXISTS ix_calls_ended ON calls(campaign_id, ended_at);
CREATE INDEX IF NOT EXISTS ix_calls_pcid ON calls(provider, provider_call_id);
-- DB-enforced invariants: one active call per agent, one active call per borrower.
CREATE UNIQUE INDEX IF NOT EXISTS ux_one_active_call_per_agent
    ON calls(agent_id) WHERE agent_id IS NOT NULL AND state NOT IN {TERMINAL_SQL};
CREATE UNIQUE INDEX IF NOT EXISTS ux_one_active_call_per_borrower
    ON calls(borrower_id) WHERE state NOT IN {TERMINAL_SQL};

CREATE TABLE IF NOT EXISTS call_transitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id    TEXT NOT NULL,
    from_state TEXT,
    to_state   TEXT NOT NULL,
    at         REAL NOT NULL,
    source     TEXT
);
CREATE INDEX IF NOT EXISTS ix_ct_call ON call_transitions(call_id);

CREATE TABLE IF NOT EXISTS agent_transitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id   TEXT NOT NULL,
    from_state TEXT,
    to_state   TEXT NOT NULL,
    at         REAL NOT NULL,
    source     TEXT
);

-- Inbox: every provider event is stored durably BEFORE it is acknowledged/processed.
-- UNIQUE(provider, provider_event_id) is the deduplication.
CREATE TABLE IF NOT EXISTS provider_events (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    provider           TEXT NOT NULL,
    provider_event_id  TEXT NOT NULL,
    provider_call_id   TEXT,
    call_id            TEXT,
    type               TEXT NOT NULL,
    payload            TEXT,
    provider_ts        REAL,
    received_at        REAL NOT NULL,
    processed_at       REAL,
    outcome            TEXT,
    UNIQUE(provider, provider_event_id)
);
CREATE INDEX IF NOT EXISTS ix_pe_pending ON provider_events(processed_at, provider_ts);

-- Outbox: commands to the provider (BRIDGE, HANGUP) written in the SAME transaction
-- as the state change that requires them, then delivered at-least-once.
CREATE TABLE IF NOT EXISTS outbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key  TEXT NOT NULL UNIQUE,
    call_id     TEXT NOT NULL,
    provider    TEXT,
    command     TEXT NOT NULL,
    args        TEXT,
    status      TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | DONE | DEAD
    attempts    INTEGER NOT NULL DEFAULT 0,
    next_at     REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox(status, next_at);

-- Permits are single use. A replayed permit (same id) fails the PRIMARY KEY.
CREATE TABLE IF NOT EXISTS permits (
    id           TEXT PRIMARY KEY,
    campaign_id  TEXT NOT NULL,
    mode         TEXT NOT NULL,
    count        INTEGER NOT NULL,
    issued_at    REAL NOT NULL,
    used_by      TEXT,
    used_at      REAL
);

CREATE TABLE IF NOT EXISTS workers (
    id            TEXT PRIMARY KEY,
    heartbeat_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS safety_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    at          REAL NOT NULL,
    campaign_id TEXT NOT NULL,
    proposed    INTEGER NOT NULL,
    approved    INTEGER NOT NULL,
    action      TEXT NOT NULL,     -- APPROVE | REDUCE | REJECT | FALLBACK_PROGRESSIVE
    mode        TEXT NOT NULL,
    reasons     TEXT
);
"""


class InvalidTransition(Exception):
    pass


class Store:
    """One Store == one DB connection. Use one Store per thread / process."""

    def __init__(self, path: str = ":memory:", clock: Optional[Clock] = None, init: bool = True):
        self.path = path
        self.clock = clock or RealClock()
        # isolation_level=None -> we control transactions explicitly with BEGIN IMMEDIATE
        self.conn = sqlite3.connect(path, isolation_level=None, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._depth = 0
        self._lock = threading.RLock()
        if init:
            self.conn.executescript(SCHEMA)

    # ------------------------------------------------------------------ txn
    @contextmanager
    def txn(self):
        """Re-entrant write transaction.

        BEGIN IMMEDIATE takes the database write lock at the start, so the
        read-check-write inside the block is atomic with respect to every other
        connection/process. (In Postgres: a transaction + row locks.)
        """
        with self._lock:
            if self._depth == 0:
                self.conn.execute("BEGIN IMMEDIATE")
            self._depth += 1
            try:
                yield self.conn
            except BaseException:
                self._depth -= 1
                if self._depth == 0:
                    self.conn.execute("ROLLBACK")
                raise
            else:
                self._depth -= 1
                if self._depth == 0:
                    self.conn.execute("COMMIT")

    def q(self, sql: str, args: Iterable = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, tuple(args)).fetchall()

    def q1(self, sql: str, args: Iterable = ()) -> Optional[sqlite3.Row]:
        return self.conn.execute(sql, tuple(args)).fetchone()

    def x(self, sql: str, args: Iterable = ()) -> int:
        return self.conn.execute(sql, tuple(args)).rowcount

    def now(self) -> float:
        return self.clock.now()

    def close(self):
        self.conn.close()

    # --------------------------------------------------------------- seeding
    def add_campaign(self, cid: str, mode: str = "PREDICTIVE", hist_answer_rate: float = 0.3,
                     hist_talk_s: float = 120):
        with self.txn():
            self.x("INSERT INTO campaigns(id, mode, hist_answer_rate, hist_talk_s) VALUES (?,?,?,?)",
                   (cid, mode, hist_answer_rate, hist_talk_s))

    def add_agents(self, campaign_id: str, n: int, prefix: str = "agent", state: AgentState = AgentState.AVAILABLE,
                   start: int = 0):
        now = self.now()
        with self.txn():
            self.conn.executemany(
                "INSERT INTO agents(id, campaign_id, state, state_since) VALUES (?,?,?,?)",
                [(f"{prefix}-{i:05d}", campaign_id, state.value, now) for i in range(start, start + n)])

    def add_borrowers(self, campaign_id: str, rows: list[tuple[str, str, float]]):
        """rows: (id, phone, priority)"""
        with self.txn():
            self.conn.executemany(
                "INSERT INTO borrowers(id, campaign_id, phone, priority, state) VALUES (?,?,?,?, 'PENDING')",
                [(r[0], campaign_id, r[1], r[2]) for r in rows])

    # ------------------------------------------------------------- generic CAS
    def get_agent(self, agent_id: str):
        return self.q1("SELECT * FROM agents WHERE id=?", (agent_id,))

    def get_call(self, call_id: str):
        return self.q1("SELECT * FROM calls WHERE id=?", (call_id,))

    def get_borrower(self, bid: str):
        return self.q1("SELECT * FROM borrowers WHERE id=?", (bid,))

    def cas_agent(self, agent_id: str, expected: AgentState | Iterable[AgentState], to: AgentState,
                  source: str, **fields) -> bool:
        """Move an agent expected -> to, atomically. Returns False if the agent
        was not in an expected state (someone else got there first)."""
        exp = [expected] if isinstance(expected, (AgentState, str)) else list(expected)
        exp = [AgentState(e) for e in exp]
        to = AgentState(to)
        for e in exp:
            if not agent_can(e, to):
                raise InvalidTransition(f"agent {e.value} -> {to.value} not allowed")
        now = self.now()
        sets = ["state=?", "version=version+1", "state_since=?"]
        args: list = [to.value, now]
        for k, v in fields.items():
            sets.append(f"{k}=?")
            args.append(v)
        with self.txn():
            row = self.q1(f"SELECT state FROM agents WHERE id=? AND state IN ({','.join('?' * len(exp))})",
                          (agent_id, *[e.value for e in exp]))
            if row is None:
                return False
            n = self.x(f"UPDATE agents SET {', '.join(sets)} WHERE id=? AND state=?",
                       (*args, agent_id, row["state"]))
            if n == 1:
                self.x("INSERT INTO agent_transitions(agent_id, from_state, to_state, at, source) VALUES (?,?,?,?,?)",
                       (agent_id, row["state"], to.value, now, source))
            return n == 1

    def cas_call(self, call_id: str, to: CallState, source: str, expected: Optional[Iterable[CallState]] = None,
                 **fields) -> bool:
        """Move a call forward along the state machine.

        The rank check (call_can) means a call can never go backwards and a
        terminal call never changes again. That is the core of out-of-order and
        duplicate event safety.
        """
        to = CallState(to)
        now = self.now()
        with self.txn():
            row = self.get_call(call_id)
            if row is None:
                return False
            cur = CallState(row["state"])
            if expected is not None and cur not in {CallState(e) for e in expected}:
                return False
            if not call_can(cur, to):
                return False
            sets = ["state=?", "version=version+1"]
            args: list = [to.value]
            for k, v in fields.items():
                sets.append(f"{k}=?")
                args.append(v)
            n = self.x(f"UPDATE calls SET {', '.join(sets)} WHERE id=? AND version=?",
                       (*args, call_id, row["version"]))
            if n == 1:
                self.x("INSERT INTO call_transitions(call_id, from_state, to_state, at, source) VALUES (?,?,?,?,?)",
                       (call_id, cur.value, to.value, now, source))
            return n == 1

    def set_call_fields(self, call_id: str, **fields) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k}=?" for k in fields)
        with self.txn():
            self.x(f"UPDATE calls SET {sets}, version=version+1 WHERE id=?", (*fields.values(), call_id))

    # ---------------------------------------------------------- reservations
    def reserve_agent(self, campaign_id: str, worker_id: str, call_id: str, lease_s: float) -> Optional[str]:
        """Atomically pick the longest-idle AVAILABLE agent and reserve it.

        Two workers racing for the same agent: both may *read* it as AVAILABLE,
        but the UPDATE ... WHERE state='AVAILABLE' succeeds for exactly one of
        them (the write lock serialises the two UPDATEs; the second one sees
        state='RESERVED' and matches 0 rows). The loser simply tries the next agent.
        """
        now = self.now()
        with self.txn():
            for _ in range(5):  # a few attempts in case of a lost race
                row = self.q1("SELECT id FROM agents WHERE campaign_id=? AND state='AVAILABLE' "
                              "ORDER BY state_since LIMIT 1", (campaign_id,))
                if row is None:
                    return None
                n = self.x("UPDATE agents SET state='RESERVED', version=version+1, state_since=?, "
                           "current_call_id=?, lease_owner=?, lease_until=? WHERE id=? AND state='AVAILABLE'",
                           (now, call_id, worker_id, now + lease_s, row["id"]))
                if n == 1:
                    self.x("INSERT INTO agent_transitions(agent_id, from_state, to_state, at, source) "
                           "VALUES (?,?,?,?,?)", (row["id"], "AVAILABLE", "RESERVED", now, worker_id))
                    return row["id"]
            return None

    def reserve_specific_agent(self, agent_id: str, worker_id: str, call_id: str, lease_s: float) -> bool:
        """Used by tests to make two workers fight over ONE named agent."""
        now = self.now()
        with self.txn():
            n = self.x("UPDATE agents SET state='RESERVED', version=version+1, state_since=?, "
                       "current_call_id=?, lease_owner=?, lease_until=? WHERE id=? AND state='AVAILABLE'",
                       (now, call_id, worker_id, now + lease_s, agent_id))
            if n == 1:
                self.x("INSERT INTO agent_transitions(agent_id, from_state, to_state, at, source) "
                       "VALUES (?,?,?,?,?)", (agent_id, "AVAILABLE", "RESERVED", now, worker_id))
            return n == 1

    def claim_borrower(self, campaign_id: str, worker_id: str, lease_s: float):
        """Pick the highest-priority borrower that is due, and claim it (CAS)."""
        now = self.now()
        with self.txn():
            for _ in range(5):
                row = self.q1("SELECT id, version FROM borrowers WHERE campaign_id=? AND state='PENDING' "
                              "AND next_attempt_at<=? ORDER BY priority DESC, next_attempt_at LIMIT 1",
                              (campaign_id, now))
                if row is None:
                    return None
                n = self.x("UPDATE borrowers SET state='IN_PROGRESS', version=version+1, lease_owner=?, "
                           "lease_until=? WHERE id=? AND version=? AND state='PENDING'",
                           (worker_id, now + lease_s, row["id"], row["version"]))
                if n == 1:
                    return self.get_borrower(row["id"])
            return None

    # ---------------------------------------------------------- pacing lease
    def acquire_pacing_lease(self, campaign_id: str, worker_id: str, lease_s: float) -> Optional[int]:
        """Only one worker paces a campaign at a time. Returns a fencing epoch or None."""
        now = self.now()
        with self.txn():
            n = self.x("UPDATE campaigns SET pacing_owner=?, pacing_until=?, "
                       "pacing_epoch = pacing_epoch + CASE WHEN pacing_owner IS ? THEN 0 ELSE 1 END "
                       "WHERE id=? AND active=1 AND (pacing_owner IS NULL OR pacing_owner=? OR pacing_until<?)",
                       (worker_id, now + lease_s, worker_id, campaign_id, worker_id, now))
            if n == 0:
                return None
            return self.q1("SELECT pacing_epoch FROM campaigns WHERE id=?", (campaign_id,))["pacing_epoch"]

    def heartbeat(self, worker_id: str):
        with self.txn():
            self.x("INSERT INTO workers(id, heartbeat_at) VALUES (?,?) "
                   "ON CONFLICT(id) DO UPDATE SET heartbeat_at=excluded.heartbeat_at", (worker_id, self.now()))

    # ---------------------------------------------------------------- inbox
    def ingest_event(self, provider: str, provider_event_id: str, type_: str, provider_call_id: Optional[str],
                     call_id: Optional[str], provider_ts: float, payload: dict) -> bool:
        """Durably store a provider event. Returns False if it is a duplicate.

        This is what a webhook handler does before returning HTTP 200. If we
        crash before this commit, the provider retries the webhook; if we crash
        after, the event is in the inbox and will be processed after restart.
        """
        with self.txn():
            n = self.x("INSERT OR IGNORE INTO provider_events(provider, provider_event_id, provider_call_id, call_id, "
                       "type, payload, provider_ts, received_at) VALUES (?,?,?,?,?,?,?,?)",
                       (provider, provider_event_id, provider_call_id, call_id, type_, json.dumps(payload),
                        provider_ts, self.now()))
            return n == 1

    # ---------------------------------------------------------------- outbox
    def enqueue_command(self, dedupe_key: str, call_id: str, provider: Optional[str], command: str, args: dict):
        with self.txn():
            self.x("INSERT OR IGNORE INTO outbox(dedupe_key, call_id, provider, command, args, next_at) "
                   "VALUES (?,?,?,?,?,?)", (dedupe_key, call_id, provider, command, json.dumps(args), self.now()))

    # --------------------------------------------------------------- invariants
    def check_invariants(self) -> list[str]:
        """Used by tests, simulation and load test. Returns a list of violations."""
        problems = []
        for r in self.q(f"SELECT agent_id, COUNT(*) c FROM calls WHERE agent_id IS NOT NULL AND state NOT IN "
                        f"{TERMINAL_SQL} GROUP BY agent_id HAVING c > 1"):
            problems.append(f"agent {r['agent_id']} has {r['c']} active calls")
        for r in self.q(f"SELECT borrower_id, COUNT(*) c FROM calls WHERE state NOT IN {TERMINAL_SQL} "
                        f"GROUP BY borrower_id HAVING c > 1"):
            problems.append(f"borrower {r['borrower_id']} has {r['c']} active calls")
        # An agent in CONNECTED must be attached to a CONNECTED call
        for r in self.q("SELECT a.id, a.current_call_id, c.state cs FROM agents a LEFT JOIN calls c "
                        "ON c.id=a.current_call_id WHERE a.state='CONNECTED'"):
            if r["cs"] != "CONNECTED":
                problems.append(f"agent {r['id']} CONNECTED but call {r['current_call_id']} is {r['cs']}")
        # A CONNECTED call must have an agent that points back at it
        for r in self.q("SELECT c.id, c.agent_id, a.state ast, a.current_call_id acc FROM calls c "
                        "LEFT JOIN agents a ON a.id=c.agent_id WHERE c.state='CONNECTED'"):
            if r["acc"] != r["id"] or r["ast"] not in ("CONNECTED", "OFFLINE"):
                problems.append(f"call {r['id']} CONNECTED but agent {r['agent_id']} is {r['ast']}")
        return problems
