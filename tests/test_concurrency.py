"""
"Two workers see the same available agent at almost the same time. Both must not be able to reserve it."

These tests use a real on-disk SQLite DB and separate connections (threads) or
separate OS processes, so the database's locking is what is being tested.
"""
import multiprocessing as mp
import sqlite3
import threading

import pytest

from helpers import make_env, predictive_permit_whitebox, temp_db
from smartdialer.allocator import CallAllocator
from smartdialer.clock import RealClock
from smartdialer.lifecycle import Lifecycle
from smartdialer.store import Store


def _seed(path, agents=1, borrowers=0):
    s = Store(path, RealClock())
    s.add_campaign("c1")
    s.add_agents("c1", agents)
    if borrowers:
        s.add_borrowers("c1", [(f"b{i}", f"+0{i}", 0) for i in range(borrowers)])
    s.close()


def test_two_threads_same_agent_exactly_one_wins():
    path = temp_db()
    _seed(path, agents=1)
    barrier = threading.Barrier(20)
    results = []

    def worker(i):
        st = Store(path, RealClock(), init=False)
        barrier.wait()                                     # start at the same instant
        results.append(st.reserve_specific_agent("agent-00000", f"w{i}", f"call{i}", 10))
        st.close()

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert results.count(True) == 1
    st = Store(path, RealClock(), init=False)
    a = st.get_agent("agent-00000")
    assert a["state"] == "RESERVED" and a["version"] == 1   # exactly one successful write


def _proc_reserve(path, n, q):
    st = Store(path, RealClock(), init=False)
    got = []
    for i in range(n):
        a = st.reserve_agent("c1", f"pid{mp.current_process().pid}", f"call-{mp.current_process().pid}-{i}", 30)
        if a:
            got.append(a)
    q.put(got)


def test_many_processes_never_double_reserve():
    path = temp_db()
    _seed(path, agents=25)
    q = mp.Queue()
    ps = [mp.Process(target=_proc_reserve, args=(path, 10, q)) for _ in range(8)]
    [p.start() for p in ps]
    got = []
    for _ in ps:
        got.extend(q.get(timeout=60))
    [p.join() for p in ps]
    assert len(got) == 25                    # every agent reserved exactly once...
    assert len(set(got)) == 25               # ...and never twice


def test_borrower_claim_race():
    path = temp_db()
    _seed(path, agents=0, borrowers=5)
    barrier = threading.Barrier(10)
    won = []

    def worker(i):
        st = Store(path, RealClock(), init=False)
        barrier.wait()
        b = st.claim_borrower("c1", f"w{i}", 30)
        if b:
            won.append(b["id"])
        st.close()

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(won) == 5 and len(set(won)) == 5


def test_db_rejects_second_active_call_for_same_agent_or_borrower():
    env = make_env(agents=1, borrowers=2)
    s = env.store
    now = s.now()
    s.x("INSERT INTO calls(id,campaign_id,borrower_id,agent_id,mode,state,idempotency_key,created_at) "
        "VALUES ('x1','c1','b000','agent-00000','PROGRESSIVE','RINGING','x1',?)", (now,))
    with pytest.raises(sqlite3.IntegrityError):   # same agent, second active call
        s.x("INSERT INTO calls(id,campaign_id,borrower_id,agent_id,mode,state,idempotency_key,created_at) "
            "VALUES ('x2','c1','b001','agent-00000','PROGRESSIVE','RINGING','x2',?)", (now,))
    with pytest.raises(sqlite3.IntegrityError):   # same borrower, second active call
        s.x("INSERT INTO calls(id,campaign_id,borrower_id,agent_id,mode,state,idempotency_key,created_at) "
            "VALUES ('x3','c1','b000',NULL,'PREDICTIVE','RINGING','x3',?)", (now,))
    # a finished call does not block a new one
    s.x("UPDATE calls SET state='COMPLETED' WHERE id='x1'")
    s.x("INSERT INTO calls(id,campaign_id,borrower_id,agent_id,mode,state,idempotency_key,created_at) "
        "VALUES ('x4','c1','b000','agent-00000','PROGRESSIVE','RINGING','x4',?)", (now,))


def test_only_one_pacing_leader_and_fencing_epoch():
    env = make_env()
    s = env.store
    e1 = s.acquire_pacing_lease("c1", "w1", 3)
    assert e1 is not None
    assert s.acquire_pacing_lease("c1", "w2", 3) is None          # w1 holds it
    assert s.acquire_pacing_lease("c1", "w1", 3) == e1            # renew keeps epoch
    env.clock.advance(4)                                          # w1 stops renewing (crashed / GC pause)
    e2 = s.acquire_pacing_lease("c1", "w2", 3)
    assert e2 == e1 + 1                                           # new leader, higher fencing token


def test_concurrent_predictive_permits_cannot_exceed_cap():
    """Two workers with valid permits at the same moment: the cap is re-checked inside the
    DB transaction, so together they can't exceed it."""
    path = temp_db()
    env = make_env(agents=5, borrowers=100, mode="PREDICTIVE", path=path)
    p1 = predictive_permit_whitebox(env, 8)     # both computed from the same snapshot,
    p2 = predictive_permit_whitebox(env, 8)     # each says "max 8 unbound in flight"
    results = []

    def run(permit, wid):
        st = Store(path, env.clock, init=False)
        alloc = CallAllocator(st, Lifecycle(st), env.router, env.worker.safety.verifier, wid)
        results.append(alloc.execute(permit))

    ts = [threading.Thread(target=run, args=(p, f"w{i}")) for i, p in enumerate((p1, p2))]
    [t.start() for t in ts]
    [t.join() for t in ts]
    total = env.store.q1("SELECT COUNT(*) c FROM calls WHERE agent_id IS NULL AND state NOT IN "
                         "('COMPLETED','FAILED','CANCELLED','NO_ANSWER','ABANDONED')")["c"]
    assert total == 8, total
    assert sum(r.placed for r in results) == 8
