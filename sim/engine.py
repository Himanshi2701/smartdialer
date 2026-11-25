"""
Discrete-time simulation harness.

Everything runs on a SimClock, so a 30-minute campaign takes a few seconds and
the same seed always gives the same result. The harness drives:
  * the mock providers' internal timelines,
  * agent desktop behaviour (wrap-up duration, scripted logouts),
  * one or more DialerWorkers,
and records metrics every tick.
"""
from __future__ import annotations

import os
import random
import sys
from dataclasses import dataclass, field
from typing import Callable, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smartdialer.clock import SimClock
from smartdialer.config import DialerPolicy, PacerConfig, SafetyConfig
from smartdialer.lifecycle import Lifecycle
from smartdialer.pacing import PredictivePacer, ProgressivePacer
from smartdialer.providers import PROFILE_A, PROFILE_B, CalleeWorld, MockProvider, ProviderRouter
from smartdialer.states import AgentState
from smartdialer.store import Store
from smartdialer.worker import DialerWorker


@dataclass
class Scenario:
    name: str
    answer_rate: float | Callable[[float], float]
    talk_mean_s: float | Callable[[float], float]
    mode: str = "PREDICTIVE"              # PREDICTIVE | PROGRESSIVE
    agents: int = 50
    duration_s: float = 1800
    providers: tuple = ("A", "B")          # routing preference: A primary, B failover (the messy one)
    wrap_s: tuple = (5.0, 18.0)            # agents' real wrap-up time (forced max is policy.max_wrap_s)
    hist_answer_rate: float = 0.3
    seed: int = 7
    workers: int = 1
    script: list = field(default_factory=list)   # [(t, callable(sim))]
    borrowers: int = 30000


class Simulation:
    def __init__(self, sc: Scenario, safety_cfg: SafetyConfig = SafetyConfig(),
                 policy: DialerPolicy = DialerPolicy(), pacer_cfg: PacerConfig = PacerConfig(),
                 db_path: str = ":memory:"):
        self.sc = sc
        self.clock = SimClock()
        self.t0 = self.clock.now()
        self.rng = random.Random(sc.seed)
        if sc.workers > 1 and db_path == ":memory:":
            import tempfile
            db_path = os.path.join(tempfile.mkdtemp(), "sim.db")   # real file: each worker gets its own connection
        self.db_path = db_path
        self.store = Store(db_path, self.clock)
        self.policy = policy
        self.cid = "camp-1"
        self.store.add_campaign(self.cid, sc.mode, hist_answer_rate=sc.hist_answer_rate)
        self.store.add_agents(self.cid, sc.agents)
        self.store.add_borrowers(self.cid, [(f"b-{i:06d}", f"+91-0000-{i:06d}", self.rng.random() * 100)
                                            for i in range(sc.borrowers)])
        self.world = CalleeWorld(sc.answer_rate, sc.talk_mean_s)
        self.world.t0 = self.t0
        self.providers = {}
        for i, p in enumerate(sc.providers):
            prof = PROFILE_A if p == "A" else PROFILE_B
            self.providers[p] = MockProvider(f"prov{p}", self.clock, self.world, prof, seed=sc.seed * 10 + i)
        self.router = ProviderRouter([self.providers[p] for p in sc.providers], self.clock)
        pacer = PredictivePacer(pacer_cfg) if sc.mode == "PREDICTIVE" else ProgressivePacer()
        self.workers = [DialerWorker(f"w{i+1}", self.store if i == 0 else Store(db_path, self.clock, init=False),
                                     self.cid, self.router, pacer, safety_cfg, policy) for i in range(sc.workers)]
        self.lc = Lifecycle(self.store, policy)
        self._wrap_target: dict = {}
        self.samples: list[dict] = []
        self.violations: list[str] = []
        self.script = sorted(sc.script, key=lambda x: x[0])
        self.crashed_workers: set = set()

    @property
    def t(self) -> float:
        return self.clock.now() - self.t0

    # ------------------------------------------------------------ agents
    def _agent_behaviour(self):
        now = self.clock.now()
        for r in self.store.q("SELECT id, state_since FROM agents WHERE campaign_id=? AND state='WRAP_UP'", (self.cid,)):
            key = (r["id"], r["state_since"])
            tgt = self._wrap_target.get(key)
            if tgt is None:
                tgt = self._wrap_target[key] = r["state_since"] + self.rng.uniform(*self.sc.wrap_s)
            if now >= tgt:
                self.lc.agent_set_state(r["id"], AgentState.AVAILABLE, "agent-desktop")

    def drop_agents(self, n: int, to: AgentState = AgentState.OFFLINE):
        rows = self.store.q("SELECT id FROM agents WHERE campaign_id=? AND state!='OFFLINE' ORDER BY "
                            "CASE state WHEN 'AVAILABLE' THEN 0 WHEN 'RESERVED' THEN 1 WHEN 'DIALING' THEN 2 ELSE 3 END, id "
                            "LIMIT ?", (self.cid, n))
        for r in rows:
            self.lc.agent_set_state(r["id"], to, "scripted-drop")
        return len(rows)

    def login_agents(self, n: int):
        rows = self.store.q("SELECT id FROM agents WHERE campaign_id=? AND state='OFFLINE' LIMIT ?", (self.cid, n))
        for r in rows:
            self.lc.agent_set_state(r["id"], AgentState.AVAILABLE, "scripted-login")

    # ------------------------------------------------------------ loop
    def step(self, dt: float = 1.0):
        self.clock.advance(dt)
        now = self.clock.now()
        while self.script and self.script[0][0] <= self.t:
            _, fn = self.script.pop(0)
            fn(self)
        for p in self.providers.values():
            p.advance(now)
        self._agent_behaviour()
        reps = [w.tick() for w in self.workers if w.id not in self.crashed_workers]
        if len(self.workers) > 1:
            self.rng.shuffle(self.workers)        # vary which worker gets there first each tick
        rep = next((r for r in reps if r.leader), reps[0] if reps else None)
        self._sample(rep)

    def run(self, check_every: int = 30):
        n = int(self.sc.duration_s)
        for i in range(n):
            self.step(1.0)
            if i % check_every == 0:
                v = self.store.check_invariants()
                if v:
                    self.violations.extend(f"t={self.t:.0f}: {x}" for x in v)
        # drain: stop dialing, let calls finish
        return self.summary()

    def _sample(self, rep):
        s = self.store
        counts = {r["state"]: r["c"] for r in s.q("SELECT state, COUNT(*) c FROM agents WHERE campaign_id=? GROUP BY state",
                                                   (self.cid,))}
        row = dict(t=self.t, **{k.lower(): counts.get(k.value, 0) for k in AgentState})
        row["logged_in"] = sum(v for k, v in counts.items() if k != "OFFLINE")
        d = rep.decision if rep else None
        row.update(proposed=d.proposed if d else 0, approved=d.approved if d else 0, action=d.action if d else "",
                   safety_mode=d.mode if d else "", shed=rep.shed if rep else 0,
                   placed=rep.allocation.placed if rep and rep.allocation else 0,
                   unbound=rep.snapshot.unbound_inflight if rep and rep.snapshot else 0,
                   waiting=rep.snapshot.waiting_answered if rep and rep.snapshot else 0,
                   answer_rate_true=self.world.answer_rate(self.clock.now()),
                   p_est=(rep.proposal.rationale.get("answer_rate_est") if rep and rep.proposal else None),
                   p_safe=(d.reasons.get("p_safe") if d else None),
                   reason=(d.reasons.get("rule") if d else ""))
        self.samples.append(row)

    # ------------------------------------------------------------ results
    def summary(self) -> dict:
        s, cid = self.store, self.cid
        q = lambda sql: s.q1(sql, (cid,))["c"]
        initiated = q("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND initiated_at IS NOT NULL")
        answered = q("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND answered_at IS NOT NULL")
        connected = q("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND connected_at IS NOT NULL")
        completed = q("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND state='COMPLETED'")
        abandoned = q("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND state='ABANDONED'")
        failed = q("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND state='FAILED'")
        cancelled = q("SELECT COUNT(*) c FROM calls WHERE campaign_id=? AND state='CANCELLED'")
        tot_li = sum(r["logged_in"] for r in self.samples) or 1
        util = sum(r["connected"] for r in self.samples) / tot_li
        occ = sum(r["connected"] + r["wrap_up"] for r in self.samples) / tot_li
        idle = sum(r["available"] + r["reserved"] + r["dialing"] for r in self.samples) / tot_li
        actions = {}
        for r in s.q("SELECT action, COUNT(*) c, SUM(proposed) p, SUM(approved) a FROM safety_log WHERE campaign_id=? "
                     "GROUP BY action", (cid,)):
            actions[r["action"]] = dict(ticks=r["c"], proposed=r["p"], approved=r["a"])
        ev = {}
        for w in self.workers:
            for k, v in w.events.counters.items():
                ev[k] = ev.get(k, 0) + v
        rec = {}
        for w in self.workers:
            for k, v in w.recovery.counters.items():
                rec[k] = rec.get(k, 0) + v
        mins = self.sc.duration_s / 60
        return dict(
            scenario=self.sc.name, mode=self.sc.mode, agents=self.sc.agents, minutes=round(mins, 1),
            utilization=round(util, 3), occupancy=round(occ, 3), idle_fraction=round(idle, 3),
            calls_initiated=initiated, answered=answered, connected=connected, completed=completed,
            abandoned=abandoned, abandon_rate=round(abandoned / answered, 4) if answered else 0.0,
            failed=failed, cancelled=cancelled,
            connects_per_agent_hour=round(connected / max(1, self.sc.agents) / (mins / 60), 2),
            safety=actions, events=ev, recovery=rec,
            provider_stats={k: dict(p.stats) for k, p in self.providers.items()},
            invariant_violations=len(self.violations),
        )
