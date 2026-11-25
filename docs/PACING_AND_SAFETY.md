# Pacing and safety: why the system made *this many* calls

Two separate brains with two separate jobs:

| | Pacing Engine | Safety Controller |
|---|---|---|
| Goal | keep agents busy (the **average** case) | never strand an answered borrower (the **worst** case) |
| Answer rate it uses | best estimate | pessimistic upper bound |
| Output | "I think we can start N" | APPROVE / REDUCE / REJECT / FALLBACK_PROGRESSIVE (+ shed) and a signed permit |
| Can it place calls? | no | no - but nothing is placed without its permit |

## 1. Progressive pacer

`calls = available agents` (minus any caller already waiting). Each call reserves its agent *before* dialing, so
with 50 agents there can never be more than 50 agent-bound calls (`tests/test_progressive.py::test_50_agents_never_more_than_50_calls`).

Which borrower: highest `priority` first (e.g. days-past-due x amount), only if `next_attempt_at` has passed.
Which agent: the one idle the longest (`ORDER BY state_since`) - fair, and spreads load.

## 2. Predictive pacer (`PredictivePacer`)

```
p        = (answered_recent + k * historical_rate) / (attempts_recent + k)        k = 20
T        = average dial -> answer time (measured)
free(T)  = agents free now
         + WRAP_UP agents whose forced wrap ends within T
         + CONNECTED agents expected to finish talk + wrap within T   (exponential talk time)
owed     = answered calls waiting for an agent
         + sum over ringing calls of P(still answers | ringing for `age` seconds)
calls    = floor( (0.9 * free(T) - owed) / p )                    capped at 6 * free(T)
```

Inputs mapped to the assignment's list:

| Assignment factor | Where it is used |
|---|---|
| current agent availability | `free now` |
| calls already connected | `CONNECTED` agents contribute expected releases; they are not free |
| calls currently ringing | `owed`, **age-aware**: a call ringing 20 s is much less likely to answer than a new one |
| historical answer rate | Bayesian prior (`hist_answer_rate`, weight k = 20) |
| call setup time | `T` and the time-to-answer distribution |
| average call duration | probability a connected agent frees up within `T` |
| provider health | safety controller (breakers, webhook lag) |
| recent campaign behaviour | 2-minute window if it has >= 50 attempts, else 15-minute window |

### Age-aware ringing probability (`stats.p_remaining`)

From the observed distribution of dial -> answer times, `S(a)` = fraction of answered calls that took longer than
`a` seconds. By Bayes:

```
P(will answer | not answered after a seconds) = p*S(a) / (p*S(a) + (1-p))
```

With p = 0.25 and answers usually arriving 4-15 s after dialing: a brand-new call is 0.25, one ringing for 10 s
is ~0.14, one ringing for 20 s is ~0.001. The first version treated them all as 0.25, which made the safety controller
cancel perfectly harmless old calls - utilization in scenario A went from 63% to 68% and shedding from 249
cancelled calls to 0 once this was added.

## 3. Safety controller rules (`SafetyController.evaluate`)

| # | Rule | Effect |
|---|---|---|
| R1 | campaign inactive | REJECT |
| R2 | no healthy provider (all breakers open) | REJECT; half-open -> exactly 1 probe call |
| R3a | agent drop: >= 20% of logged-in agents went OFFLINE/PAUSED in 10 s | FALLBACK_PROGRESSIVE for 60 s + shed at epsilon |
| R3b | abandon budget: >= 1% abandon rate over 15 min (with >= 30 answers) or >= 5 abandons | FALLBACK_PROGRESSIVE for 60 s |
| R3c | no provider whose webhook p95 lag <= 1.5 s | FALLBACK_PROGRESSIVE (predictive bridging needs fast webhooks) |
| R3d | cold start: < 20 resolved attempts | FALLBACK_PROGRESSIVE |
| R3e | cool-down from an earlier trigger | FALLBACK_PROGRESSIVE |
| R4 | Poisson-binomial bound | REDUCE to the largest N with P(answers > C) <= 1% |
| R5 | hard caps: <= 3 unbound calls per unit of capacity, <= 200 new calls per tick | REDUCE |
| shed | if the calls already ringing are now too risky (> 10%, or > 1% right after an agent drop) | cancel the newest ones before they are answered |

FALLBACK_PROGRESSIVE is fully deterministic: it allows
`available - waiting - (still-ringing predictive calls)` agent-bound calls, i.e. even if **every** predictive call
still ringing is answered, there is an agent for it.

### R4 in detail

* `p_safe = max(UCB_short, UCB_long)` - one-sided 95% Wilson upper bound of the answer rate in the 2-minute and the
  15-minute window. Using the **max** is deliberate:
  * answer rate jumps up (10% -> 70%): the short window catches it within a couple of minutes;
  * answer rate crashes (70% -> 10%): the long window keeps p_safe high for 15 minutes, so the system does not
    respond to the crash by dialing 10 lines per agent and then get flooded when the rate bounces back.
* `C` = agents free now - callers already waiting + WRAP_UP agents whose *forced* wrap-up ends before the earliest
  plausible answer (5th percentile time-to-answer + 2 s hold). These are very likely, not certain, to be free: an
  agent could pause instead. That residual risk is covered by epsilon and the 2 s hold.
* Each ringing call i gets `p_i = p_remaining(p_safe, age_i)`; each new call gets `p_safe`.
* The number of answers is Poisson-binomial; its distribution is computed exactly, truncated at C
  (`stats.OverflowDist`, O(C) per call). New calls are added one at a time while `P(answers > C) <= 0.01`.

## 4. Worked examples (real output of the code)

Numbers below come from running the pacer and the safety controller on a fixed snapshot
(10 free agents, 30 talking, 2 in wrap-up with 3 s and 12 s left, answer-time 4-15 s).

**Example 1 - calm, 25% answer rate, nothing ringing**

| Pacer | value | Safety | value |
|---|---|---|---|
| p (estimate) | 0.245 | p_safe (upper bound, short window) | 0.303 |
| free within T = 10 + 1 wrap-up | 11 | capacity C = 10 free + 1 wrap-up forced to end in time | 11 |
| need = 0.9 x 11 - 0 | 9.9 | largest N with P(Bin(N, 0.303) > 11) <= 1% | **21** |
| calls = 9.9 / 0.245 | **40** | P(overflow) at 21 calls | 0.0097 |

Decision: **REDUCE 40 -> 21**. "Why 21 and not 40?" Because with 40 calls and a plausible answer rate of 30%, the
chance that more than 11 people pick up at once is far above 1%. "Why 21 and not 11 (progressive)?" Because 21 calls
at <= 30% answer rate overflow 11 agents less than 1% of the time.

**Example 2 - same, but 8 calls already ringing (ages 1,1,12,14,18,20,22,25 s)**

Pacer: expected answers from those 8 = 0.66 (only the two new ones matter) -> proposes 37.
Safety: expected answers in flight 0.83 at p_safe -> approves **18** (= 21 minus roughly the risk the 8 already carry).

**Example 3 - answer rate surge: last 2 minutes show 60% answering**

Pacer: p = 0.53 -> proposes 18. Safety: UCB_short = 0.677 dominates the long window's 0.273 -> approves **12**
(barely above one call per free agent). The model is not trusted to have "learned" the surge; the bound tightens
as soon as the data shows it.

Every decision is stored in the `safety_log` table with all of these fields, so for any second of any run you can
answer "why did it make this many calls right now".

## 5. Results (from `docs/reports/SIMULATION_REPORT.md`, 50 agents, 30 simulated minutes)

| Scenario | Progressive utilization | Predictive utilization | Predictive abandon rate |
|---|---|---|---|
| A: 20% answer, 120 s talk | 43.0% | **67.6%** | 0.00% |
| B: 50% answer, 90 s talk | 62.8% | **73.2%** | 0.00% |
| C: 70% answer, 180 s talk | 82.6% | **85.0%** | 0.00% |
| D: changing 30% -> 65% -> 10%, talk 120 -> 60 -> 180 s | 47.2% | **66.6%** | 0.00% |

Utilization = share of logged-in agent time spent talking. The remaining time is wrap-up (forced max 20 s) and
waiting. Progressive mode has 0 abandons by construction in every run.

## 6. What could have been done instead

| Approach | Why not (here) |
|---|---|
| Erlang-C / Erlang-A queueing model | Good for *staffing*; assumes stationary arrival rates. Pacing needs a per-second decision with small, changing numbers - an explicit probability bound is easier to explain and to make safe |
| Fixed dial ratio (e.g. 2.5 lines per agent) | Simple, but wrong whenever the answer rate changes; no safety guarantee |
| Target-abandon PID controller (classic predictive dialers) | Adjusts the ratio after abandons happen - it *uses* abandons as feedback. Here the bound acts before the call is placed; the abandon budget is only a second line of defence |
| ML answer-rate model per borrower | Could raise p accuracy (time of day, past behaviour). It would plug into `p` and `p_i` - the safety bound stays the same |
| Normal approximation instead of exact Poisson-binomial | Needed at very large C (thousands of agents in one pool) for speed; exact is cheap and more correct at prototype sizes |
