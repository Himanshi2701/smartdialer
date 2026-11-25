"""Configuration objects. Frozen dataclasses: nothing can mutate them at runtime."""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DialerPolicy:
    # leases / timeouts
    agent_reserve_lease_s: float = 10.0     # how long a RESERVED agent may stay reserved by one worker
    borrower_lease_s: float = 30.0
    setup_lease_s: float = 8.0              # worker must get a provider answer to place_call in this time
    uncertain_giveup_s: float = 30.0        # after this, an un-reconcilable call is failed + hung up
    ring_stale_s: float = 90.0              # no events for this long on a ringing call -> reconcile
    connected_stale_s: float = 3600.0
    # wrap-up
    max_wrap_s: float = 20.0                # WRAP_UP is force-ended after this (makes agents predictable)
    # predictive hold
    max_hold_s: float = 2.0                 # answered predictive call may wait this long for an agent
    # borrower retry policy
    max_attempts: int = 3
    no_answer_retry_s: float = 1800.0
    provider_fail_retry_base_s: float = 15.0
    provider_fail_retry_max_s: float = 600.0
    abandon_cooldown_s: float = 72 * 3600.0  # never re-dial an abandoned borrower quickly (compliance)


@dataclass(frozen=True)
class SafetyConfig:
    # probability bound: P(more answers than free agents) <= epsilon for each pacing decision
    epsilon: float = 0.01
    shed_epsilon: float = 0.10              # cancel ringing calls only if risk is clearly too high
    # abandon-rate budget (rolling window). Rules in some countries (e.g. US/UK) use ~3%; we target 1%.
    abandon_rate_limit: float = 0.01
    abandon_window_s: float = 900.0
    abandon_min_answered: int = 30          # don't judge the rate on tiny samples...
    abandon_hard_count: int = 5             # ...but this many abandons in the window always trips it
    # answer-rate estimation for the safety bound (pessimistic = HIGH answer rate)
    short_window_s: float = 120.0
    long_window_s: float = 900.0
    min_samples: int = 20                   # cold start: below this, progressive only
    z_upper: float = 1.645                  # one-sided 95% upper confidence bound
    # hard caps the pacer can never exceed
    max_lines_per_agent: float = 3.0        # unbound in-flight calls <= this * free capacity
    max_new_calls_per_tick: int = 200       # also a CPS (calls-per-second) guard for the provider
    # agent-availability drop detection
    agent_drop_window_s: float = 10.0
    agent_drop_fraction: float = 0.2        # >=20% of logged-in agents gone within the window
    fallback_cooldown_s: float = 60.0
    permit_ttl_s: float = 5.0


@dataclass(frozen=True)
class PacerConfig:
    target_occupancy: float = 0.9
    lookahead_s: float = 0.0                # 0 -> use measured time-to-answer
    prior_strength: float = 20.0            # pseudo-observations for the historical answer rate
    window_s: float = 300.0
    max_dial_ratio: float = 6.0             # pacer's own sanity cap (safety has its own)
