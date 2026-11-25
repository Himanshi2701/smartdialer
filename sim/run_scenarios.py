"""
Run the simulator under the assignment's scenarios and write a report.

    python sim/run_scenarios.py            # full (30 simulated minutes each, ~1 min wall time)
    python sim/run_scenarios.py --quick    # 10 simulated minutes each

Outputs (docs/reports/):
    SIMULATION_REPORT.md   table + explanation of every run
    results.json           raw summaries
    <scenario>_<mode>.csv  per-second time series
    <scenario>_<mode>.png  charts (agents, pacing vs safety, in-flight, answer rate)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sim.engine import Scenario, Simulation  # noqa: E402
from smartdialer.states import AgentState  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs", "reports")

# reference palette (fixed order): blue, orange, aqua, yellow; neutral gray for "truth"
C1, C2, C3, C4, GRAY = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#8a8984"


def piecewise(points):
    """[(t_start, value), ...] -> f(t)"""
    def f(t):
        v = points[0][1]
        for ts, val in points:
            if t >= ts:
                v = val
        return v
    return f


def scenarios(dur):
    third = dur / 3
    out = []
    for name, ar, tt in [("A", 0.20, 120), ("B", 0.50, 90), ("C", 0.70, 180)]:
        for mode in ("PROGRESSIVE", "PREDICTIVE"):
            out.append((f"{name}: answer {int(ar*100)}%, talk {tt}s", Scenario(name, ar, tt, mode=mode, duration_s=dur)))
    d_ar = piecewise([(0, 0.30), (third, 0.65), (2 * third, 0.10)])
    d_tt = piecewise([(0, 120), (third, 60), (2 * third, 180)])
    for mode in ("PROGRESSIVE", "PREDICTIVE"):
        out.append(("D: changing (30%->65%->10%, talk 120->60->180s)",
                    Scenario("D", d_ar, d_tt, mode=mode, duration_s=dur)))

    # E: provider outage. A times out for a while (fail over to B), then BOTH are down for a bit.
    def a_down(sim):
        sim.providers["A"].set_outage("timeout", event_delay_s=5.0)

    def b_down(sim):
        sim.providers["B"].set_outage("timeout", event_delay_s=5.0)

    def b_up(sim):
        sim.providers["B"].set_outage(None)

    def a_up(sim):
        sim.providers["A"].set_outage(None)
    out.append(("E: provider outage (A down, then A+B down)",
                Scenario("E", 0.25, 120, mode="PREDICTIVE", duration_s=dur,
                         script=[(third, a_down), (third + 60, b_down), (third + 150, b_up), (2 * third, a_up)])))

    # F: 100 agents, 40 disappear within 3 seconds, come back later
    def drop(sim):
        sim.drop_agents(14)

    def back(sim):
        sim.login_agents(40)
    out.append(("F: agent drop (100 agents, 40 vanish in 3s)",
                Scenario("F", 0.25, 120, mode="PREDICTIVE", agents=100, duration_s=dur,
                         script=[(third, drop), (third + 1, drop), (third + 2, lambda s: s.drop_agents(12)),
                                 (2 * third, back)])))
    # G: surge: model has learnt 10%, then 70% answer suddenly (the dangerous direction)
    out.append(("G: answer-rate surge 10% -> 70%",
                Scenario("G", piecewise([(0, 0.10), (third, 0.70)]), 120, mode="PREDICTIVE", duration_s=dur)))
    # H: only the messy provider (slow webhooks, duplicates, reordering, timeouts)
    out.append(("H: only Provider B (slow, duplicates, reorder)",
                Scenario("H", 0.25, 120, mode="PREDICTIVE", providers=("B",), duration_s=dur)))
    # I: 3 workers on one campaign, one crashes in the middle
    out.append(("I: 3 workers, worker w2 crashes",
                Scenario("I", 0.25, 120, mode="PREDICTIVE", workers=3, duration_s=dur,
                         script=[(third, lambda s: s.crashed_workers.add("w2"))])))
    return out


def write_csv(path, samples):
    keys = list(samples[0].keys())
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(samples)


def roll(xs, n=10):
    out, acc = [], 0.0
    for i, x in enumerate(xs):
        acc += x or 0
        if i >= n:
            acc -= xs[i - n] or 0
        out.append(acc)
    return out


def chart(path, title, samples):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    t = [r["t"] / 60 for r in samples]
    fig, ax = plt.subplots(4, 1, figsize=(10, 11), sharex=True)
    fig.suptitle(title + "   (x axis: simulated minutes)", fontsize=12, x=0.01, ha="left", color="#0b0b0b")

    def style(a, ttl):
        a.set_title(ttl, fontsize=10, loc="left", color="#52514e")
        a.grid(axis="y", color="#e6e5e0", linewidth=0.8)
        for sp in ("top", "right"):
            a.spines[sp].set_visible(False)
        for sp in ("left", "bottom"):
            a.spines[sp].set_color("#c3c2b7")
        a.tick_params(colors="#52514e", labelsize=8)

    a = ax[0]
    a.plot(t, [r["connected"] for r in samples], color=C1, lw=2, label="talking (CONNECTED)")
    a.plot(t, [r["wrap_up"] for r in samples], color=C2, lw=2, label="wrap-up")
    a.plot(t, [r["available"] + r["reserved"] + r["dialing"] for r in samples], color=C3, lw=2,
           label="idle / waiting for a call")
    a.plot(t, [r["logged_in"] for r in samples], color=GRAY, lw=1.2, ls="--", label="logged in")
    style(a, "Agents")
    a.legend(fontsize=8, frameon=False, ncol=4, loc="upper left", bbox_to_anchor=(0, -0.08))

    a = ax[1]
    a.plot(t, roll([r["proposed"] for r in samples]), color=GRAY, lw=1.5, label="pacer proposed")
    a.plot(t, roll([r["approved"] for r in samples]), color=C1, lw=2, label="safety approved")
    a.plot(t, roll([r["placed"] for r in samples]), color=C2, lw=1.2, label="actually placed")
    style(a, "New calls per 10 s: pacer proposal vs safety controller")
    a.set_yscale("symlog", linthresh=10)
    a.legend(fontsize=8, frameon=False, ncol=3, loc="upper left", bbox_to_anchor=(0, -0.08))

    a = ax[2]
    a.plot(t, [r["unbound"] for r in samples], color=C1, lw=2, label="predictive calls ringing (no agent yet)")
    a.plot(t, [r["waiting"] for r in samples], color=C2, lw=2, label="answered, waiting for agent (hold)")
    style(a, "Calls in flight")
    a.legend(fontsize=8, frameon=False, ncol=2, loc="upper left", bbox_to_anchor=(0, -0.08))

    a = ax[3]
    a.plot(t, [r["answer_rate_true"] for r in samples], color=GRAY, lw=1.5, ls="--", label="true answer rate")
    a.plot(t, [r["p_est"] if r["p_est"] is not None else float("nan") for r in samples], color=C1, lw=2,
           label="pacer estimate")
    a.plot(t, [r["p_safe"] if r["p_safe"] is not None else float("nan") for r in samples], color=C2, lw=2,
           label="safety (upper bound)")
    fb = [1.02 if r["action"] == "FALLBACK_PROGRESSIVE" else float("nan") for r in samples]
    a.scatter(t, fb, s=6, color=C4, marker="s", label="tick in FALLBACK_PROGRESSIVE")
    a.set_ylim(0, 1.08)
    style(a, "Answer rate: truth vs estimates")
    a.legend(fontsize=8, frameon=False, ncol=4, loc="upper left", bbox_to_anchor=(0, -0.12))
    fig.tight_layout(rect=(0, 0, 1, 0.97), h_pad=2.2)
    fig.savefig(path, dpi=110)
    plt.close(fig)


def first_after(samples, t0, pred):
    for r in samples:
        if r["t"] >= t0 and pred(r):
            return r["t"] - t0
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--no-charts", action="store_true")
    args = ap.parse_args()
    dur = 600 if args.quick else 1800
    os.makedirs(OUT, exist_ok=True)
    results = []
    t_all = time.time()
    for title, sc in scenarios(dur):
        t0 = time.time()
        sim = Simulation(sc)
        s = sim.run()
        s["title"] = title
        s["wall_s"] = round(time.time() - t0, 1)
        tag = f"{sc.name}_{sc.mode.lower()}"
        write_csv(os.path.join(OUT, f"{tag}.csv"), sim.samples)
        if not args.no_charts:
            chart(os.path.join(OUT, f"{tag}.png"), f"{title}  [{sc.mode}]", sim.samples)
        s["chart"] = f"{tag}.png"
        # scenario-specific observations
        third = dur / 3
        if sc.name == "F":
            s["reaction_s_to_fallback"] = first_after(sim.samples, third, lambda r: r["action"] == "FALLBACK_PROGRESSIVE")
            s["shed_total"] = sum(r["shed"] for r in sim.samples)
        if sc.name == "E":
            s["breaker_transitions"] = {k: [(round(t - sim.t0), st) for t, st in b.transitions]
                                        for k, b in sim.router.breakers.items()}
        if sc.name == "G":
            s["seconds_until_p_safe_above_0.6"] = first_after(sim.samples, third,
                                                              lambda r: (r["p_safe"] or 0) >= 0.6)
        results.append(s)
        print(f"{title:55s} {sc.mode:12s} util={s['utilization']:.3f} init={s['calls_initiated']:5d} "
              f"conn={s['connected']:4d} aband={s['abandoned']:3d} ({s['abandon_rate']*100:.2f}%) "
              f"viol={s['invariant_violations']} [{s['wall_s']}s]")
    with open(os.path.join(OUT, "results.json"), "w") as f:
        json.dump(results, f, indent=2, default=str)
    write_report(results, dur)
    print(f"done in {time.time() - t_all:.0f}s -> {OUT}")


def write_report(results, dur):
    L = []
    L.append("# Simulation report\n")
    L.append(f"Generated by `python sim/run_scenarios.py`. Every run: {int(dur/60)} simulated minutes, "
             "1 s pacing tick, seed 7, max wrap-up 20 s (agents actually wrap 5-18 s), "
             "providers = A primary + B failover unless stated. Utilization = share of logged-in agent time "
             "spent talking to a borrower. Abandon = borrower answered but no agent connected within the 2 s hold.\n")
    L.append("## Summary\n")
    L.append("| Scenario | Mode | Agents | Utilization | Occupancy (talk+wrap) | Calls initiated | Connected | "
             "Abandoned | Abandon rate | Safety decisions (ticks) | Invariant violations |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for s in results:
        acts = ", ".join(f"{k} {v['ticks']}" for k, v in sorted(s["safety"].items()))
        L.append(f"| {s['title']} | {s['mode']} | {s['agents']} | **{s['utilization']*100:.1f}%** | "
                 f"{s['occupancy']*100:.1f}% | {s['calls_initiated']} | {s['connected']} | {s['abandoned']} | "
                 f"{s['abandon_rate']*100:.2f}% | {acts} | {s['invariant_violations']} |")
    L.append("")
    by = {(s["scenario"], s["mode"]): s for s in results}
    L.append("## Progressive vs predictive\n")
    L.append("| Scenario | Progressive util. | Predictive util. | Gain | Predictive abandon rate |")
    L.append("|---|---|---|---|---|")
    for k in "ABCD":
        p, q = by.get((k, "PROGRESSIVE")), by.get((k, "PREDICTIVE"))
        if p and q:
            L.append(f"| {q['title']} | {p['utilization']*100:.1f}% | {q['utilization']*100:.1f}% | "
                     f"{(q['utilization']-p['utilization'])*100:+.1f} pts | {q['abandon_rate']*100:.2f}% |")
    L.append("")
    L.append("Reading it: predictive helps most when the answer rate is low (agents in progressive mode spend most "
             "of their time listening to phones ring). When the answer rate is high and calls are long (C), "
             "progressive is already efficient, so there is little left to gain - and the safety controller "
             "correctly refuses to take risk for a tiny benefit.\n")
    L.append("## Failure scenarios - what happened\n")
    for s in results:
        if s["scenario"] in "EFGHI":
            L.append(f"### {s['title']}\n")
            L.append(f"![chart]({s['chart']})\n")
            extra = {k: s[k] for k in ("reaction_s_to_fallback", "shed_total", "breaker_transitions",
                                       "seconds_until_p_safe_above_0.6") if k in s}
            L.append(f"- utilization {s['utilization']*100:.1f}%, connected {s['connected']}, abandoned "
                     f"{s['abandoned']} ({s['abandon_rate']*100:.2f}%), failed {s['failed']}, cancelled "
                     f"{s['cancelled']}, invariant violations {s['invariant_violations']}")
            L.append(f"- safety decisions: `{json.dumps(s['safety'])}`")
            L.append(f"- event handling: `{json.dumps(s['events'])}`")
            L.append(f"- recovery: `{json.dumps(s['recovery'])}`")
            for k, v in extra.items():
                L.append(f"- {k}: `{v}`")
            L.append("")
    L.append("## Charts for the main scenarios\n")
    for s in results:
        if s["scenario"] in "ABCD":
            L.append(f"### {s['title']} - {s['mode']}\n\n![chart]({s['chart']})\n")
    with open(os.path.join(OUT, "SIMULATION_REPORT.md"), "w") as f:
        f.write("\n".join(L))


if __name__ == "__main__":
    main()
