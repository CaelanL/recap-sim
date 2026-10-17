"""Probabilistic failure-mode sweep over the SIMULATION (stochastic hazards).

Runs many seeds per fault configuration and measures how often each failure
mode emerges. Percentages are properties of this simulated model with the
assumed hazard rates, NOT measurements of the real system; they rank relative
likelihood given the model, and every assumption is tunable.
"""
import json, sys, time
sys.path.insert(0, "/home/user/recap-sim")
from recap_sim.sim import Simulation

SEEDS = int(sys.argv[1]) if len(sys.argv) > 1 else 200
SIM_MIN = 15
COMMON = {"duration_sim_minutes": SIM_MIN, "recaps_per_5min": 200,
          "users": 500, "chunks_per_recap": 3}

HZ_PRE = lambda p: {"type": "crash_hazard", "after": "adf_write", "prob": p}
HZ_POST = lambda p: {"type": "crash_hazard", "after": "processed_insert", "prob": p}
LAG = lambda s: {"type": "pg_replica_lag", "seconds": s}
PART_MONO = {"type": "partition", "edge": "processor->monolith",
             "start": 240.0, "end": 420.0}
DROP = lambda e, p: {"type": "drop_prob", "edge": e, "prob": p}

CONFIGS = [
    ("baseline_no_faults", []),
    ("hazard_pre_0.1pct", [HZ_PRE(0.001)]),
    ("hazard_pre_0.5pct", [HZ_PRE(0.005)]),
    ("hazard_pre_2pct", [HZ_PRE(0.02)]),
    ("hazard_post_0.5pct_no_lag", [HZ_POST(0.005)]),
    ("hazard_post_0.5pct_lag60", [HZ_POST(0.005), LAG(60)]),
    ("hazard_post_0.5pct_lag300", [HZ_POST(0.005), LAG(300)]),
    ("hazard_post_2pct_lag60", [HZ_POST(0.02), LAG(60)]),
    ("hazard_pre_0.5pct_plus_lag60", [HZ_PRE(0.005), LAG(60)]),
    ("drop_2pct_monolith_lag60", [DROP("processor->monolith", 0.02), LAG(60)]),
    ("drop_2pct_plus_hazard_post_lag60",
     [DROP("processor->monolith", 0.02), HZ_POST(0.005), LAG(60)]),
    ("partition_plus_hazard_post_lag60", [PART_MONO, HZ_POST(0.005), LAG(60)]),
    ("full_bug_partition_lag_hazard",
     [PART_MONO, LAG(60), HZ_PRE(0.005), HZ_POST(0.005)]),
]


def outcomes(sim):
    pg = sim.postgres
    adf = pg.table("adf_reports").rows
    by = {}
    for r in adf:
        by[r["recap_id"]] = by.get(r["recap_id"], 0) + 1
    dup_adf = {k: v for k, v in by.items() if v > 1}
    notifs = pg.table("cc_notifications").rows
    nby = {}
    for n in notifs:
        nby[n["recap_id"]] = nby.get(n["recap_id"], 0) + 1
    dup_notif = {k: v for k, v in nby.items() if v > 1}
    ana = [r for r in pg.table("cloud.event_track").rows
           if r.get("kind") == "recap_analytic"]
    aby = {}
    for a in ana:
        aby[a["recap_id"]] = aby.get(a["recap_id"], 0) + 1
    dup_ana = {k: v for k, v in aby.items() if v > 1}
    emitted = sim.spark_jobs.recaps_emitted
    missed = max(0, emitted - len(by))
    return {
        "dup_adf_runs": 1 if dup_adf else 0,
        "dup_adf_recaps": len(dup_adf),
        "dup_notif_runs": 1 if dup_notif else 0,
        "dup_notif_recaps": len(dup_notif),
        "dup_ana_runs": 1 if dup_ana else 0,
        "dlq_stuck_runs": 1 if (sim.dlq1.depth + sim.dlq2.depth) > 0 else 0,
        "dlq_stuck_msgs": sim.dlq1.depth + sim.dlq2.depth,
        "missed_recaps": missed,
        "missed_runs": 1 if missed > 10 else 0,
        "streamhub_stuck_runs": 1 if sim.streamhub_failure.depth > 0 else 0,
        "emitted": emitted,
    }


def main():
    results = {}
    t0 = time.time()
    for name, faults in CONFIGS:
        agg = None
        for seed in range(1, SEEDS + 1):
            cfg = dict(COMMON)
            cfg["seed"] = seed
            cfg["faults"] = faults
            sim = Simulation(cfg)
            sim.run()
            o = outcomes(sim)
            if agg is None:
                agg = {k: 0 for k in o}
            for k, v in o.items():
                agg[k] += v
        n = SEEDS
        results[name] = {
            "runs": n,
            "pct_dup_adf": round(100.0 * agg["dup_adf_runs"] / n, 1),
            "mean_dup_adf_recaps_per_affected":
                round(agg["dup_adf_recaps"] / agg["dup_adf_runs"], 1)
                if agg["dup_adf_runs"] else 0,
            "pct_dup_notifications": round(100.0 * agg["dup_notif_runs"] / n, 1),
            "mean_dup_notif_recaps_per_affected":
                round(agg["dup_notif_recaps"] / agg["dup_notif_runs"], 1)
                if agg["dup_notif_runs"] else 0,
            "pct_dup_analytics": round(100.0 * agg["dup_ana_runs"] / n, 1),
            "pct_dlq_stuck_at_end": round(100.0 * agg["dlq_stuck_runs"] / n, 1),
            "pct_missed_recaps": round(100.0 * agg["missed_runs"] / n, 1),
            "mean_missed_per_run": round(agg["missed_recaps"] / n, 1),
            "pct_streamhub_stuck": round(100.0 * agg["streamhub_stuck_runs"] / n, 1),
            "mean_recaps_per_run": round(agg["emitted"] / n, 1),
        }
        print("%-40s dup_adf=%5.1f%% (x%-4.1f) dup_notif=%5.1f%% dlq=%4.1f%% missed=%4.1f%% [%.0fs]"
              % (name, results[name]["pct_dup_adf"],
                 results[name]["mean_dup_adf_recaps_per_affected"],
                 results[name]["pct_dup_notifications"],
                 results[name]["pct_dlq_stuck_at_end"],
                 results[name]["pct_missed_recaps"], time.time() - t0),
              flush=True)
    json.dump({"seeds_per_config": SEEDS, "sim_minutes": SIM_MIN,
               "common": COMMON, "results": results},
              open("/home/user/recap-sim/sweep_results.json", "w"), indent=2)
    print("saved sweep_results.json")


if __name__ == "__main__":
    main()
