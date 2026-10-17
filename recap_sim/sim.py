"""Simulation orchestrator: builds the pipeline, applies fault injection,
runs scenarios, checks invariants. SIMULATED - plausible reconstruction, not
a claimed real incident.

Workflow modeled (per Caelan's authoritative description):
 1 execute query in Socrates; 2 on success enqueue one chunk-pointer event
 per chunk index; 3 chunk downloader lambdas receive pointer events;
 4 retrieve recap reports data from Socrates GW per chunk; 5 build recap JSON
 events and distribute to SQS 1/2 round-robin; 6 processors batch-pull recap
 events; 7 idempotency check against PostgreSQL; 8 generate ADF report and
 save to the user's personal space; 9 notify the user; 10 insert successfully
 processed records; 11 send track events to Analytics SQS; 12 analytics pull
 track events; 13 send events via analytics client (to cloud.event_track).
scratch.recap_reports is the prepared data set table; Socrates GW is the
query access layer used to retrieve large prepared data sets."""
import collections, itertools, json

from .engine import Engine, Network, Log
from .aws import FakeQueue
from .store import Postgres
from .components import (DbSparkJobs, StatementExecutor, DownloadChunks,
                         SocratesGw, RecapReportsProcessor,
                         AnalyticsProcessor, StreamhubReplayer, RecapReplayer)

# Compressed scale: the real system serves 8.5M users / 140k tenants at ~8,000
# recaps per 5 minutes. The simulation runs thousands of synthetic users at a
# reduced recap rate; ratios and mechanics, not absolute volume, are the point.
DEFAULTS = {
    "seed": 42,
    "users": 2000,
    "tenants": 140,          # stands in for 140k at 1:1000 compression
    "recaps_per_5min": 400,  # 8,000 real, compressed 20:1
    "chunks_per_recap": 3,
    "duration_sim_minutes": 30,
    "visibility_timeout": 30.0,
    "max_receive_count": 5,
    "workers_per_group": 4,  # real groups run up to ~40 lambdas
    "drop_prob": 0.0,
    "faults": [],
}


class Simulation:
    def __init__(self, config=None):
        cfg = dict(DEFAULTS)
        cfg.update(config or {})
        self.cfg = cfg
        self.horizon = cfg["duration_sim_minutes"] * 60.0
        self.tenants = cfg["tenants"]
        self.chunks_per_recap = cfg["chunks_per_recap"]
        self.engine = Engine(cfg["seed"])
        self._msg_counter = itertools.count(1)
        self.log = Log()
        self.net = Network(self.engine, drop_prob=cfg["drop_prob"])
        self.postgres = Postgres()
        self.pg_lag = 0.0  # replica lag visible to idempotency checks
        self._rr = 0       # round-robin counter for SQS distribution

        # Queues: main SQS 1/2 with DLQs, analytics, streamhub failure
        self.dlq1 = FakeQueue(self, "dlq_1")
        self.dlq2 = FakeQueue(self, "dlq_2")
        self.queue1 = FakeQueue(self, "sqs_queue_1",
                                visibility_timeout=cfg["visibility_timeout"],
                                max_receive_count=cfg["max_receive_count"],
                                dlq=self.dlq1)
        self.queue2 = FakeQueue(self, "sqs_queue_2",
                                visibility_timeout=cfg["visibility_timeout"],
                                max_receive_count=cfg["max_receive_count"],
                                dlq=self.dlq2)
        self.analytics_queue = FakeQueue(self, "analytic_events_sqs")
        self.streamhub_failure = FakeQueue(self, "streamhub_failure_sqs")

        # Components (one class per diagram box; easy to correct/extend)
        self.socrates_gw = SocratesGw(self)
        self.spark_jobs = DbSparkJobs(self, cfg["users"],
                                      cfg["recaps_per_5min"],
                                      cfg["chunks_per_recap"])
        self.statement_executor = StatementExecutor(self)
        self.download_chunks = DownloadChunks(self, n_lambdas=5)
        self.processors = ([RecapReportsProcessor(self, "group1", i)
                            for i in range(1, cfg["workers_per_group"] + 1)] +
                           [RecapReportsProcessor(self, "group2", i)
                            for i in range(1, cfg["workers_per_group"] + 1)])
        self.analytics = [AnalyticsProcessor(self, "analytics_processor_1"),
                          AnalyticsProcessor(self, "analytics_processor_2")]
        self.streamhub_replayer = StreamhubReplayer(self)
        self.replayers = [RecapReplayer(self, self.queue1, "recap_replayer_1"),
                          RecapReplayer(self, self.queue2, "recap_replayer_2")]

        self._apply_faults(cfg["faults"])

    def log_event(self, source, event, data=None):
        self.log.add(self.engine.now, source, event, data)

    def _apply_faults(self, faults):
        self.active_faults = []
        for f in faults:
            f = dict(f)
            kind = f.pop("type")
            self.active_faults.append({"type": kind, **f})
            if kind == "pg_replica_lag":
                self.pg_lag = f["seconds"]
            elif kind == "worker_crash_after_write":
                target = f.get("target", "all")
                for w in self.processors:
                    if target in ("all", w.group, w.name):
                        w.crash_after_write = True
                        w._crash_budget = f.get("times", 1)
            elif kind == "crash_hazard":
                # after: "adf_write" (before processed insert) or
                #        "processed_insert" (before SQS delete)
                after = f.get("after", "adf_write")
                for w in self.processors:
                    if after == "adf_write":
                        w.hazard_pre_insert = f["prob"]
                    elif after == "processed_insert":
                        w.hazard_post_insert = f["prob"]
                    else:
                        w.hazard_after_notify = f["prob"]
            elif kind == "partition":
                self.net.add_partition(f["edge"], f["start"], f["end"])
            elif kind == "drop_prob":
                self.net.configure_edge(f.get("edge", "*"),
                                        drop_prob=f["prob"])
            else:
                raise ValueError("unknown fault type: %r" % kind)

    def start(self):
        self.spark_jobs.start()
        self.download_chunks.start()
        for w in self.processors:
            w.start()
        for a in self.analytics:
            a.start()
        self.streamhub_replayer.start()
        for r in self.replayers:
            r.start()

    def run(self):
        self.start()
        self.engine.run(until=self.horizon, max_events=2_000_000)
        return self.summary()

    # ---- observability -------------------------------------------------
    def snapshot(self):
        return {
            "now": round(self.engine.now, 2),
            "simulated": True,
            "queues": [self.queue1.stats(), self.queue2.stats(),
                       self.analytics_queue.stats(),
                       self.streamhub_failure.stats()],
            "postgres": self.postgres.stats(),
            "components": [c.stats() for c in self._all_components()],
            "log_tail": [{"t": t, "src": s, "event": e, "data": d}
                         for t, s, e, d in self.log.tail(30)],
        }

    def _all_components(self):
        return [self.spark_jobs, self.statement_executor, self.download_chunks,
                self.socrates_gw] + self.processors + self.analytics + \
               [self.streamhub_replayer] + self.replayers

    def digest(self):
        return self.engine.digest(self.log.lines())

    def summary(self):
        reports = self.postgres.table("adf_reports").rows
        by_recap = collections.Counter(r["recap_id"] for r in reports)
        duplicates = {k: v for k, v in by_recap.items() if v > 1}
        notifs = self.postgres.table("cc_notifications").rows
        notif_dups = {k: v for k, v in
                      collections.Counter(n["recap_id"] for n in notifs).items()
                      if v > 1}
        return {
            "simulated": True,
            "seed": self.cfg["seed"],
            "config": {k: v for k, v in self.cfg.items() if k != "faults"},
            "faults": getattr(self, "active_faults", []),
            "sim_minutes": round(self.engine.now / 60.0, 2),
            "events_fired": self.engine.events_fired,
            "recaps_emitted": self.spark_jobs.recaps_emitted,
            "adf_reports_written": len(reports),
            "duplicate_report_rows": len(duplicates),
            "duplicate_examples": dict(list(sorted(duplicates.items()))[:10]),
            "cc_notifications": len(notifs),
            "duplicate_notifications": len(notif_dups),
            "idempotency": {"processed_records":
                            self.postgres.table("processed_recaps").count()},
            "queues": {q.name: q.stats() for q in
                       [self.queue1, self.queue2, self.analytics_queue,
                        self.streamhub_failure]},
            "digest": self.digest(),
        }


# ---- scenarios ---------------------------------------------------------

def baseline_config(seed=42, **kw):
    c = {}
    if seed is not None:
        c["seed"] = seed
    c.update(kw)
    return c


def bug_config(seed=1337, **kw):
    """Act 2: the plausible failure, re-homed to the explicit Postgres
    idempotency check (workflow step 7). A processor worker crashes after
    writing the ADF report but BEFORE inserting the processed record and
    deleting the SQS message; a Monolith outage pushes other attempts through
    visibility-timeout expiry to the DLQ; the Recap Replayer redrives them;
    and a Postgres replica lag means the redriven message idempotency check
    reads a stale view with no processed record. Result: duplicate ADF
    reports and duplicate user notifications. Reproduces exactly on the same
    seed. SIMULATED reconstruction, not a real incident."""
    c = {
        "faults": [
            {"type": "pg_replica_lag", "seconds": 90.0},
            {"type": "worker_crash_after_write", "target": "group1",
             "times": 2},
            {"type": "partition", "edge": "processor->monolith",
             "start": 240.0, "end": 420.0},
        ],
    }
    if seed is not None:
        c["seed"] = seed
    c.update(kw)
    return c


SCENARIOS = {
    "baseline": baseline_config,
    "bug": bug_config,
}


# ---- act 3: invariants / chaos experiments ----------------------------

def check_invariants(sim):
    """Deterministic post-run invariant checks."""
    reports = sim.postgres.table("adf_reports").rows
    by_recap = collections.Counter(r["recap_id"] for r in reports)
    dups = {k: v for k, v in by_recap.items() if v > 1}
    notifs = sim.postgres.table("cc_notifications").rows
    ndups = {k: v for k, v in
             collections.Counter(n["recap_id"] for n in notifs).items() if v > 1}
    analytics = [r for r in sim.postgres.table("cloud.event_track").rows
                 if r.get("kind") == "recap_analytic"]
    adups = {k: v for k, v in
             collections.Counter(a["recap_id"] for a in analytics).items()
             if v > 1}
    dlq_left = sim.dlq1.depth + sim.dlq2.depth
    results = [
        {"invariant": "no_duplicate_adf_reports",
         "passed": not dups,
         "evidence": {"duplicate_rows": len(dups),
                      "examples": dict(list(sorted(dups.items()))[:5])}},
        {"invariant": "no_duplicate_cc_notifications",
         "passed": not ndups,
         "evidence": {"duplicate_notifications": len(ndups)}},
        {"invariant": "no_duplicate_analytic_events",
         "passed": not adups,
         "evidence": {"duplicate_events": len(adups)}},
        {"invariant": "dlq_drains_by_end_of_run",
         "passed": dlq_left == 0,
         "evidence": {"dlq_depth_remaining": dlq_left}},
        {"invariant": "every_emitted_recap_has_a_report",
         "passed": len(by_recap) >= sim.spark_jobs.recaps_emitted - 50,
         "evidence": {"emitted": sim.spark_jobs.recaps_emitted,
                      "distinct_recaps_with_report": len(by_recap),
                      "note": "small tail may still be in flight at horizon"}},
    ]
    return results


def run_experiment(spec):
    """Act 3 hook: an agent proposes chaos experiments + invariants as a
    JSON spec; the simulator runs it deterministically and verifies."""
    cfg = {"seed": spec.get("seed", 7),
           "duration_sim_minutes": spec.get("duration_sim_minutes", 30),
           "faults": spec.get("faults", [])}
    cfg.update(spec.get("params", {}))
    sim = Simulation(cfg)
    sim.run()
    all_inv = {r["invariant"]: r for r in check_invariants(sim)}
    chosen = spec.get("invariants") or list(all_inv)
    results = [all_inv[k] for k in chosen if k in all_inv]
    return {"experiment": spec.get("name", "unnamed"),
            "simulated": True, "seed": cfg["seed"],
            "invariants": results,
            "all_passed": all(r["passed"] for r in results),
            "summary": {k: sim.summary()[k] for k in
                        ("adf_reports_written", "duplicate_report_rows",
                         "cc_notifications", "duplicate_notifications",
                         "digest")}}
