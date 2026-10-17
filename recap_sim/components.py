"""Pipeline actors, one per box in the architecture diagram. SIMULATED.

Each component is easy to correct/extend: behavior lives in small methods and
all cross-box traffic goes through sim.net.send(edge, ...) or FakeQueue, so
Caelan can adjust one box without touching the rest.
"""
import collections

from .aws import FakeQueue


class Component:
    def __init__(self, sim, name):
        self.sim = sim
        self.name = name
        self.metrics = collections.Counter()
        self.alive = True

    def log(self, event, data=None):
        self.sim.log.add(self.sim.engine.now, self.name, event, data)

    def schedule(self, delay, fn):
        self.sim.engine.schedule(delay, fn)

    def stats(self):
        return {"name": self.name, "metrics": dict(self.metrics)}


class DbSparkJobs(Component):
    """Flows 1,2: emits recap source rows into scratch.recap_reports and
    cloud.event_track. Drives the whole simulation's synthetic workload."""

    def __init__(self, sim, users, recaps_per_5min, chunks_per_recap):
        super().__init__(sim, "db_spark_jobs")
        self.users = users
        self.recaps_per_5min = recaps_per_5min
        self.chunks_per_recap = chunks_per_recap
        self.scratch = sim.postgres.table("scratch.recap_reports")
        self.event_track = sim.postgres.table("cloud.event_track")
        self.recaps_emitted = 0

    def start(self):
        self._tick()

    def _tick(self):
        if not self.alive or self.sim.engine.now >= self.sim.horizon:
            return
        # Poisson-ish arrival via seeded rng; one recap per tick.
        interval = 300.0 / max(1, self.recaps_per_5min)
        jitter = self.sim.engine.rng.uniform(0.5, 1.5) * interval
        self.schedule(jitter, self._tick)
        user_id = self.sim.engine.rng.randrange(self.users)
        recap_id = "recap-%06d" % self.recaps_emitted
        self.recaps_emitted += 1
        self.scratch.insert({"recap_id": recap_id, "user_id": user_id,
                             "tenant": user_id % self.sim.tenants,
                             "status": "ready"})
        self.metrics["recaps_emitted"] += 1
        self.log("recap_ready", {"recap_id": recap_id, "user_id": user_id})
        self.sim.statement_executor.enqueue(recap_id, user_id)


class StatementExecutor(Component):
    """Flow 1b/2b: turns each scratch row into Statement Chunk events and
    calls Socrates GW."""

    def __init__(self, sim):
        super().__init__(sim, "statement_executor")
        self.chunk_events = []  # pending chunk events (fan-out source)

    def enqueue(self, recap_id, user_id):
        self.schedule(self.sim.engine.rng.uniform(0.1, 0.5),
                      lambda: self._process(recap_id, user_id))

    def _process(self, recap_id, user_id):
        # Step 1-2 of the real workflow: execute the query in Socrates
        # (initiated from the data bot result), and on success enqueue one
        # small POINTER message per downloadable chunk index. The chunk queue
        # holds pointers, not recap data.
        n = self.sim.chunks_per_recap
        ok = self.sim.net.send("statement_executor->socrates_gw",
                               lambda: self.sim.socrates_gw.start_query(recap_id, n),
                               self.sim.engine.rng)
        if not ok:
            self.metrics["socrates_query_failures"] += 1
            self.log("socrates_query_start_failed", {"recap_id": recap_id})
            self.schedule(self.sim.engine.rng.uniform(2.0, 6.0),
                          lambda: self._process(recap_id, user_id))
            return
        for i in range(n):
            ev = {"recap_id": recap_id, "user_id": user_id,
                  "chunk_index": i, "kind": "chunk_pointer"}
            self.chunk_events.append(ev)
            self.metrics["chunk_events_emitted"] += 1
        self.log("statement_chunks_enqueued", {"recap_id": recap_id, "n": n})


class DownloadChunks(Component):
    """Flows 3,4,5: five lambdas consume Statement Chunk events, fetch chunk
    data via Socrates GW, and publish recap jobs to SQS Queue 1/2 once all
    chunks of a recap are downloaded."""

    def __init__(self, sim, n_lambdas=5):
        super().__init__(sim, "download_chunks")
        self.n_lambdas = n_lambdas
        self.downloaded = collections.defaultdict(set)
        self.recap_user = {}
        self._pending = []

    def start(self):
        for i in range(self.n_lambdas):
            self._poll(i)

    def _poll(self, i):
        if not self.alive:
            return
        self.schedule(self.sim.engine.rng.uniform(0.2, 0.8),
                      lambda: self._poll(i))
        if self.sim.statement_executor.chunk_events:
            ev = self.sim.statement_executor.chunk_events.pop(0)
            self._handle(ev)

    def _handle(self, ev):
        recap_id, chunk = ev["recap_id"], ev["chunk_index"]
        self.recap_user[recap_id] = ev["user_id"]
        ok = self.sim.net.send("download_chunks->socrates_gw",
                               lambda: self.sim.socrates_gw.fetch(recap_id, chunk),
                               self.sim.engine.rng)
        if not ok:
            self.metrics["socrates_gw_failures"] += 1
            self.log("socrates_gw_unreachable", {"recap_id": recap_id})
            # retry later (rate-limit aware: back off)
            self.schedule(self.sim.engine.rng.uniform(2.0, 6.0),
                          lambda: self._handle(ev))
            return
        self.downloaded[recap_id].add(chunk)
        self.metrics["chunks_downloaded"] += 1
        if len(self.downloaded[recap_id]) >= self.sim.chunks_per_recap:
            self._publish(recap_id)

    def _publish(self, recap_id):
        user_id = self.recap_user.get(recap_id, -1)
        job = {"recap_id": recap_id, "user_id": user_id}
        # Workflow step 5: distribute recap JSON events round-robin
        self.sim._rr = getattr(self.sim, "_rr", 0) + 1
        q = self.sim.queue1 if self.sim._rr % 2 == 1 else self.sim.queue2
        q.send(job, source="download_chunks")
        self.metrics["recap_jobs_published"] += 1
        self.log("recap_job_published",
                 {"recap_id": recap_id, "queue": q.name})


class IdGatekeeper(Component):
    """Dedup gate inside the Monolith. TTL cache: this expiry is the hook for
    the double-processing bug scenario."""

    def __init__(self, sim, ttl=300.0):
        super().__init__(sim, "id_gatekeeper")
        self.ttl = ttl
        self.cache = {}  # key -> expiry sim-time

    def check_and_put(self, key):
        """Returns True on cache HIT (already seen recently)."""
        now = self.sim.engine.now
        exp = self.cache.get(key)
        if exp is not None and exp > now:
            self.metrics["hits"] += 1
            return True
        if exp is not None:
            self.metrics["expired"] += 1
            self.log("cache_entry_expired", {"key": key})
        else:
            self.metrics["misses"] += 1
        self.cache[key] = now + self.ttl
        return False


class SocratesGw(Component):
    def __init__(self, sim):
        super().__init__(sim, "socrates_gw")
        self.metrics["requests"] = 0

    def start_query(self, recap_id, n):
        self.metrics["queries_started"] += 1

    def fetch(self, recap_id, chunk):
        # Query access layer: retrieves large prepared data sets (chunks of
        # scratch.recap_reports) into the microservice.
        self.metrics["chunk_fetches"] += 1


class RecapReportsProcessor(Component):
    """Flows 6-10: one worker lambda inside a RecapReportsProcessor group.
    Consumes recap jobs from its SQS queue, checks the Id-gatekeeper, writes
    the recap report to Postgres, notifies via the Monolith (CC-notifications),
    and emits an analytic event. Any failure leaves the message undeleted so
    SQS visibility timeout / DLQ / redrive take over."""

    def __init__(self, sim, group, worker):
        super().__init__(sim, "recap_processor.%s.%d" % (group, worker))
        self.group = group
        self.queue = sim.queue1 if group == "group1" else sim.queue2
        self.crash_after_write = False  # deterministic fault-injectable
        self._crash_budget = 0          # how many more times the fault fires
        self._crashed_until = 0.0
        self.hazard_pre_insert = 0.0    # per-message crash prob: after ADF
                                        # write, BEFORE processed-record insert
        self.hazard_post_insert = 0.0   # per-message crash prob: after the
                                        # processed insert, before notify
        self.hazard_after_notify = 0.0  # per-message crash prob: after notify,
                                        # BEFORE the SQS delete

    def start(self):
        self._poll()

    def _poll(self):
        if not self.alive:
            return
        self.schedule(self.sim.engine.rng.uniform(0.2, 0.9), self._poll)
        if self.sim.engine.now < self._crashed_until:
            return
        for m in self.queue.receive(max_messages=1):
            self._process(m)

    def _process(self, m):
        job = m.body
        recap_id = job["recap_id"]
        self.metrics["jobs_received"] += 1
        # Id-gatekeeper via the Monolith (flow 8). Network failure => no
        # delete => visibility timeout => retry / DLQ.
        ok = self.sim.net.send(
            "processor->monolith",
            lambda: self._idempotency_checked(m, recap_id),
            self.sim.engine.rng)
        if not ok:
            self.metrics["monolith_unreachable"] += 1
            self.log("monolith_call_failed", {"recap_id": recap_id})
            return  # leave in flight; visibility timeout will redeliver

    def _idempotency_checked(self, m, recap_id):
        if not self.alive:
            return
        # Workflow step 7: idempotency check against PostgreSQL - was this
        # user's recap already processed? Honors replica lag: a row is only
        # visible after pg_lag seconds, so a redriven message can be checked
        # against a stale view.
        now = self.sim.engine.now
        t = self.sim.postgres.table("processed_recaps")
        seen = [r for r in t.select(recap_id=recap_id)
                if r.get("committed_at", 0.0) <= now - self.sim.pg_lag]
        if seen:
            self.metrics["idempotency_hits"] += 1
            self.log("idempotency_hit", {"recap_id": recap_id})
            self.queue.delete(m)
            return
        self.metrics["idempotency_misses"] += 1
        # Workflow step 8: generate the ADF report and save it to the user's
        # personal space (simulated as a Postgres row).
        self.sim.postgres.table("adf_reports").insert(
            {"recap_id": recap_id, "user_id": m.body["user_id"],
             "group": self.group, "written_at": now,
             "redriven": m.body.get("redriven", 0)})
        self.metrics["adf_reports_written"] += 1
        self.log("adf_report_written", {"recap_id": recap_id, "msg": m.id})
        if self.hazard_pre_insert > 0 and                 self.sim.engine.rng.random() < self.hazard_pre_insert:
            # Stochastic hazard: crash inside the unprotected window between
            # the ADF write (step 8) and the processed-record insert (step 10).
            self.metrics["hazard_crashes_pre_insert"] += 1
            self.log("HAZARD_CRASH_PRE_INSERT", {"recap_id": recap_id})
            self._crashed_until = self.sim.engine.now + 15.0
            return
        if self.crash_after_write and self._crash_budget > 0:
            # Injected fault: lambda 'crashes' after the ADF report write
            # but BEFORE the processed-record insert and the SQS delete. The
            # message reappears after visibility timeout; the idempotency
            # check then finds no processed record -> duplicate ADF report.
            self.metrics["injected_crashes"] += 1
            self.log("WORKER_CRASH_AFTER_WRITE", {"recap_id": recap_id,
                                                  "msg": m.id})
            self._crash_budget -= 1
            if self._crash_budget <= 0:
                self.crash_after_write = False
            self._crashed_until = self.sim.engine.now + 15.0
            return
        self._finish(m, recap_id)

    def _finish(self, m, recap_id):
        # Workflow step 10: insert the successfully processed record (this is
        # what makes future idempotency checks hit).
        self.sim.postgres.table("processed_recaps").insert(
            {"recap_id": recap_id, "user_id": m.body["user_id"],
             "committed_at": self.sim.engine.now})
        # Workflow step 9: notify the user via the Monolith (CC-notifications).
        # The SQS delete happens only after the notification is delivered, so
        # a crash between the processed-record insert (step 10) and the delete
        # leaves the message in flight for visibility-timeout redelivery.
        ok = self.sim.net.send(
            "monolith->cc_notifications",
            lambda: self._notified(m, recap_id),
            self.sim.engine.rng)
        if not ok:
            # Streamhub failure path (S1): park it for the Streamhub Replayer
            self.sim.streamhub_failure.send(
                {"recap_id": recap_id, "user_id": m.body["user_id"],
                 "stage": "cc_notification"}, source=self.name)
            self.metrics["streamhub_failures"] += 1
            self.log("cc_notification_failed_streamhub", {"recap_id": recap_id})
            self.queue.delete(m)

    def _notified(self, m, recap_id):
        if self.hazard_post_insert > 0 and                 self.sim.engine.rng.random() < self.hazard_post_insert:
            # Stochastic hazard: crash AFTER the processed-record insert and
            # notify but BEFORE the SQS delete. Whether this duplicates
            # depends on replica lag vs the visibility-timeout redelivery.
            self.metrics["hazard_crashes_post_insert"] += 1
            self.log("HAZARD_CRASH_POST_INSERT", {"recap_id": recap_id})
            self._crashed_until = self.sim.engine.now + 15.0
            return
        self.sim.postgres.table("cc_notifications").insert(
            {"recap_id": recap_id, "user_id": m.body["user_id"],
             "sent_at": self.sim.engine.now})
        self.metrics["cc_notifications"] += 1
        if self.hazard_after_notify > 0 and                 self.sim.engine.rng.random() < self.hazard_after_notify:
            # Stochastic hazard: crash AFTER the user was notified but BEFORE
            # the SQS delete -> redelivery can duplicate BOTH the ADF report
            # and the notification (when replica lag hides the processed row).
            self.metrics["hazard_crashes_after_notify"] += 1
            self.log("HAZARD_CRASH_AFTER_NOTIFY", {"recap_id": recap_id})
            self._crashed_until = self.sim.engine.now + 15.0
            return
        # Analytic event (workflow step 11) and only now the SQS delete
        self.sim.analytics_queue.send(
            {"recap_id": recap_id, "user_id": m.body["user_id"], "group": self.group},
            source=self.name)
        self.metrics["analytic_events_emitted"] += 1
        self.queue.delete(m)
        self.log("job_completed", {"recap_id": recap_id, "msg": m.id})


class AnalyticsProcessor(Component):
    """Flows 11,12: consume Analytic Events SQS, write to cloud.event_track."""

    def __init__(self, sim, name="analytics_processor"):
        super().__init__(sim, name)
        self.event_track = sim.postgres.table("cloud.event_track")

    def start(self):
        self._poll()

    def _poll(self):
        if not self.alive:
            return
        self.schedule(self.sim.engine.rng.uniform(0.3, 1.0), self._poll)
        for m in self.sim.analytics_queue.receive(max_messages=5):
            self.event_track.insert({"kind": "recap_analytic",
                                     **m.body})
            self.metrics["analytic_events_processed"] += 1
            self.sim.analytics_queue.delete(m)


class StreamhubReplayer(Component):
    """S2/S3: retry Streamhub failures; on final failure notify CC anyway."""

    def __init__(self, sim):
        super().__init__(sim, "streamhub_replayer")

    def start(self):
        self._poll()

    def _poll(self):
        if not self.alive:
            return
        self.schedule(self.sim.engine.rng.uniform(1.0, 3.0), self._poll)
        for m in self.sim.streamhub_failure.receive(max_messages=10):
            ok = self.sim.net.send(
                "monolith->cc_notifications",
                lambda mm=m: self._deliver(mm),
                self.sim.engine.rng)
            if ok:
                self.sim.streamhub_failure.delete(m)
            else:
                self.metrics["retries"] += 1

    def _deliver(self, m):
        self.sim.postgres.table("cc_notifications").insert(
            {"recap_id": m.body["recap_id"], "user_id": m.body["user_id"],
             "sent_at": self.sim.engine.now, "via": "streamhub_replayer"})
        self.metrics["cc_notifications"] += 1


class RecapReplayer(Component):
    """R1/R2: periodically redrive DLQ contents back onto the main queue."""

    def __init__(self, sim, queue, name):
        super().__init__(sim, name)
        self.queue = queue

    def start(self):
        self._tick()

    def _tick(self):
        if not self.alive:
            return
        self.schedule(self.sim.engine.rng.uniform(5.0, 15.0), self._tick)
        moved = self.queue.redrive_from_dlq(max_messages=50)
        if moved:
            self.metrics["messages_redriven"] += moved
