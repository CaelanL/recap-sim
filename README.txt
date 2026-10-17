CONFLUENCE RECAP PIPELINE - DETERMINISTIC SIMULATION
====================================================

Everything in this project is a SIMULATION of the real Confluence Recap
system ("Spotify Wrapped for Confluence"). It runs single-process in the
shared Linux sandbox, uses synthetic data, and makes no connection to real
AWS, SQS, Postgres, or Confluence. The failure scenario is a PLAUSIBLE
RECONSTRUCTION, not a claimed real incident.

Real-system context (from Caelan): 8.5M users, 140k tenants, ~8,000 recaps
per 5 minutes, item-level dead-letter recovery, rate-limit aware, built as a
microservice to keep traffic off the backend monolith. The simulation
compresses scale ~20:1 (2,000 synthetic users, 140 tenants standing in for
140k, 400 recaps per 5 sim-minutes); mechanics and failure modes, not
absolute volume, are the point.

WORKFLOW MODELED (Caelan's authoritative 13-step description)
  1  Execute a query in Socrates (initiated from the data bot result);
     aggregated data downloads into the microservice.
  2  On success, enqueue one small POINTER message per chunk index on the
     Statement Chunk events queue (pointers, not recap data).
  3  Download Chunks lambdas receive chunk download events.
  4  Retrieve recap reports data from Socrates GW (query access layer for
     large prepared data sets; scratch.recap_reports is the prepared data
     set table).
  5  Build recap JSON events and distribute to SQS 1 / SQS 2 round-robin.
  6  RecapReportsProcessor groups batch-pull recap events.
  7  Idempotency check against PostgreSQL (was this user already processed?).
  8  Generate an ADF report per item and save it to the user's personal
     space (simulated as Postgres rows).
  9  Notify the user (CC-notifications via the Monolith; failures go through
     the Streamhub Failure SQS + Streamhub Replayer).
  10 Insert successfully processed records (the idempotency source of truth).
  11 Send track events to the Analytic Events SQS.
  12 Analytics processor lambdas pull track events.
  13 Send events via the analytics client into cloud.event_track.

Also modeled: SQS visibility timeout, maxReceiveCount -> DLQ, and the Recap
Replayers (R1/R2) that redrive DLQ contents back onto the main queues.

RUNNING IT (from /home/user/recap-sim)
  Headless baseline (no faults, all invariants pass):
      python3 -m recap_sim run --scenario baseline
  Seeded replay of the injected bug (runs twice, compares digests):
      python3 -m recap_sim replay --scenario bug --seed 1337
  Chaos experiment / invariant verification (agent-facing hook):
      python3 -m recap_sim experiment experiments/double_process_redrive.json
      python3 -m recap_sim experiment experiments/replica_lag_only.json
  Animated UI (SVG pipeline, clickable nodes, live queue/DLQ counts,
  event log, scenario/seed/speed controls):
      python3 -m recap_sim serve --port 8710
      then open http://localhost:8710

THE INJECTED FAILURE (Act 2, SIMULATED reconstruction)
  Config: seed 1337, faults = worker_crash_after_write (group1, 2x),
  pg_replica_lag 90s, processor->monolith partition at sim-t 240-420s.
  Mechanism: a processor worker crashes AFTER writing the ADF report but
  BEFORE inserting the processed record (step 10) and deleting the SQS
  message. The message reappears via visibility timeout; a Monolith outage
  pushes other attempts through maxReceiveCount into the DLQ; the Recap
  Replayer redrives them; the Postgres idempotency check (step 7) finds no
  processed record - the insert never happened, and replica lag can also
  hide a just-inserted record from a redriven check. The reprocess writes a
  DUPLICATE ADF report. Observed on seed 1337: 8 duplicate ADF report rows
  (e.g. recap-000000 x2), 76 messages to DLQ, all redriven, digest identical
  across repeated runs. In this seed the first attempt crashed before
  notifying, so duplicate CC-notifications did not materialize; a crash
  after step 9 would duplicate them too (try raising "times").

DETERMINISM
  All randomness flows from one seeded PRNG; event order is (time, seq).
  The same seed + config reproduces the same event log bit-for-bit; the
  SHA-256 digest of the log is printed by every run and compared by
  "replay".

EXTENDING / CORRECTING THE MODEL
  Each diagram box is its own class in recap_sim/components.py with small
  methods; cross-box traffic goes through Network.send (latency, drops,
  partitions) or FakeQueue (recap_sim/aws.py). Storage is in-memory tables
  (recap_sim/store.py). Fault vocabulary lives in Simulation._apply_faults
  (recap_sim/sim.py): pg_replica_lag, worker_crash_after_write, partition,
  drop_prob. New invariants go in check_invariants(). Socrates GW is
  currently a thin stub (queries_started / chunk_fetches counters) because
  its exact role is still being confirmed - correct it in one place.

FILES
  recap_sim/engine.py    discrete-event engine, seeded PRNG, network, log
  recap_sim/aws.py       fake SQS: visibility timeout, DLQ, redrive
  recap_sim/store.py     in-memory Postgres-style tables
  recap_sim/components.py one actor class per diagram box
  recap_sim/sim.py       orchestrator, scenarios, faults, invariants
  recap_sim/__main__.py  CLI (run / replay / experiment / serve)
  recap_sim/server.py    UI server (stdlib http.server, no dependencies)
  recap_sim/static/      index.html + app.js (SVG pipeline, polling UI)
  experiments/           example chaos-experiment specs (JSON)
