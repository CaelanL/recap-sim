// UI for the SIMULATION. Polls /state and updates the SVG.
var selected = null, lastLogT = 0, logLines = [];

function $(id) { return document.getElementById(id); }

function resetSim() {
  fetch("/control", {method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({action: "reset",
      scenario: $("scenario").value, seed: parseInt($("seed").value || "42")})})
    .then(r => r.json()).then(s => { lastLogT = 0; logLines = []; $("log").innerHTML = ""; });
}
function togglePause() {
  var paused = $("pp").textContent === "resume";
  fetch("/control", {method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({action: paused ? "resume" : "pause"})});
  $("pp").textContent = paused ? "pause" : "resume";
}
function setSpeed(v) { $("spd").textContent = v;
  fetch("/control", {method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({action: "speed", speed: parseFloat(v)})}); }

function q(s, name) { return (s.queues || []).find(q => q.name === name) || {}; }
function comp(s, prefix) { return (s.components || []).filter(c => c.name.indexOf(prefix) === 0); }
function sum(list, key) { return list.reduce((a, c) => a + (c.metrics[key] || 0), 0); }

function tick() {
  fetch("/state").then(r => r.json()).then(s => {
    if (!s.running) return;
    $("clock").textContent = "sim t=" + (s.now / 60).toFixed(1) + " min" +
      (s.paused ? " (paused)" : "") + (s.done ? " (done)" : "");
    $("c-spark").textContent = "recaps: " + sum(comp(s, "db_spark_jobs"), "recaps_emitted");
    $("c-socrates").textContent = "reqs: " + sum(comp(s, "socrates_gw"), "queries_started") + sum(comp(s, "socrates_gw"), "chunk_fetches");
    $("c-executor").textContent = "chunks: " + sum(comp(s, "statement_executor"), "chunk_events_emitted");
    $("c-chunks").textContent = "pending: " + sum(comp(s, "statement_executor"), "chunk_events_emitted");
    $("c-download").textContent = "downloaded: " + sum(comp(s, "download_chunks"), "chunks_downloaded");
    $("c-download2").textContent = "published: " + sum(comp(s, "download_chunks"), "recap_jobs_published");
    var q1 = q(s, "sqs_queue_1"), q2 = q(s, "sqs_queue_2");
    $("c-q1").textContent = "depth " + q1.depth + " / fly " + q1.in_flight;
    $("c-q1b").textContent = "to_dlq " + (q1.to_dlq || 0);
    $("c-q2").textContent = "depth " + q2.depth + " / fly " + q2.in_flight;
    $("c-q2b").textContent = "to_dlq " + (q2.to_dlq || 0);
    $("c-dlq1").textContent = "depth " + (q1.dlq_depth || 0);
    $("c-dlq2").textContent = "depth " + (q2.dlq_depth || 0);
    $("c-rep1").textContent = "redriven " + sum(comp(s, "recap_replayer_1"), "messages_redriven");
    $("c-rep2").textContent = "redriven " + sum(comp(s, "recap_replayer_2"), "messages_redriven");
    var g1 = comp(s, "recap_processor.group1"), g2 = comp(s, "recap_processor.group2");
    $("c-g1").textContent = "adf: " + sum(g1, "adf_reports_written");
    $("c-g1b").textContent = "idem hits: " + sum(g1, "idempotency_hits") + " crashes: " + sum(g1, "injected_crashes");
    $("c-g2").textContent = "adf: " + sum(g2, "adf_reports_written");
    $("c-g2b").textContent = "idem hits: " + sum(g2, "idempotency_hits") + " crashes: " + sum(g2, "injected_crashes");
    var pgr = (s.postgres || {})["adf_reports"] || 0;
    $("c-pg").textContent = "adf_reports: " + pgr;
    $("c-pg2").textContent = "processed: " + ((s.postgres || {})["processed_recaps"] || 0);
    var gk = ({metrics: {}});
    $("c-gk").textContent = "misses " + (gk.metrics.misses || 0) + " hits " + (gk.metrics.hits || 0);
    $("c-gk2").textContent = "EXPIRED " + (gk.metrics.expired || 0);
    $("c-cc").textContent = "cc_notifications: " + ((s.postgres || {})["cc_notifications"] || 0);
    $("c-cc2").textContent = "";
    $("c-anaq").textContent = "depth " + q(s, "analytic_events_sqs").depth;
    $("c-anal").textContent = "processed " + sum(comp(s, "analytics_processor"), "analytic_events_processed");
    $("c-shq").textContent = "depth " + q(s, "streamhub_failure_sqs").depth;
    $("c-scratch").textContent = "rows: " + ((s.postgres || {})["scratch.recap_reports"] || 0);
    $("c-eventtrack").textContent = "rows: " + ((s.postgres || {})["cloud.event_track"] || 0);
    // animate hot edges
    ["e-spark-exec","e-exec-chunks","e-chunks-dl","e-dl-q1","e-dl-q2","e-q1-g1","e-q2-g2","e-g1-pg","e-g2-pg","e-anaq-anal"].forEach(id => {});
    document.querySelectorAll(".edge").forEach(e => e.classList.remove("hot"));
    if (q1.depth + q2.depth > 0) { heat("e-q1-g1"); heat("e-q2-g2"); }
    if ((q1.dlq_depth || 0) + (q2.dlq_depth || 0) > 0) { heat("e-dlq1-q1"); heat("e-dlq2-q2"); }
    
    // log
    (s.log_tail || []).slice().reverse().forEach(e => {
      var key = e.t + "|" + e.src + "|" + e.event + "|" + JSON.stringify(e.data);
      if (!logLines.includes(key)) {
        logLines.push(key);
        var div = document.createElement("div");
        div.className = /CRASH|expired|dlq|failed/i.test(e.event) ? "ev-bad" : "";
        div.textContent = e.t.toFixed(1) + "s " + e.src + " " + e.event + " " + JSON.stringify(e.data);
        $("log").prepend(div);
      }
    });
    if (logLines.length > 300) logLines = logLines.slice(-300);
    if (selected) showDetail(selected, s);
  });
}
function heat(id) { var e = $(id); if (e) e.classList.add("hot"); }

var NODE_MAP = {
  "n-spark": s => ["DB spark jobs", comp(s, "db_spark_jobs")[0].metrics],
  "n-socrates": s => ["Socrates GW (query access layer)", comp(s, "socrates_gw")[0].metrics],
  "n-executor": s => ["Statement Executor", comp(s, "statement_executor")[0].metrics],
  "n-download": s => ["Download Chunks (5 lambdas)", comp(s, "download_chunks")[0].metrics],
  "n-q1": s => ["SQS Queue 1", q(s, "sqs_queue_1")],
  "n-q2": s => ["SQS Queue 2", q(s, "sqs_queue_2")],
  "n-dlq1": s => ["DLQ 1", {depth: q(s, "sqs_queue_1").dlq_depth}],
  "n-dlq2": s => ["DLQ 2", {depth: q(s, "sqs_queue_2").dlq_depth}],
  "n-rep1": s => ["Recap Replayer 1 (R1)", comp(s, "recap_replayer_1")[0].metrics],
  "n-rep2": s => ["Recap Replayer 2 (R2)", comp(s, "recap_replayer_2")[0].metrics],
  "n-g1": s => ["RecapReportsProcessor Group 1", agg(comp(s, "recap_processor.group1"))],
  "n-g2": s => ["RecapReportsProcessor Group 2", agg(comp(s, "recap_processor.group2"))],
  "n-pg": s => ["Postgres (simulated tables)", s.postgres],
  "n-mono1": s => ["Idempotency check (Postgres)", {processed: (s.postgres || {})["processed_recaps"] || 0, adf_reports: (s.postgres || {})["adf_reports"] || 0}],
  "n-mono2": s => ["Monolith: CC-notifications", {rows: (s.postgres || {})["cc_notifications"] || 0}],
  "n-anaq": s => ["Analytic Events SQS", q(s, "analytic_events_sqs")],
  "n-anal": s => ["Analytics processor Lambdas", agg(comp(s, "analytics_processor"))],
  "n-shq": s => ["Streamhub Failure SQS", q(s, "streamhub_failure_sqs")],
  "n-scratch": s => ["scratch.recap_reports", {rows: (s.postgres || {})["scratch.recap_reports"] || 0}],
  "n-eventtrack": s => ["cloud.event_track", {rows: (s.postgres || {})["cloud.event_track"] || 0}],
};
function agg(list) {
  var m = {};
  list.forEach(c => Object.keys(c.metrics).forEach(k => m[k] = (m[k] || 0) + c.metrics[k]));
  return m;
}
function showDetail(id, s) {
  var f = NODE_MAP[id];
  if (!f) return;
  var r = f(s);
  var rows = Object.entries(r[1]).map(e => "<tr><td>" + e[0] + "</td><td>" + e[1] + "</td></tr>").join("");
  $("selname").textContent = r[0];
  $("seldetail").innerHTML = "<table>" + rows + "</table>";
}
document.querySelectorAll(".node").forEach(n => {
  n.addEventListener("click", () => {
    document.querySelectorAll(".node rect").forEach(r => r.classList.remove("sel"));
    n.querySelector("rect").classList.add("sel");
    selected = n.id;
    fetch("/state").then(r => r.json()).then(s => showDetail(n.id, s));
  });
});
setInterval(tick, 500);
tick();
