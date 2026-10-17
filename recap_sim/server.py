"""Animated walkthrough UI for the SIMULATION. Serves a single page with an
SVG of the pipeline; the page polls /state for live queue depths, DLQ counts,
component metrics and the event log. No external dependencies."""
import json, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .sim import Simulation, SCENARIOS

STATIC = os.path.join(os.path.dirname(__file__), "static")


class Runner(threading.Thread):
    """Steps the simulation engine in real time: speed = sim-seconds per
    real second (e.g. 30 => 1 real second advances 30 sim seconds)."""

    def __init__(self):
        super().__init__(daemon=True)
        self.lock = threading.Lock()
        self.sim = None
        self.speed = 30.0
        self.paused = True
        self.done = False

    def reset(self, scenario="baseline", seed=42):
        with self.lock:
            cfg = SCENARIOS[scenario](seed=seed)
            self.sim = Simulation(cfg)
            self.sim.start()
            self.paused = False
            self.done = False

    def run(self):
        last = time.time()
        while True:
            time.sleep(0.05)
            with self.lock:
                if self.sim is None or self.paused:
                    last = time.time()
                    continue
                now = time.time()
                budget = (now - last) * self.speed
                last = now
                target = self.sim.engine.now + budget
                fired = 0
                while self.sim.engine.now < target and fired < 20000:
                    if not self.sim.engine.step():
                        self.done = True
                        break
                    fired += 1

    def state(self):
        with self.lock:
            if self.sim is None:
                return {"running": False}
            s = self.sim.snapshot()
            s["paused"] = self.paused
            s["done"] = self.done
            s["speed"] = self.speed
            s["scenario"] = self.sim.cfg.get("faults") and "bug" or "baseline"
            s["seed"] = self.sim.cfg["seed"]
            s["running"] = True
            return s


runner = Runner()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            f = open(os.path.join(STATIC, "index.html"), "rb")
            body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/state":
            self._json(runner.state())
        else:
            path = os.path.join(STATIC, os.path.basename(self.path))
            if self.path.startswith('/static/') and os.path.isfile(path):
                body = open(path, 'rb').read()
                ctype = 'text/javascript' if path.endswith('.js') else 'text/html'
                self.send_response(200)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        req = json.loads(self.rfile.read(n) or b"{}")
        action = req.get("action")
        if action == "reset":
            runner.reset(req.get("scenario", "baseline"),
                         int(req.get("seed", 42)))
        elif action == "pause":
            runner.paused = True
        elif action == "resume":
            runner.paused = False
        elif action == "speed":
            runner.speed = float(req.get("speed", 30))
        self._json(runner.state())


def serve(port=8710):
    runner.reset("baseline", seed=42)
    runner.start()
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print("SIMULATION UI on http://localhost:%d  (Ctrl-C to stop)" % port)
    httpd.serve_forever()
