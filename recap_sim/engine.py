"""Discrete-event simulation engine with injectable Clock / Network / Storage.

Deterministic: all randomness flows from one seeded random.Random, and event
ordering is (time, sequence-number), so the same seed + config reproduces the
same run bit-for-bit (see sim.digest()).
"""
import heapq, itertools, random, hashlib, json


class Network:
    """Injectable network: per-edge latency, drop probability, partitions."""

    def __init__(self, engine, default_latency=(0.05, 0.4), drop_prob=0.0):
        self.engine = engine
        self.default_latency = default_latency
        self.drop_prob = drop_prob
        self.edges = {}          # edge name -> dict(latency=(lo,hi), drop_prob=p)
        self.partitions = []     # list of (edge, start, end) sim-time windows

    def configure_edge(self, name, latency=None, drop_prob=None):
        e = self.edges.setdefault(name, {})
        if latency is not None:
            e["latency"] = latency
        if drop_prob is not None:
            e["drop_prob"] = drop_prob

    def add_partition(self, edge, start, end):
        self.partitions.append((edge, start, end))

    def _blocked(self, edge):
        t = self.engine.now
        return any(e == edge and s <= t < end for e, s, end in self.partitions)

    def send(self, edge, deliver_fn, rng=None):
        """Attempt delivery over an edge. Returns True if it will be delivered
        (after latency), False if dropped or partitioned. Deterministic."""
        rng = rng or self.engine.rng
        if self._blocked(edge):
            return False
        cfg = self.edges.get(edge, {})
        lo, hi = cfg.get("latency", self.default_latency)
        p = cfg.get("drop_prob", self.drop_prob)
        if rng.random() < p:
            return False
        delay = rng.uniform(lo, hi)
        self.engine.schedule(delay, deliver_fn)
        return True


class Engine:
    """Virtual-clock discrete event engine. In headless mode time is pure
    sim-time (no real sleeping); the UI thread steps the same engine."""

    def __init__(self, seed):
        self.now = 0.0
        self._queue = []
        self._seq = itertools.count()
        self.rng = random.Random(seed)
        self.events_fired = 0

    def schedule(self, delay, fn):
        if delay < 0:
            delay = 0.0
        heapq.heappush(self._queue, (self.now + delay, next(self._seq), fn))

    def cancel_all(self):
        self._queue.clear()

    def step(self):
        """Fire one event. Returns False when the queue is empty."""
        if not self._queue:
            return False
        t, _seq, fn = heapq.heappop(self._queue)
        self.now = t
        self.events_fired += 1
        fn()
        return True

    def run(self, until=None, max_events=None):
        fired = 0
        while self._queue:
            if until is not None and self._queue[0][0] > until:
                break
            if max_events is not None and fired >= max_events:
                break
            self.step()
            fired += 1
        return fired

    def digest(self, log_lines):
        h = hashlib.sha256()
        for line in log_lines:
            h.update(line.encode("utf-8"))
            h.update(b"\n")
        return h.hexdigest()


class Log:
    """Structured event log; the digest of (time, source, event, data) lines
    is the determinism fingerprint of a run."""

    def __init__(self, cap=200000):
        self.entries = []
        self.cap = cap

    def add(self, time, source, event, data=None):
        if len(self.entries) < self.cap:
            self.entries.append((round(time, 3), source, event, data))

    def lines(self):
        return [json.dumps(e, sort_keys=True, default=str) for e in self.entries]

    def tail(self, n=50):
        return self.entries[-n:]
