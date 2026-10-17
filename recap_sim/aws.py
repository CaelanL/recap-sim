"""Faithful-enough fake SQS: visibility timeout, receive counts,
maxReceiveCount -> DLQ, and DLQ redrive. SIMULATED - no real AWS calls."""
import itertools


class Message:

    def __init__(self, body, source=""):
        self.body = body
        self.source = source
        self.receive_count = 0
        self.visible_at = 0.0     # sim-time when it becomes visible again
        self.in_flight = False
        self.first_sent_at = None
        self.dlq_time = None

    def to_dict(self):
        return {"id": self.id, "receive_count": self.receive_count,
                "in_flight": self.in_flight, "body": self.body}


class FakeQueue:
    """One SQS queue + optional DLQ + redrive policy."""

    def __init__(self, sim, name, visibility_timeout=30.0,
                 max_receive_count=5, dlq=None):
        self.sim = sim
        self.name = name
        self.visibility_timeout = visibility_timeout
        self.max_receive_count = max_receive_count
        self.dlq = dlq
        self.messages = []        # pending (visible or in-flight)
        self.total_sent = 0
        self.total_received = 0
        self.total_deleted = 0
        self.to_dlq_count = 0
        self.redriven_in = 0

    def send(self, body, source=""):
        m = Message(body, source)
        m.id = "m%d" % next(self.sim._msg_counter)
        m.first_sent_at = self.sim.engine.now
        self.messages.append(m)
        self.total_sent += 1
        return m

    def receive(self, max_messages=10):
        """Poll for visible messages. Applies visibility timeout and
        maxReceiveCount -> DLQ, mirroring SQS semantics."""
        now = self.sim.engine.now
        out = []
        for m in list(self.messages):
            if len(out) >= max_messages:
                break
            if m.in_flight or m.visible_at > now:
                continue
            m.receive_count += 1
            if self.dlq is not None and m.receive_count > self.max_receive_count:
                self.messages.remove(m)
                m.dlq_time = now
                self.dlq.send(m.body, source="redrive-from:%s" % self.name)
                self.to_dlq_count += 1
                self.sim.log.add(now, self.name, "message_to_dlq",
                                 {"msg": m.id, "body": m.body})
                continue
            m.in_flight = True
            m.visible_at = now + self.visibility_timeout
            self._schedule_visibility_restore(m)
            self.total_received += 1
            out.append(m)
        return out

    def _schedule_visibility_restore(self, m):
        vt = self.visibility_timeout
        def restore():
            # If still in flight and its visibility window elapsed, make it
            # visible again (SQS: message reappears after visibility timeout).
            if m.in_flight and m.visible_at <= self.sim.engine.now + 1e-9:
                m.in_flight = False
                m.visible_at = 0.0
                self.sim.log.add(self.sim.engine.now, self.name,
                                 "visibility_timeout_expired", {"msg": m.id})
        self.sim.engine.schedule(vt, restore)

    def delete(self, m):
        if m in self.messages:
            self.messages.remove(m)
            self.total_deleted += 1

    def redrive_from_dlq(self, max_messages=50):
        """Recap Replayer: move messages from this queue's DLQ back onto the
        main queue (receive count resets, like an SQS redrive)."""
        if self.dlq is None:
            return 0
        moved = 0
        for m in list(self.dlq.messages):
            if moved >= max_messages:
                break
            self.dlq.messages.remove(m)
            body = m.body
            body = dict(body)
            body["redriven"] = body.get("redriven", 0) + 1
            self.send(body, source="redrive")
            moved += 1
            self.redriven_in += 1
        if moved:
            self.sim.log.add(self.sim.engine.now, self.name, "dlq_redrive",
                             {"moved": moved})
        return moved

    @property
    def depth(self):
        return sum(1 for m in self.messages if not m.in_flight)

    @property
    def in_flight(self):
        return sum(1 for m in self.messages if m.in_flight)

    def stats(self):
        d = {"name": self.name, "depth": self.depth, "in_flight": self.in_flight,
             "total_sent": self.total_sent, "total_deleted": self.total_deleted,
             "to_dlq": self.to_dlq_count, "redriven_in": self.redriven_in}
        if self.dlq is not None:
            d["dlq_depth"] = self.dlq.depth
        return d
