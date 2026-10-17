"""In-memory 'Postgres' and source tables. SIMULATED storage."""
import collections


class Table:
    def __init__(self, name):
        self.name = name
        self.rows = []
        self.next_id = 1

    def insert(self, row):
        row = dict(row)
        row.setdefault("id", self.next_id)
        if row["id"] >= self.next_id:
            self.next_id = row["id"] + 1
        self.rows.append(row)
        return row

    def select(self, **eq):
        return [r for r in self.rows
                if all(k in r and r[k] == v for k, v in eq.items())]

    def count(self, **eq):
        return len(self.select(**eq))


class Postgres:
    """Fake Postgres instance shared by both RecapReportsProcessor groups."""
    def __init__(self):
        self.tables = collections.OrderedDict()

    def table(self, name):
        if name not in self.tables:
            self.tables[name] = Table(name)
        return self.tables[name]

    def stats(self):
        return {n: len(t.rows) for n, t in self.tables.items()}
