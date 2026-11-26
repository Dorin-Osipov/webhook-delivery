import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path

from delivery import Worker
from demo import receiver


class DeliveryTests(unittest.TestCase):
    def test_retry_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp, receiver([503, 204]) as (url, received):
            path = Path(tmp) / "queue.db"
            with closing(sqlite3.connect(path)) as db:
                worker = Worker(db, url)
                event = worker.enqueue({"type": "approved"})
                now = time.time()
                self.assertEqual(worker.run_one(now=now), event)
                self.assertIsNone(worker.run_one(now=now))
            with closing(sqlite3.connect(path)) as db:
                worker = Worker(db, url)
                self.assertEqual(worker.run_one(now=now + 10), event)
                self.assertIsNone(worker.run_one(now=now + 20))
                self.assertEqual([a["status"] for a in worker.history(event)], [503, 204])
                self.assertEqual(received, [(event, {"type": "approved"})] * 2)

    def test_terminal_outcomes(self):
        for statuses in ([400], [307], [429, 503, 500]):
            with self.subTest(statuses=statuses), receiver(statuses) as (url, received):
                with closing(sqlite3.connect(":memory:")) as db:
                    worker = Worker(db, url)
                    event = worker.enqueue({})
                    now = time.time()
                    for i in range(4):
                        worker.run_one(now=now + 10 * i)
                    self.assertEqual([a["status"] for a in worker.history(event)], statuses)
                    self.assertEqual(db.execute("SELECT state FROM events").fetchone()[0], "failed")
