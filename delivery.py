"""Deliver an event to a configured receiver and record each HTTP attempt."""
import http.client
import json
import sqlite3
import time
from urllib.parse import urlsplit
from uuid import uuid4


class Worker:
    """Single-worker SQLite queue. A receiver can deduplicate by Webhook-Id."""

    def __init__(self, db, destination):
        self.db = db
        self.destination = urlsplit(destination)
        if self.destination.scheme not in {"http", "https"} or not self.destination.hostname:
            raise ValueError("Expected an HTTP(S) receiver URL")
        db.row_factory = sqlite3.Row
        db.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, payload TEXT NOT NULL, due REAL NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending'
            );
            CREATE TABLE IF NOT EXISTS attempts (
                event TEXT NOT NULL REFERENCES events(id), number INTEGER NOT NULL,
                started REAL NOT NULL, status INTEGER, error TEXT,
                PRIMARY KEY (event, number)
            );
        """)

    def enqueue(self, payload):
        event_id = str(uuid4())
        with self.db:
            self.db.execute("INSERT INTO events(id,payload,due) VALUES (?,?,?)",
                            (event_id, json.dumps(payload), time.time()))
        return event_id

    def history(self, event_id):
        return [dict(row) for row in self.db.execute(
            "SELECT number, started, status, error FROM attempts WHERE event=? ORDER BY number",
            (event_id,))]

    def run_one(self, *, now=None):
        """Process one due event. Return its ID, or None when the queue is idle."""
        now = time.time() if now is None else now
        event = self.db.execute(
            "SELECT * FROM events WHERE state='pending' AND due<=? ORDER BY due,id LIMIT 1",
            (now,)).fetchone()
        if event is None:
            return None
        url = self.destination
        transport = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
        connection = transport(url.hostname, url.port, timeout=3)
        status = error = None
        try:
            path = (url.path or "/") + ("?" + url.query if url.query else "")
            connection.request("POST", path, body=event["payload"].encode(),
                               headers={"Content-Type": "application/json", "Webhook-Id": event["id"]})
            status = connection.getresponse().status
        except (OSError, http.client.HTTPException) as exc:
            error = type(exc).__name__
        finally:
            connection.close()

        number = len(self.history(event["id"])) + 1
        retryable = status is None or status in (408, 429) or 500 <= status < 600
        state = "failed"
        if status is not None and 200 <= status < 300:
            state = "delivered"
        elif retryable and number < 3:
            state = "pending"
        with self.db:
            self.db.execute("INSERT INTO attempts VALUES (?,?,?,?,?)",
                            (event["id"], number, now, status, error))
            self.db.execute("UPDATE events SET state=?, due=? WHERE id=?",
                            (state, max(time.time(), now) + 2 ** (number - 1), event["id"]))
        return event["id"]
