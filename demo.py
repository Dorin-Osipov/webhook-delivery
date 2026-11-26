"""Run a real local receiver that fails once, then accepts the same event."""
import json
import sqlite3
import tempfile
import time
from contextlib import closing, contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread

from delivery import Worker


@contextmanager
def receiver(statuses):
    received = []
    responses = iter(statuses)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append((self.headers["Webhook-Id"],
                             json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            self.send_response(next(responses, 500))
            self.end_headers()

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/events", received
        finally:
            server.shutdown()
            thread.join()


def main():
    with tempfile.TemporaryDirectory() as tmp, receiver([503, 204]) as (url, received):
        with closing(sqlite3.connect(Path(tmp) / "queue.db")) as db:
            worker = Worker(db, url)
            event = worker.enqueue({"type": "supplier.approved", "supplier_id": "S-104"})
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if worker.run_one():
                    attempt = worker.history(event)[-1]
                    print(f"Attempt {attempt['number']} for {event}: HTTP {attempt['status']}")
                    if attempt["status"] == 204:
                        break
                time.sleep(0.05)
            if [a["status"] for a in worker.history(event)] != [503, 204] or received[0] != received[1]:
                raise RuntimeError("Expected a retry of the same event followed by delivery")


if __name__ == "__main__":
    main()
