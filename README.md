# Webhook Delivery Worker

What happens when a supplier is approved but the partner system is offline? This Python worker stores the notification in SQLite, retries transient failures, and records each attempt under a stable event ID.

## Try a failed delivery

With Python 3.11+ run:

```sh
python demo.py
python -m unittest -v
```

The demo starts a loopback HTTP receiver. Its first response is 503. About a second later it accepts the same event with 204. Both lines show the same event ID. The demo uses a temporary database and cleans up after itself.

## Delivery contract

- The operator configures one destination for the queue. Keep that destination stable across restarts. Producers supply JSON payloads.
- A 2xx response completes delivery. Network errors, HTTP 408, 429 and 5xx trigger up to two retries, after one and two seconds. Other responses end delivery immediately. Redirects are recorded as responses.
- The worker commits the attempt and the next queue state together. If the process stops after the receiver accepts a request but before that commit, the same event can arrive again. Receivers can use the stable `Webhook-Id` to deduplicate.
- Run one worker per database. The restart test reopens a file-backed database between attempts and verifies the receiver sees the same payload and ID.

## Seven-day scope

1. Durable single-worker delivery with a real failure and retry demo.
2. Event submission and delivery-history API with producer idempotency.
3. PostgreSQL queue with worker leases and crash recovery.
4. Signed deliveries and receiver-side verification.
5. Failed-delivery inspection and controlled replay.
6. Delivery metrics and a reproducible backlog experiment.
7. Compose quickstart and a failure-recovery walkthrough.
