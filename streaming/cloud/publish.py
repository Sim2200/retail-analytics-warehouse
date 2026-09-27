"""Replay orders into the Pub/Sub `orders` topic: the Kafka producer's twin.

Same JSON payload as streaming/producer.py; `event_time` is also set as a message
attribute (RFC 3339) because Beam's Pub/Sub source takes element timestamps from
an attribute. After the replay, a 1 event/s tail keeps the watermark moving so
the last full window closes. Writes results/pubsub_publish.json.

    python streaming/cloud/publish.py --project <id> --rate 2000 --seconds 165 --tail-seconds 90
"""

from __future__ import annotations

import argparse
import json
import time
from concurrent import futures
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import pubsub_v1


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ts * 1000) % 1000:03d}Z"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--replay", default="data/replay_orders.parquet")
    ap.add_argument("--rate", type=float, default=2000)
    ap.add_argument("--seconds", type=float, default=165)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tail-seconds", type=float, default=90)
    a = ap.parse_args()

    table = pq.read_table(a.replay, columns=["order_id", "customer_id", "order_total", "item_count", "status"])
    if a.limit:
        table = table.slice(0, a.limit)
    rows = zip(*(table.column(c).to_pylist() for c in ("order_id", "customer_id", "order_total", "item_count", "status")))

    publisher = pubsub_v1.PublisherClient(
        batch_settings=pubsub_v1.types.BatchSettings(max_messages=500, max_bytes=1_000_000, max_latency=0.05))
    topic = publisher.topic_path(a.project, a.topic)
    pending: list[futures.Future] = []
    start = time.time()
    sent = 0
    for order_id, customer_id, total, items, status in rows:
        now = time.time()
        if now - start > a.seconds:
            break
        if a.rate > 0:
            due = start + sent / a.rate
            if due > now:
                time.sleep(due - now)
                now = time.time()
        stamp = iso(now)
        payload = json.dumps({"order_id": order_id, "customer_id": customer_id, "order_total": float(total),
                              "item_count": int(items), "status": status, "event_time": stamp,
                              "produced_at": stamp}).encode()
        pending.append(publisher.publish(topic, payload, event_time=stamp))
        sent += 1
        if sent % 50_000 == 0:
            futures.wait(pending)
            pending.clear()
            print(f"  {sent:,} events, {sent / (time.time() - start):,.0f}/s", flush=True)
    futures.wait(pending)
    elapsed = time.time() - start
    if a.tail_seconds > 0:
        tail_end = time.time() + a.tail_seconds
        i = 0
        while time.time() < tail_end:
            stamp = iso(time.time())
            publisher.publish(topic, json.dumps({"order_id": -1 - i, "customer_id": 0, "order_total": 0.0,
                                                 "item_count": 0, "status": "tail", "event_time": stamp,
                                                 "produced_at": stamp}).encode(), event_time=stamp).result()
            i += 1
            time.sleep(1)
    result = {"events_sent": sent, "seconds": round(elapsed, 2), "events_per_second": round(sent / elapsed, 1),
              "target_rate": a.rate, "started_at": iso(start), "finished_at": iso(time.time())}
    Path("results").mkdir(exist_ok=True)
    Path("results/pubsub_publish.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
