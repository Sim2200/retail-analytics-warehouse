"""Replay orders from the warehouse into the Kafka `orders` topic as JSON events.

Events are sent in original order at a target rate (or as fast as possible with
--rate 0). Each event carries:
  event_time   the moment the event was produced (this is the event-time the
               Flink job windows on, so windows are wall-clock minutes)
  produced_at  the same timestamp, kept separately so latency can be measured
               even if event_time is later replaced by the original ordered_at
  ordered_at   the original timestamp from the dataset, for reference

The replay set is a Parquet file exported from the warehouse beforehand
(streaming/export_replay.py), so the producer never opens warehouse.duckdb while
the loader holds its write lock.

Writes results/producer.json with events sent, wall time and events/second.

    python streaming/producer.py --rate 2000 --seconds 180
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
from kafka import KafkaProducer


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ts * 1000) % 1000:03d}Z"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--replay", default="data/replay_orders.parquet")
    ap.add_argument("--rate", type=float, default=2000, help="target events/second; 0 = unthrottled")
    ap.add_argument("--seconds", type=float, default=180, help="stop after this long")
    ap.add_argument("--limit", type=int, default=0, help="max events (0 = all orders)")
    a = ap.parse_args()

    table = pq.read_table(a.replay, columns=["order_id", "customer_id", "order_total", "item_count", "status"])
    if a.limit:
        table = table.slice(0, a.limit)
    rows = zip(*(table.column(c).to_pylist() for c in ("order_id", "customer_id", "order_total", "item_count", "status")))

    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap, acks=1, linger_ms=20, batch_size=64 * 1024,
        compression_type="lz4", value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: str(k).encode(),
    )
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
        producer.send("orders", key=order_id, value={
            "order_id": order_id, "customer_id": customer_id, "order_total": float(total),
            "item_count": int(items), "status": status, "event_time": stamp, "produced_at": stamp,
        })
        sent += 1
        if sent % 50_000 == 0:
            print(f"  {sent:,} events, {sent / (time.time() - start):,.0f}/s", flush=True)
    producer.flush()
    elapsed = time.time() - start
    result = {"events_sent": sent, "seconds": round(elapsed, 2), "events_per_second": round(sent / elapsed, 1),
              "target_rate": a.rate, "started_at": iso(start), "finished_at": iso(time.time())}
    Path("results").mkdir(exist_ok=True)
    Path("results/producer.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
