"""Consume the Flink window aggregates from Kafka and write them to stream.window_metrics.

For every window row it records loaded_at (now) and
  latency_ms = loaded_at - window_end
which is the end-to-end time from a window closing to its aggregate being
queryable in the warehouse: watermark delay + Flink processing + Kafka + this loader.

Stops after --idle seconds without a new message. Writes results/streaming.json.

    python streaming/loader.py --idle 90
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from kafka import KafkaConsumer


def parse_ts(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--db", default="warehouse.duckdb")
    ap.add_argument("--idle", type=float, default=90, help="stop after this many seconds without messages")
    ap.add_argument("--reset", action="store_true", help="truncate stream.window_metrics first")
    a = ap.parse_args()

    con = duckdb.connect(a.db)
    if a.reset:
        con.execute("DELETE FROM stream.window_metrics")
    consumer = KafkaConsumer("order_metrics", bootstrap_servers=a.bootstrap, auto_offset_reset="earliest",
                             group_id=None, consumer_timeout_ms=int(a.idle * 1000),
                             value_deserializer=lambda v: json.loads(v.decode()))
    rows = []
    for msg in consumer:
        v = msg.value
        loaded = datetime.now(timezone.utc).replace(tzinfo=None)
        w_end, emitted = parse_ts(v["window_end"]), parse_ts(v["emitted_at"])
        latency_ms = int((loaded - w_end).total_seconds() * 1000)
        row = (parse_ts(v["window_start"]), w_end, int(v["order_count"]), float(v["revenue"]),
               float(v["avg_basket_size"]), emitted, loaded, latency_ms)
        con.execute("INSERT INTO stream.window_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)
        rows.append({"window_start": row[0].isoformat(), "window_end": row[1].isoformat(), "orders": row[2],
                     "revenue": round(row[3], 2), "avg_basket": round(row[4], 3),
                     "flink_ms": int((emitted - w_end).total_seconds() * 1000), "latency_ms": latency_ms})
        print(f"window {row[0]:%H:%M} orders={row[2]:,} revenue={row[3]:,.2f} "
              f"flink={rows[-1]['flink_ms']} ms  end-to-end={latency_ms} ms", flush=True)
    con.close()
    lat = [r["latency_ms"] for r in rows]
    flink = [r["flink_ms"] for r in rows]
    summary = {
        "windows": len(rows),
        "orders_in_windows": sum(r["orders"] for r in rows),
        "latency_ms": {"min": min(lat), "median": statistics.median(lat), "max": max(lat)} if lat else None,
        "flink_emit_ms": {"min": min(flink), "median": statistics.median(flink), "max": max(flink)} if flink else None,
        "windows_detail": rows,
    }
    Path("results").mkdir(exist_ok=True)
    Path("results/streaming.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "windows_detail"}))


if __name__ == "__main__":
    main()
