"""Land the Parquet files in the `raw` schema of the DuckDB warehouse.

Each table gets a `_loaded_at` column so dbt source freshness has something to check.

    python scripts/load_raw.py                 # data/raw -> warehouse.duckdb
    python scripts/load_raw.py --db ci.duckdb  # CI
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

TABLES = ["customers", "customer_updates", "products", "orders", "order_items"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse.duckdb")
    ap.add_argument("--src", type=Path, default=Path("data/raw"))
    a = ap.parse_args()
    con = duckdb.connect(a.db)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    t0 = time.perf_counter()
    for name in TABLES:
        path = a.src / f"{name}.parquet"
        if not path.exists():
            raise SystemExit(f"missing {path}; run `make data` first")
        con.execute(f"CREATE OR REPLACE TABLE raw.{name} AS SELECT *, now() AS _loaded_at FROM read_parquet('{path}')")
        rows = con.execute(f"SELECT count(*) FROM raw.{name}").fetchone()[0]
        print(f"raw.{name:17s} {rows:>10,} rows")
    # The streaming sink table, empty until the Flink job has run, so dbt models
    # that read it build (and pass tests) in CI without Kafka.
    con.execute("CREATE SCHEMA IF NOT EXISTS stream")
    con.execute("""CREATE TABLE IF NOT EXISTS stream.window_metrics (
        window_start TIMESTAMP, window_end TIMESTAMP, order_count BIGINT, revenue DOUBLE,
        avg_basket_size DOUBLE, emitted_at TIMESTAMP, loaded_at TIMESTAMP, latency_ms BIGINT)""")
    con.close()
    print(f"loaded in {time.perf_counter() - t0:.1f}s -> {a.db}")


if __name__ == "__main__":
    main()
