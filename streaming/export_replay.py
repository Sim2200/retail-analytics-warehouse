"""Export the orders to replay (from marts.fact_orders, in time order) to a Parquet file.

Run before the producer so it does not need the DuckDB file, which the loader
locks for writing during the experiment.

    python streaming/export_replay.py [--db warehouse.duckdb] [--out data/replay_orders.parquet]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse.duckdb")
    ap.add_argument("--out", type=Path, default=Path("data/replay_orders.parquet"))
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(a.db, read_only=True)
    con.execute(f"""COPY (SELECT order_id, customer_id, ordered_at, order_total, item_count, status
                         FROM marts.fact_orders ORDER BY ordered_at)
                    TO '{a.out}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{a.out}')").fetchone()[0]
    con.close()
    print(f"exported {n:,} orders -> {a.out}")


if __name__ == "__main__":
    main()
