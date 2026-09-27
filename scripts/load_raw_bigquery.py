"""Land the Parquet files in the `raw` dataset of a BigQuery project (load jobs: free).

Same tables and `_loaded_at` column as scripts/load_raw.py, so the dbt sources
work unchanged. Also creates the empty `stream.window_metrics` table.

    python scripts/load_raw_bigquery.py --project <id> [--location US]
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

TABLES = ["customers", "customer_updates", "products", "orders", "order_items"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--location", default="US")
    ap.add_argument("--src", type=Path, default=Path("data/raw"))
    a = ap.parse_args()
    client = bigquery.Client(project=a.project, location=a.location)
    for ds in ("raw", "stream"):
        client.create_dataset(bigquery.Dataset(f"{a.project}.{ds}"), exists_ok=True)
    t0 = time.perf_counter()
    loaded_at = datetime.now(timezone.utc)
    for name in TABLES:
        table = pq.read_table(a.src / f"{name}.parquet")
        table = table.append_column("_loaded_at", pa.array([loaded_at] * table.num_rows, pa.timestamp("us", tz="UTC")))
        tmp = Path("/tmp") / f"{name}_bq.parquet"
        pq.write_table(table, tmp)
        job = client.load_table_from_file(
            tmp.open("rb"), f"{a.project}.raw.{name}",
            job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                              write_disposition="WRITE_TRUNCATE"))
        job.result()
        rows = client.get_table(f"{a.project}.raw.{name}").num_rows
        print(f"raw.{name:17s} {rows:>10,} rows")
    schema = [bigquery.SchemaField(n, t) for n, t in (
        ("window_start", "TIMESTAMP"), ("window_end", "TIMESTAMP"), ("order_count", "INT64"), ("revenue", "FLOAT64"),
        ("avg_basket_size", "FLOAT64"), ("emitted_at", "TIMESTAMP"), ("loaded_at", "TIMESTAMP"), ("latency_ms", "INT64"))]
    client.create_table(bigquery.Table(f"{a.project}.stream.window_metrics", schema=schema), exists_ok=True)
    print(f"loaded in {time.perf_counter() - t0:.1f}s -> {a.project} ({a.location})")


if __name__ == "__main__":
    main()
