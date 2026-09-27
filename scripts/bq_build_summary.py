"""Summarise the last BigQuery dbt build: test counts, build time, bytes billed and cost.

Reads dbt/target/run_results.json (written by the BigQuery build) and queries
INFORMATION_SCHEMA.JOBS for every job dbt ran in that invocation, so bytes billed
and slot time are real, not estimated. Writes results/bigquery_build.json.

    python scripts/bq_build_summary.py --project <id> [--location US]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.cloud import bigquery

ON_DEMAND_USD_PER_TIB = 6.25  # BigQuery on-demand analysis price (US), after the free 1 TiB/month


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--location", default="US")
    ap.add_argument("--run-results", type=Path, default=Path("dbt/target/run_results.json"))
    a = ap.parse_args()
    rr = json.loads(a.run_results.read_text())
    results = rr["results"]
    tests = [r for r in results if r["unique_id"].startswith("test.")]
    models = [r for r in results if r["unique_id"].split(".")[0] in ("model", "snapshot")]
    invocation = rr["metadata"]["invocation_id"]

    client = bigquery.Client(project=a.project, location=a.location)
    q = f"""
        SELECT count(*) AS jobs, sum(total_bytes_billed) AS bytes_billed, sum(total_bytes_processed) AS bytes_processed,
               sum(total_slot_ms) AS slot_ms, min(creation_time) AS first_job, max(end_time) AS last_job
        FROM `region-{a.location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE creation_time > timestamp_sub(current_timestamp(), interval 2 hour)
          AND (job_type = 'QUERY') AND labels IS NOT NULL
          AND EXISTS (SELECT 1 FROM unnest(labels) l WHERE l.key = 'dbt_invocation_id' AND l.value = '{invocation}')
    """
    row = list(client.query(q).result())[0]
    bytes_billed = int(row.bytes_billed or 0)
    out = {
        "project": a.project, "location": a.location, "invocation_id": invocation,
        "models_built": sum(1 for m in models if m["status"] == "success"),
        "models_failed": sum(1 for m in models if m["status"] == "error"),
        "tests_run": len(tests), "tests_passed": sum(1 for t in tests if t["status"] == "pass"),
        "tests_failed": sum(1 for t in tests if t["status"] in ("fail", "error")),
        "build_seconds": round(rr["elapsed_time"], 1),
        "bigquery_jobs": int(row.jobs or 0),
        "bytes_processed": int(row.bytes_processed or 0),
        "bytes_billed": bytes_billed,
        "gib_billed": round(bytes_billed / 2**30, 3),
        "slot_seconds": round((row.slot_ms or 0) / 1000, 1),
        "on_demand_cost_usd": round(bytes_billed / 2**40 * ON_DEMAND_USD_PER_TIB, 4),
        "note": "cost at the on-demand list price; the first 1 TiB per month is free",
    }
    Path("results").mkdir(exist_ok=True)
    Path("results/bigquery_build.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
