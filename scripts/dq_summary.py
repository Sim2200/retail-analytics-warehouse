"""Write a data-quality summary row after each dbt build.

Reads dbt's target/run_results.json and appends one row per build to
governance.dq_summary (tests run / passed / failed / warned, models built,
build duration) plus one row per failed test to governance.dq_failures.
Analysts can query these tables to see whether the marts are trustworthy today.

    python scripts/dq_summary.py [--db warehouse.duckdb] [--run-results dbt/target/run_results.json]
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse.duckdb")
    ap.add_argument("--run-results", type=Path, default=Path("dbt/target/run_results.json"))
    a = ap.parse_args()
    rr = json.loads(a.run_results.read_text())
    results = rr["results"]
    tests = [r for r in results if r["unique_id"].startswith("test.")]
    models = [r for r in results if r["unique_id"].split(".")[0] in ("model", "snapshot", "seed")]
    status = lambda s: sum(1 for t in tests if t["status"] == s)  # noqa: E731
    summary = {
        "built_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "invocation_id": rr["metadata"]["invocation_id"],
        "command": rr["args"].get("which", ""),
        "models_built": sum(1 for m in models if m["status"] == "success"),
        "models_failed": sum(1 for m in models if m["status"] == "error"),
        "tests_run": len(tests),
        "tests_passed": status("pass"),
        "tests_failed": status("fail") + status("error"),
        "tests_warned": status("warn"),
        "build_seconds": round(rr["elapsed_time"], 2),
    }
    con = duckdb.connect(a.db)
    con.execute("CREATE SCHEMA IF NOT EXISTS governance")
    con.execute("""CREATE TABLE IF NOT EXISTS governance.dq_summary (
        built_at TIMESTAMP, invocation_id VARCHAR, command VARCHAR, models_built INTEGER, models_failed INTEGER,
        tests_run INTEGER, tests_passed INTEGER, tests_failed INTEGER, tests_warned INTEGER, build_seconds DOUBLE)""")
    con.execute("""CREATE TABLE IF NOT EXISTS governance.dq_failures (
        built_at TIMESTAMP, invocation_id VARCHAR, test_name VARCHAR, status VARCHAR, failures BIGINT, message VARCHAR)""")
    con.execute("INSERT INTO governance.dq_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", list(summary.values()))
    for t in tests:
        if t["status"] in ("fail", "error", "warn"):
            con.execute("INSERT INTO governance.dq_failures VALUES (?, ?, ?, ?, ?, ?)",
                        [summary["built_at"], summary["invocation_id"], t["unique_id"].split(".")[2],
                         t["status"], t.get("failures") or 0, (t.get("message") or "")[:500]])
    con.close()
    pct = 100 * summary["tests_passed"] / max(1, summary["tests_run"])
    print(f"dq_summary: {summary['tests_passed']}/{summary['tests_run']} tests passed ({pct:.1f}%), "
          f"{summary['models_built']} models built in {summary['build_seconds']}s")
    Path("results").mkdir(exist_ok=True)
    Path("results/last_build.json").write_text(json.dumps({**summary, "built_at": summary["built_at"].isoformat()}, indent=2))


if __name__ == "__main__":
    main()
