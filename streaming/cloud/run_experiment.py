"""The streaming experiment on Google Cloud: Pub/Sub -> Dataflow (Beam) -> BigQuery.

Mirrors streaming/run_experiment.py (Kafka -> Flink -> DuckDB):

1. drain the Pub/Sub subscription so only this run's events count,
2. launch the Beam job on Dataflow and wait until it is RUNNING with workers,
3. replay orders with publish.py at the target rate (then a 1 event/s tail),
4. poll BigQuery every 2 s and record when each window row first becomes queryable
   (latency_ms = first_seen - window_end, the same definition as the local loader),
5. cancel the job, read its resource metrics, copy the windows with their latency
   into stream.window_metrics so the dbt model works, and write results/dataflow_run.json.

    python streaming/cloud/run_experiment.py --project <id> --rate 2000 --seconds 165
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PY = sys.executable
REGION = "us-central1"
RAW_TABLE = "stream.window_metrics_dataflow"   # written by the Beam job
FINAL_TABLE = "stream.window_metrics"          # + loaded_at / latency_ms, read by dbt

# Dataflow streaming list prices for us-central1 (USD), used only for the cost estimate.
PRICE_VCPU_H, PRICE_GB_H, PRICE_SHUFFLE_GB = 0.069, 0.003557, 0.018


def sh(*args: str, capture: bool = False, check: bool = True) -> str:
    r = subprocess.run(args, text=True, capture_output=capture, check=check)
    return r.stdout if capture else ""


def gcloud(*args: str) -> str:
    return sh("gcloud", *args, "--format=json", capture=True)


def job_state(project: str, name: str) -> dict | None:
    jobs = json.loads(gcloud("dataflow", "jobs", "list", "--project", project, "--region", REGION,
                             "--status=active", f"--filter=name={name}"))
    return jobs[0] if jobs else None


def job_metrics(project: str, job_id: str) -> dict:
    """Job-level resource counters from the Dataflow REST API (gcloud has no metrics command)."""
    try:
        import google.auth
        import google.auth.transport.requests
        import requests
        creds, _ = google.auth.default()
        creds.refresh(google.auth.transport.requests.Request())
        r = requests.get(f"https://dataflow.googleapis.com/v1b3/projects/{project}/locations/{REGION}/jobs/{job_id}/metrics",
                         headers={"Authorization": f"Bearer {creds.token}"}, timeout=30)
        want = ("TotalVcpuTime", "TotalMemoryUsage", "TotalStreamingDataProcessed", "TotalSeCuUsage", "TotalPdUsage",
                "CurrentVcpuCount")
        return {m["name"]["name"]: m.get("scalar") for m in r.json().get("metrics", [])
                if m["name"]["name"] in want and "tentative" not in (m["name"].get("context") or {})}
    except Exception as e:  # metrics are a nice-to-have; never lose the run over them
        print("   metrics unavailable:", e)
        return {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--rate", type=float, default=2000)
    ap.add_argument("--seconds", type=float, default=165)
    ap.add_argument("--tail-seconds", type=float, default=120)
    ap.add_argument("--workers", type=int, default=1, help="initial worker count")
    ap.add_argument("--max-workers", type=int, default=2)
    ap.add_argument("--out", default="results/dataflow_run.json")
    ap.add_argument("--machine-type", default="e2-medium")
    a = ap.parse_args()
    P = a.project
    bucket = f"gs://{P}-dataflow"
    name = f"order-metrics-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    client = bigquery.Client(project=P)

    print("1. drain subscription")
    sh("gcloud", "pubsub", "subscriptions", "seek", "orders-beam", "--project", P,
       f"--time={datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    client.query(f"""CREATE OR REPLACE TABLE `{P}.{RAW_TABLE}` (window_start TIMESTAMP, window_end TIMESTAMP,
                     order_count INT64, revenue FLOAT64, avg_basket_size FLOAT64, emitted_at TIMESTAMP)""").result()

    print("2. launch Dataflow job", name)
    t_launch = time.time()
    sh(PY, "streaming/cloud/order_metrics_beam.py",
       "--subscription", f"projects/{P}/subscriptions/orders-beam", "--table", f"{P}:{RAW_TABLE}",
       "--runner", "DataflowRunner", "--project", P, "--region", REGION, "--job_name", name,
       "--temp_location", f"{bucket}/tmp", "--staging_location", f"{bucket}/staging",
       "--machine_type", a.machine_type, "--num_workers", str(a.workers), "--max_num_workers", str(a.max_workers),
       "--enable_streaming_engine", "--experiments", "use_runner_v2")
    while True:
        j = job_state(P, name)
        st = (j.get("state") or j.get("currentState") or "?").replace("JOB_STATE_", "").title() if j else "?"
        print(f"   {st}", flush=True)
        if st == "Running":
            break
        if st in ("Failed", "Cancelled", "Done"):
            raise SystemExit("job did not start")
        time.sleep(15)
    t_running = time.time()
    print(f"   running after {t_running - t_launch:.0f} s; warm-up: 1 event/s until a window lands in BigQuery")
    warm = subprocess.Popen([PY, "streaming/cloud/publish.py", "--project", P, "--rate", "2", "--seconds", "900",
                             "--tail-seconds", "0"], stdout=subprocess.DEVNULL)
    while not list(client.query(f"SELECT 1 FROM `{P}.{RAW_TABLE}` LIMIT 1").result()):
        time.sleep(5)
    warm.terminate()
    t_ready = time.time()
    print(f"   first window landed {t_ready - t_running:.0f} s after RUNNING (worker harness up and reading)")
    client.query(f"TRUNCATE TABLE `{P}.{RAW_TABLE}`").result()

    print("3. publish + 4. poll BigQuery")
    pub = threading.Thread(target=lambda: sh(PY, "streaming/cloud/publish.py", "--project", P, "--rate", str(a.rate),
                                             "--seconds", str(a.seconds), "--tail-seconds", str(a.tail_seconds)))
    t0 = datetime.now(timezone.utc)
    pub.start()
    seen: dict[str, dict] = {}
    last_new = time.time()
    while pub.is_alive() or time.time() - last_new < 90:
        rows = client.query(f"""SELECT window_start, window_end, order_count, revenue, avg_basket_size, emitted_at
                                FROM `{P}.{RAW_TABLE}` WHERE emitted_at >= @t0""",
                            job_config=bigquery.QueryJobConfig(
                                query_parameters=[bigquery.ScalarQueryParameter("t0", "TIMESTAMP", t0)])).result()
        now = datetime.now(timezone.utc)
        for r in rows:
            k = r.window_start.isoformat()
            if k not in seen:
                lat = int((now - r.window_end).total_seconds() * 1000)
                seen[k] = {"window_start": r.window_start, "window_end": r.window_end, "orders": r.order_count,
                           "revenue": round(r.revenue, 2), "avg_basket": round(r.avg_basket_size, 3),
                           "emitted_at": r.emitted_at, "loaded_at": now,
                           "beam_ms": int((r.emitted_at - r.window_end).total_seconds() * 1000), "latency_ms": lat}
                last_new = time.time()
                print(f"   window {r.window_start:%H:%M} orders={r.order_count:,} revenue={r.revenue:,.2f} "
                      f"beam={seen[k]['beam_ms']} ms  end-to-end={lat} ms", flush=True)
        time.sleep(2)
    pub.join()

    print("5. cancel job, collect metrics")
    j = job_state(P, name)
    job_id = j["id"]
    totals = job_metrics(P, job_id)
    sh("gcloud", "dataflow", "jobs", "cancel", job_id, "--project", P, "--region", REGION, check=False)
    vcpu_h = float(totals.get("TotalVcpuTime") or 0) / 3600
    gb_h = float(totals.get("TotalMemoryUsage") or 0) / 1024 / 3600     # reported in MB-seconds
    shuffle_gb = float(totals.get("TotalStreamingDataProcessed") or 0) / 1024
    cost = vcpu_h * PRICE_VCPU_H + gb_h * PRICE_GB_H + shuffle_gb * PRICE_SHUFFLE_GB

    windows = sorted(seen.values(), key=lambda w: w["window_start"])
    full = [w for w in windows if w["orders"] > 0]
    if full:
        client.insert_rows_json(f"{P}.{FINAL_TABLE}", [{
            "window_start": w["window_start"].isoformat(), "window_end": w["window_end"].isoformat(),
            "order_count": w["orders"], "revenue": w["revenue"], "avg_basket_size": w["avg_basket"],
            "emitted_at": w["emitted_at"].isoformat(), "loaded_at": w["loaded_at"].isoformat(),
            "latency_ms": w["latency_ms"]} for w in full])
    lat = [w["latency_ms"] for w in full]
    beam_ms = [w["beam_ms"] for w in full]
    out = {
        "job": {"name": name, "id": job_id, "region": REGION, "machine_type": a.machine_type,
                "workers": a.workers, "max_workers": a.max_workers, "seconds_to_running": round(t_running - t_launch),
                "seconds_to_first_window": round(t_ready - t_running),
                "wall_seconds": round(time.time() - t_launch)},
        "publisher": json.loads(Path("results/pubsub_publish.json").read_text()),
        "windows": len(full), "orders_in_windows": sum(w["orders"] for w in full),
        "latency_ms": {"min": min(lat), "median": statistics.median(lat), "max": max(lat)} if lat else None,
        "beam_emit_ms": {"min": min(beam_ms), "median": statistics.median(beam_ms), "max": max(beam_ms)} if beam_ms else None,
        "dataflow_metrics": totals,
        "cost_estimate_usd": {"vcpu_hours": round(vcpu_h, 3), "memory_gb_hours": round(gb_h, 3),
                              "streaming_data_gb": round(shuffle_gb, 3), "total": round(cost, 4),
                              "note": "Dataflow list prices for us-central1; Pub/Sub and BigQuery streaming inserts are cents at this volume"},
        "windows_detail": [{**w, "window_start": w["window_start"].isoformat(), "window_end": w["window_end"].isoformat(),
                            "emitted_at": w["emitted_at"].isoformat(), "loaded_at": w["loaded_at"].isoformat()} for w in windows],
    }
    Path(a.out).write_text(json.dumps(out, indent=2))
    print(json.dumps({k: v for k, v in out.items() if k != "windows_detail"}, indent=2))


if __name__ == "__main__":
    main()
