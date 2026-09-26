"""The streaming experiment, end to end (requires `make stream-up`):

0. export the replay set to Parquet (the producer must not open DuckDB while the loader writes to it),
1. create the topics,
2. submit the Flink job (detached),
3. start the loader in the background,
4. replay orders with the producer at the target rate,
5. wait for the last windows to land, then stop the loader and cancel the job,
6. merge results/producer.json + results/streaming.json into results/streaming_run.json.

    python streaming/run_experiment.py --rate 2000 --seconds 180
"""

from __future__ import annotations

import argparse
import json
import json as _json
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

COMPOSE = ["docker", "compose", "-f", "streaming/docker-compose.yml"]
PY = sys.executable  # the flink venv; the producer/loader only need kafka-python + duckdb
DBT_PY = ".venv/bin/python"


def sh(*args: str, check: bool = True, capture: bool = False) -> str:
    r = subprocess.run(args, check=check, text=True, capture_output=capture)
    return r.stdout if capture else ""


def flink_jobs() -> list[dict]:
    with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=10) as r:
        return _json.load(r)["jobs"]


def sample_flink_rate(job_id: str | None, samples: list[float], stop: threading.Event) -> None:
    """Every 5 s, read the Kafka source operator's numRecordsOutPerSecond (summed over its
    subtasks) from the Flink REST API. The task-level counters are useless here: the source
    task has no input, and its output is the pre-aggregated window state, not the events."""
    if not job_id:
        return
    try:
        with urllib.request.urlopen(f"http://localhost:8081/jobs/{job_id}", timeout=10) as r:
            job = _json.load(r)
        vertex = next(v for v in job["vertices"] if v["name"].startswith("Source"))
        names = [f"{i}.Source__orders[1].numRecordsOutPerSecond" for i in range(vertex["parallelism"])]
    except Exception:  # noqa: BLE001
        return
    url = (f"http://localhost:8081/jobs/{job_id}/vertices/{vertex['id']}/metrics?get="
           + ",".join(n.replace("[", "%5B").replace("]", "%5D") for n in names))
    while not stop.is_set():
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                data = _json.load(r)
            total = sum(float(m["value"]) for m in data if m.get("value") not in (None, ""))
            if total > 0:
                samples.append(total)
        except Exception:  # noqa: BLE001
            pass
        stop.wait(5)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=2000)
    ap.add_argument("--seconds", type=float, default=180)
    ap.add_argument("--tail", type=float, default=75, help="trailer seconds so the last window closes (see producer.py)")
    ap.add_argument("--out", default="results/streaming_run.json")
    a = ap.parse_args()

    print("== export replay set")
    sh(DBT_PY, "streaming/export_replay.py")

    print("== cancel leftover jobs, recreate topics (a clean run must not replay old events)")
    for job in flink_jobs():
        if job["state"] == "RUNNING":
            sh(*COMPOSE, "exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False, capture=True)
    kafka_topics = [*COMPOSE, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092"]
    for topic in ("orders", "order_metrics"):
        sh(*kafka_topics, "--delete", "--if-exists", "--topic", topic, capture=True)
    time.sleep(3)
    for topic in ("orders", "order_metrics"):
        sh(*kafka_topics, "--create", "--if-not-exists", "--topic", topic, "--partitions", "4", "--replication-factor", "1", capture=True)

    print("== submit Flink job")
    out = sh(*COMPOSE, "exec", "-T", "jobmanager", "flink", "run", "-d", "-py", "/opt/flink/jobs/order_metrics.py", capture=True)
    job_id = next((line.split()[-1] for line in out.splitlines() if "JobID" in line), None)
    print(f"   job {job_id}")
    time.sleep(8)  # let the job reach RUNNING before traffic starts

    print("== loader (background)")
    loader = subprocess.Popen([DBT_PY, "streaming/loader.py", "--reset", "--idle", "150"])

    samples: list[float] = []
    stop = threading.Event()
    sampler = threading.Thread(target=sample_flink_rate, args=(job_id, samples, stop), daemon=True)
    sampler.start()

    print(f"== producer: {a.rate:g} events/s for {a.seconds:g}s")
    sh(DBT_PY, "streaming/producer.py", "--rate", str(a.rate), "--seconds", str(a.seconds), "--tail-seconds", str(a.tail))
    stop.set()

    print("== waiting for the last windows to close and load")
    loader.wait()

    if job_id:
        sh(*COMPOSE, "exec", "-T", "jobmanager", "flink", "cancel", job_id, check=False)

    producer = json.loads(Path("results/producer.json").read_text())
    streaming = json.loads(Path("results/streaming.json").read_text())
    merged = {"producer": producer,
              "flink_records_in_per_second": {"peak": round(max(samples), 1) if samples else None,
                                              "mean": round(sum(samples) / len(samples), 1) if samples else None,
                                              "samples": len(samples)},
              **streaming}
    Path(a.out).write_text(json.dumps(merged, indent=2))
    print("== result")
    print(json.dumps({k: v for k, v in merged.items() if k != "windows_detail"}, indent=2))


if __name__ == "__main__":
    main()
