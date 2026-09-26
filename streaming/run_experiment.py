"""The streaming experiment, end to end (requires `make stream-up`):

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
import subprocess
import sys
import time
from pathlib import Path

COMPOSE = ["docker", "compose", "-f", "streaming/docker-compose.yml"]
PY = sys.executable  # the flink venv; the producer/loader only need kafka-python + duckdb
DBT_PY = ".venv/bin/python"


def sh(*args: str, check: bool = True, capture: bool = False) -> str:
    r = subprocess.run(args, check=check, text=True, capture_output=capture)
    return r.stdout if capture else ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=2000)
    ap.add_argument("--seconds", type=float, default=180)
    a = ap.parse_args()

    print("== topics")
    for topic in ("orders", "order_metrics"):
        sh(*COMPOSE, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092",
           "--create", "--if-not-exists", "--topic", topic, "--partitions", "4", "--replication-factor", "1")

    print("== submit Flink job")
    out = sh(*COMPOSE, "exec", "-T", "jobmanager", "flink", "run", "-d", "-py", "/opt/flink/jobs/order_metrics.py", capture=True)
    job_id = next((line.split()[-1] for line in out.splitlines() if "JobID" in line), None)
    print(f"   job {job_id}")
    time.sleep(8)  # let the job reach RUNNING before traffic starts

    print("== loader (background)")
    loader = subprocess.Popen([DBT_PY, "streaming/loader.py", "--reset", "--idle", "150"])

    print(f"== producer: {a.rate:g} events/s for {a.seconds:g}s")
    sh(DBT_PY, "streaming/producer.py", "--rate", str(a.rate), "--seconds", str(a.seconds))

    print("== waiting for the last windows to close and load")
    loader.wait()

    if job_id:
        sh(*COMPOSE, "exec", "-T", "jobmanager", "flink", "cancel", job_id, check=False)

    producer = json.loads(Path("results/producer.json").read_text())
    streaming = json.loads(Path("results/streaming.json").read_text())
    merged = {"producer": producer, **streaming}
    Path("results/streaming_run.json").write_text(json.dumps(merged, indent=2))
    print("== result")
    print(json.dumps({k: v for k, v in merged.items() if k != "windows_detail"}, indent=2))


if __name__ == "__main__":
    main()
