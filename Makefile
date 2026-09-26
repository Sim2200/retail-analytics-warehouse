PY      := .venv/bin/python
DBT     := cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt
FLINKPY := .venv-flink/bin/python
COMPOSE := docker compose -f streaming/docker-compose.yml
SCALE   ?= 1.0

.PHONY: setup data load build build-incremental docs lint test stream-up stream stream-down stream-logs all clean

setup:              ## two virtualenvs: dbt stack, and PyFlink (their dependencies conflict)
	uv venv -q -p 3.11 .venv && uv pip install -q -p .venv/bin/python -r requirements.txt
	uv venv -q -p 3.11 .venv-flink && uv pip install -q -p .venv-flink/bin/python -r requirements-flink.txt

data:               ## generate the synthetic dataset (SCALE=0.05 for a CI-sized sample)
	$(PY) data_gen/generate.py --scale $(SCALE)

load: data          ## land the Parquet files in the raw schema of warehouse.duckdb
	$(PY) scripts/load_raw.py

build: load         ## full build: initial customer snapshot, then the update batch, then dbt build (run + test)
	$(DBT) run --select stg_customers --vars '{include_customer_updates: false}'
	$(DBT) snapshot --vars '{include_customer_updates: false}'
	$(DBT) build --full-refresh
	$(PY) scripts/dq_summary.py

build-incremental:  ## the everyday build: only new orders are processed
	$(DBT) build
	$(PY) scripts/dq_summary.py

docs:               ## dbt docs + lineage image (docs/lineage.png)
	$(DBT) docs generate
	$(PY) scripts/lineage.py
	@echo "open dbt/target/index.html for the docs site"

lint:               ## sqlfluff on all models
	.venv/bin/sqlfluff lint dbt/models dbt/snapshots

freshness:          ## dbt source freshness
	$(DBT) source freshness

stream-up:          ## Kafka (KRaft) + Flink JobManager/TaskManager
	$(COMPOSE) up -d --build --wait

stream:             ## replay orders into Kafka, run the Flink job, load the aggregates, report throughput + latency
	$(FLINKPY) streaming/run_experiment.py

stream-down:
	$(COMPOSE) down -v

stream-logs:
	$(COMPOSE) logs --tail 100 jobmanager taskmanager

all: build docs     ## batch side end to end (run `make stream-up stream` for the streaming side)

clean:
	rm -rf dbt/target dbt/logs warehouse.duckdb warehouse.duckdb.wal results/last_build.json
