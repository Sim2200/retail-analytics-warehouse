PY      := .venv/bin/python
BEAMPY  := .venv-beam/bin/python
DBT     := cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt
FLINKPY := .venv-flink/bin/python
COMPOSE := docker compose -f streaming/docker-compose.yml
SCALE   ?= 1.0
BQ_PROJECT ?= your-gcp-project-id
DBT_BQ  := cd dbt && DBT_PROFILES_DIR=. BQ_PROJECT=$(BQ_PROJECT) ../.venv/bin/dbt

.PHONY: setup data load build build-incremental docs lint test stream-up stream stream-down stream-logs all clean bq-load bq-build cloud-stream-up cloud-stream cloud-stream-down

setup:              ## two virtualenvs: dbt stack, and PyFlink (their dependencies conflict)
	uv venv -q -p 3.11 .venv && uv pip install -q -p .venv/bin/python -r requirements.txt
	uv venv -q -p 3.11 .venv-flink && uv pip install -q -p .venv-flink/bin/python -r requirements-flink.txt

setup-beam:         ## third venv for the Dataflow experiment (apache-beam[gcp])
	uv venv -q -p 3.11 .venv-beam && uv pip install -q -p .venv-beam/bin/python -r requirements-beam.txt

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

bq-load:            ## load the raw Parquet files into BigQuery (needs gcloud auth application-default login)
	$(PY) scripts/load_raw_bigquery.py --project $(BQ_PROJECT)

bq-build:           ## the same two-pass snapshot + dbt build, on BigQuery; writes results/bigquery_build.json
	$(DBT_BQ) run --target bigquery --select stg_customers --vars '{include_customer_updates: false}'
	$(DBT_BQ) snapshot --target bigquery --vars '{include_customer_updates: false}'
	$(DBT_BQ) build --target bigquery --full-refresh
	$(PY) scripts/bq_build_summary.py --project $(BQ_PROJECT)
	$(PY) scripts/bq_authorized_view.py --project $(BQ_PROJECT)

cloud-stream-up:    ## Pub/Sub topic + subscription, Dataflow staging bucket, worker IAM (one-off)
	gcloud services enable dataflow.googleapis.com pubsub.googleapis.com --project $(BQ_PROJECT)
	gcloud storage buckets create gs://$(BQ_PROJECT)-dataflow --location us-central1 --project $(BQ_PROJECT) || true
	gcloud pubsub topics create orders --project $(BQ_PROJECT) || true
	gcloud pubsub subscriptions create orders-beam --topic orders --project $(BQ_PROJECT) --ack-deadline 60 || true

cloud-stream:       ## Pub/Sub -> Dataflow (Beam) -> BigQuery: replay, window, measure -> results/dataflow_run.json
	$(BEAMPY) streaming/cloud/run_experiment.py --project $(BQ_PROJECT) --rate 2000 --seconds 165

cloud-stream-down:  ## cancel any running job and delete the topic, subscription and bucket
	-gcloud dataflow jobs list --project $(BQ_PROJECT) --region us-central1 --status=active --format='value(id)' \
	  | xargs -n1 -I{} gcloud dataflow jobs cancel {} --project $(BQ_PROJECT) --region us-central1
	-gcloud pubsub subscriptions delete orders-beam --project $(BQ_PROJECT) --quiet
	-gcloud pubsub topics delete orders --project $(BQ_PROJECT) --quiet
	-gcloud storage rm -r gs://$(BQ_PROJECT)-dataflow --quiet

all: build docs     ## batch side end to end (run `make stream-up stream` for the streaming side)

clean:
	rm -rf dbt/target dbt/logs warehouse.duckdb warehouse.duckdb.wal results/last_build.json
