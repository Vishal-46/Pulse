# Pulse 2.0 - short commands. Run `make help` to list them.
PY := .venv/bin/python
# Steps depend on each other (stack -> data -> pipeline): never run targets in parallel,
# even if your shell aliases make to "make -j12".
.NOTPARALLEL:

.PHONY: help setup up down reset status replay live record stream extract pipeline pipeline-offline \
        report explain test test-int lint demo

help:            ## list commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-17s %s\n", $$1, $$2}'

setup:           ## create .venv (Python 3.12) and install dependencies
	uv sync
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

up:              ## start Redpanda, Console, RustFS (waits until healthy)
	docker compose up -d --wait

down:            ## stop and remove containers (data volumes are KEPT)
	docker compose down

reset:           ## DANGER: remove containers AND volumes (all Kafka + lake data)
	docker compose down -v

status:          ## containers + topic offsets
	docker compose ps
	docker exec pulse-redpanda rpk topic describe market.bars.1m -p || true

replay:          ## publish the recorded 1-minute session to Kafka (600x speed)
	$(PY) -m pulse.ingestion.producer replay --speed 600

live:            ## publish real 1-minute bars every 60 s (NSE hours: 09:15-15:30 IST)
	$(PY) -m pulse.ingestion.producer live

record:          ## save today's 1-minute bars to data/replay/ for later replay
	$(PY) -m pulse.ingestion.producer record

stream:          ## run Kafka -> Bronze continuously (30 s micro-batches), Ctrl+C to stop
	$(PY) -m pulse.streaming.bronze_stream

extract:         ## fetch 2 years of daily bars from Yahoo -> Bronze
	$(PY) -m pulse.ingestion.batch_extract

pipeline:        ## full batch pipeline: extract, bronze, silver, gold, quality, report
	$(PY) -m pulse.pipeline

pipeline-offline: ## same, but daily history from data/sample (no internet needed)
	$(PY) -m pulse.pipeline --daily-source sample

report:          ## run the Spark SQL analytics report on Gold
	$(PY) -m pulse.analytics.report

explain:         ## print Spark's physical plan for a Gold table (look for BroadcastHashJoin)
	$(PY) -m pulse.transform.gold --explain

test:            ## unit tests (no Docker needed)
	.venv/bin/pytest -v

test-int:        ## integration tests (needs `make up` + `make pipeline` first)
	.venv/bin/pytest -v -m integration

lint:            ## static checks
	.venv/bin/ruff check src tests

demo: up replay pipeline-offline ## from zero: start stack, publish data, run everything
