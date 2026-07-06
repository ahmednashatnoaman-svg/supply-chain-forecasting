# Supply Chain Forecasting — developer entrypoints
# Run `make help` for the list.
.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose -f infra/docker/docker-compose.yml
# Set PROFILE=core for a lighter footprint (Kafka+Redis+Spark+HDFS only, no monitoring).
# Defaults to `full` (all services including Prometheus, Grafana, MLflow, n8n).
PROFILE ?= full

.PHONY: help setup data up down restart logs ps smoke test test-unit test-integration \
        test-e2e lint fmt type dq batch stream ingest dashboard clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Create venv, install deps + pre-commit hooks
	python -m venv .venv && . .venv/bin/activate && \
	  pip install -U pip && pip install -e ".[dev]" && pre-commit install

data: ## Download & stage the Retailrocket sample into HDFS
	bash scripts/download_data.sh && bash scripts/hdfs_load.sh

up: ## Start the stack (PROFILE=core|full, default=full)
	$(COMPOSE) --profile $(PROFILE) up -d && bash scripts/bootstrap.sh

down: ## Stop the stack
	$(COMPOSE) --profile $(PROFILE) down

restart: down up ## Restart the stack

logs: ## Tail all container logs
	$(COMPOSE) logs -f --tail=100

ps: ## Show container status
	$(COMPOSE) ps

ingest: ## Run the Kafka traffic generator (Hatem)
	python -m ingestion.generator.traffic_generator

batch: ## Run the nightly batch pipeline once (Emad)
	python -m batch.airflow_dags.run_batch_once

stream: ## Start the streaming pricing engine (Nashat)
	python -m streaming.pipeline.pricing_stream

dashboard: ## Launch the Streamlit command center (Ziad)
	streamlit run automation/dashboard/app.py

smoke: ## End-to-end walking-skeleton test
	pytest tests/e2e/test_walking_skeleton.py -v

test: test-unit test-integration ## Run unit + integration tests
test-unit: ## Run unit tests
	pytest tests/unit -v
test-integration: ## Run integration tests (testcontainers)
	pytest tests/integration -v
test-e2e: ## Run end-to-end tests
	pytest tests/e2e -v
dq: ## Run data-quality (Great Expectations) checks
	pytest tests/data_quality -v

lint: ## Lint with ruff
	ruff check .
fmt: ## Format with ruff + black
	ruff check --fix . && black .
type: ## Type-check with mypy
	mypy libs streaming batch ingestion automation

clean: ## Remove local build/cache artifacts
	find . -type d -name __pycache__ -prune -exec rm -rf {} + ; \
	rm -rf .pytest_cache .ruff_cache .mypy_cache spark-warehouse metastore_db derby.log
