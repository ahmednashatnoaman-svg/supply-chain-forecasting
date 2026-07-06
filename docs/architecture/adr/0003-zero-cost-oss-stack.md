# ADR 0003 — Zero-cost, fully open-source, self-hosted stack

**Status:** Accepted · **Date:** 2026-07-06

## Context
Hard requirement: the project must run at **$0** — no paid APIs, models, SaaS, or billed cloud.

## Decision
Every component is free & open-source and self-hosted in Docker (Spark, Hadoop, Kafka, Redis, MLflow,
Airflow, n8n, Prometheus, Grafana, Apicurio Schema Registry). CI runs on **GitHub Actions free minutes
for public repos** (hence the repo is public). Cloud manifests (Helm/Terraform) are **never applied** to
a paid provider. A CI guard (`scripts/check_no_paid_deps.py`) fails the build if a paid SDK appears.

## Consequences
- ✅ Reproducible on any laptop; no credit card, ever.
- ✅ Public repo doubles as a portfolio artifact.
- ⚠️ Local resource ceiling (RAM/CPU). Mitigated by Compose profiles (`core` vs `full`) + sample dataset.
- See [cost-and-licensing](../../reference/cost-and-licensing.md) for the full inventory.
