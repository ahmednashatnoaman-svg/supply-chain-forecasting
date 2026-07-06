# Cost & Licensing — Zero-Cost Guarantee

This project is engineered to run at **$0**. Every component is free & open-source and self-hosted in
Docker. This document is the audit trail proving it, plus the rules that keep it that way.

## Golden rules

1. **If a dependency needs a credit card, it is rejected.** No paid APIs, models, or SaaS.
2. **Compute is local.** Spark/Hadoop/Kafka/LSTM all run in your Docker. No managed clusters.
3. **Cloud is manifests-only.** `infra/helm` and `infra/terraform` are provided so the system *can*
   move to a cluster, but they are **never `apply`-ed** against a paid provider in this project.
4. **CI is free.** GitHub Actions bills nothing for **public** repositories (unlimited minutes),
   which is why this repo is public.

## Component inventory

| Component | Role | License | Cost | Notes |
|---|---|---|---|---|
| Apache Spark 3.5 | Compute (SQL/MLlib/GraphFrames/Streaming) | Apache-2.0 | Free | Self-hosted |
| Apache Hadoop 3.3 (HDFS) | Storage | Apache-2.0 | Free | Self-hosted |
| Apache Kafka + Zookeeper | Streaming bus | Apache-2.0 | Free | Self-hosted |
| Schema Registry (Apicurio) | Avro schema mgmt | Apache-2.0 | Free | OSS alt to Confluent SR |
| Redis 7 | Serving cache | RSALv2/SSPL (self-host free) | Free | Or Valkey (BSD) drop-in |
| GraphFrames | Graph on Spark | Apache-2.0 | Free | PySpark package |
| PyTorch | LSTM train + local inference | BSD-3 | Free | **No paid GPU/API** |
| MLflow | Model registry/tracking | Apache-2.0 | Free | Self-hosted server |
| Apache Airflow | Batch orchestration | Apache-2.0 | Free | Self-hosted |
| Great Expectations | Data quality | Apache-2.0 | Free | Library |
| n8n | Automation/reorder | Sustainable-Use (self-host free) | Free | Simulated emails |
| Streamlit | Dashboard | Apache-2.0 | Free | Self-hosted |
| Prometheus + Grafana | Monitoring | Apache-2.0 / AGPL-3.0 | Free | Self-hosted |
| Docker + Compose | Local orchestration | Apache-2.0 (Docker Desktop free for small/personal) | Free | See note ↓ |
| GitHub Actions | CI/CD | — | Free | **Public repo = unlimited minutes** |
| Retailrocket dataset | Data | CC (Kaggle) | Free | Free Kaggle account |

> **Docker Desktop note:** free for personal use, education, and small businesses. On Linux CI/servers
> use the fully-free Docker Engine / `docker compose` plugin. Podman is a free drop-in alternative.

## Free-tier substitutions (if you ever need externals)

| Need | Free option |
|---|---|
| SMTP for n8n supplier email | Gmail free SMTP, or Mailtrap free tier, or just log to console |
| Object storage (instead of HDFS) | MinIO (Apache-2.0, self-hosted, S3-compatible) |
| Managed Kafka (not used) | Keep self-hosted; Redpanda Community is a free OSS alternative |
| Model serving (not needed) | Local PyTorch `pandas_udf`; or free ONNX Runtime |

## What is explicitly NOT used (to avoid cost)

- ❌ OpenAI / Anthropic / any paid LLM or inference API in the data path.
- ❌ AWS EMR / GCP Dataproc / Databricks / Confluent Cloud billed tiers.
- ❌ Paid monitoring (Datadog/New Relic) — Prometheus/Grafana instead.
- ❌ Paid schema registry / paid data-quality SaaS.

## Keeping it zero-cost in review

CI includes a lightweight check (`scripts/check_no_paid_deps.py`) that scans `pyproject.toml` and
compose files for a denylist of known paid SDKs and fails the build if any appear.
