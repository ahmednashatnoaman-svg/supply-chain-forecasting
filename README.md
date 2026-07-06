# Supply Chain Demand Forecasting & Dynamic Pricing

A production-grade **Lambda-architecture** big-data platform that forecasts retail demand and sets
prices dynamically in real time. Historical sales train a nightly baseline (batch layer); live
clickstream traffic modulates prices within seconds (speed layer); Redis + a dashboard serve results.

Built on **Apache Spark** (SQL, MLlib, GraphFrames, Structured Streaming, Deep Learning),
**Apache Kafka**, and **Hadoop/HDFS**, orchestrated across a 5-person team.

---

## Architecture at a glance

```
Retailrocket CSVs ─▶ HDFS (bronze/silver/gold)
                          │  nightly batch (Airflow)
        ┌─────────────────┴─────────────────┐
        ▼                                     ▼
  MLlib demand forecast              GraphFrames cross-elasticity
        └─────────────┬───────────────────────┘
                      ▼  publish
                    REDIS  ◀────────────┐  read
                      ▲                 │
   Kafka: live_web_traffic ─▶ Spark Structured Streaming
                                        │ + LSTM surge classifier
                                        │ + dynamic pricing formula
                                        ▼ produce
                       automated_pricing_updates / system_alerts
                                        │
                     ┌──────────────────┴───────────────┐
                     ▼                                   ▼
             n8n auto-reorder                  Streamlit dashboard
```

See [`docs/architecture/system-design.md`](docs/architecture/system-design.md) for the full design
and [`docs/architecture/data-contracts.md`](docs/architecture/data-contracts.md) for the interface
contracts that let all five layers integrate.

## Team & ownership

| Member | Layer owned | Directory |
|---|---|---|
| **Nashat** (Tech Lead) | Speed layer: streaming + LSTM + pricing; shared `contracts/`; CI/CD | [`streaming/`](streaming/) |
| **Emad** (Data/ML Lead) | Batch layer: ETL + MLlib + GraphFrames; Airflow; data quality | [`batch/`](batch/) |
| **Nagy** | Infrastructure: Docker/HDFS/Spark/Kafka/Redis; monitoring; K8s | [`infra/`](infra/) |
| **Hatem** | Ingestion & simulation: Kafka topics, traffic generator | [`ingestion/`](ingestion/) |
| **Ziad** | Automation & serving: n8n, Streamlit dashboard | [`automation/`](automation/) |

Full plans: [master plan](docs/master-plan.md) · per-member plans in [`docs/plans/`](docs/plans/).

**New to the repo?** Open this repo in Claude Code and ask it to "onboard me" (or run `/scf-onboard
<your-name>`) — it reads `.claude/skills/scf-onboard/SKILL.md` and gives you your layer, your next
open task, and the git/PR conventions in under a minute.

**Financial & pricing strategy:** [pricing-strategy](docs/reference/pricing-strategy.md) (value-based +
honest psychology) · [financial-model](docs/reference/financial-model.md) (ROI business case) ·
[cost-and-licensing](docs/reference/cost-and-licensing.md) (zero-cost guarantee).

**Technical playbooks:** [kafka-playbook](docs/reference/kafka-playbook.md) ·
[spark-playbook](docs/reference/spark-playbook.md) · [mlops-playbook](docs/reference/mlops-playbook.md).

## Quick start

**Dataset:** [Retailrocket Recommender System Dataset (Kaggle, free)](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
— chosen over Instacart because `item_properties.csv` carries the price/time-series signal dynamic
pricing needs. See [ADR-0004](docs/architecture/adr/0004-dataset-choice.md) for the full rationale.
Needs a free Kaggle account + API token (`~/.kaggle/kaggle.json`) — `make data` uses it automatically.

```bash
cp .env.example .env          # fill in local values
make setup                    # create venv, install deps, pre-commit hooks
make data                     # download & stage Retailrocket sample into HDFS
make up                       # start the full stack (Docker Compose)
make smoke                    # run the end-to-end walking-skeleton test
make dashboard                # open the Streamlit command center
```

See [`docs/runbooks/`](docs/runbooks/) for operational procedures and
[`docs/reference/project-brief.md`](docs/reference/project-brief.md) for the original brief.

## Tech stack

Spark 3.5 · Hadoop 3.3 (HDFS) · Kafka 3.x + Schema Registry (Avro) · Redis 7 · GraphFrames ·
PyTorch (LSTM) · MLflow · Airflow · Great Expectations · n8n · Streamlit · Prometheus + Grafana ·
Docker Compose (dev) / Helm + Terraform (cloud-ready) · GitHub Actions CI/CD.

## License

[MIT](LICENSE)
