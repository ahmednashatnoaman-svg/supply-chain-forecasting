# Design Spec — Supply Chain Demand Forecasting & Dynamic Pricing

- **Date:** 2026-07-06
- **Status:** Approved (brainstorming gate passed)
- **Authors:** Nashat (tech lead) + team
- **Supersedes:** `docs/reference/project-brief.md` (original outline)

---

## 1. Problem & goals

Retailers lose money on both **stock-outs** (lost sales) and **overstock** (holding cost, markdowns),
and static prices leave margin on the table during demand spikes. This system builds a **closed loop**:
historical sales predict a **baseline demand** (batch), live clickstream traffic detects **surges** and
adjusts **price** in near-real-time (speed), while respecting **cross-elasticity** so raising one price
does not cannibalize a connected high-margin product.

**Success criteria**

1. Nightly batch produces a per-SKU 30-day demand forecast and a product cross-elasticity graph.
2. A live surge on `live_web_traffic` produces a new price on `automated_pricing_updates` within the
   configured streaming window (default 60s), bounded by margin/uplift guardrails.
3. Low stock during a surge fires `system_alerts` → n8n drafts an emergency purchase order.
4. The whole path is observable (Prometheus/Grafana), tested (unit→integration→e2e→data-quality),
   and reproducible via `make up` on Docker, with cloud-ready manifests.

## 2. Architecture — Lambda (batch + speed + serving)

- **Batch layer (Emad):** HDFS medallion (bronze→silver→gold). PySpark ETL builds rolling 7d/30d
  velocity features; MLlib (Gradient Boosted Trees) forecasts baseline demand; GraphFrames builds the
  product knowledge graph (PageRank + community detection) for cross-elasticity. Airflow orchestrates
  nightly; results published to Redis + MLflow.
- **Speed layer (Nashat):** Spark Structured Streaming consumes `live_web_traffic`, computes
  sliding-window sales velocity, runs an LSTM surge classifier (PyTorch, served via `pandas_udf`),
  applies the pricing formula using baseline + elasticity read from Redis, and emits
  `automated_pricing_updates` and `system_alerts`.
- **Serving layer:** Redis holds the hot state (forecasts, elasticity, current price, inventory).
  Ziad's Streamlit dashboard and n8n automation read from Redis/Kafka.
- **Ingestion (Hatem):** Kafka topic + Schema Registry design and a Python traffic generator that
  replays the Retailrocket event stream with synthetic surges; plus HDFS data staging.
- **Infra (Nagy):** Docker Compose stack (Hadoop/HDFS, Spark master+workers, Kafka+ZK, Redis,
  Prometheus, Grafana, MLflow, n8n) + K8s/Helm + Terraform stubs for cloud lift.

## 3. Integration contracts (the crux of parallel work)

All cross-layer interfaces are frozen in `contracts/` and treated as versioned APIs. Changing one
requires a PR reviewed by the two adjacent owners.

- **Kafka topics + Avro:** `live_web_traffic`, `inventory_updates`, `system_alerts`,
  `automated_pricing_updates`. Keys, partitions, and Avro payloads defined in `contracts/avro/`.
- **Redis keys:** `forecast:{sku}`, `graph:elasticity:{sku}`, `graph:community:{sku}`,
  `price:current:{sku}`, `inventory:{sku}`, `velocity:{sku}:{window}` — in `contracts/redis/`.
- **HDFS medallion:** `/data/{bronze,silver,gold}/...` Parquet schemas — in `contracts/hdfs/`.
- **Model signatures:** MLlib forecast output schema + LSTM tensor spec — in `contracts/models/`,
  versioned in MLflow.

## 4. Decisions & rationale

| Decision | Choice | Why |
|---|---|---|
| Graph engine | **GraphFrames** (not raw GraphX) | GraphX is Scala-only; GraphFrames gives PySpark parity. |
| DL framework | **PyTorch** LSTM, inference via `pandas_udf` | Simplest reliable path to distributed inference in PySpark; export to MLflow. |
| Kafka serialization | **Avro + Schema Registry** | Schema evolution safety for a production, multi-team system. |
| Batch orchestration | **Airflow** | Production-grade scheduling/retries/backfill for the nightly DAG. |
| Serving store | **Redis** | Microsecond reads for the streaming pricing loop. |
| Dev environment | **Docker Compose** mirrors prod; Helm/Terraform for cloud | Reproducible locally, liftable to EMR/Dataproc/K8s. |

## 5. Orchestration approach

**Contract-First Parallel + Walking Skeleton** (chosen over sequential or pure vertical-slice):
Week 0 the team co-authors `contracts/` and gets a thin end-to-end path running with stubs; then each
member owns one layer and deepens it in parallel against mocks. Integration is continuous.

## 6. Team & scope

| Member | Layer | Weight | Cross-cutting |
|---|---|---|---|
| Nashat (lead) | Speed: streaming + LSTM + pricing | Heaviest | `contracts/`, CI/CD |
| Emad | Batch: ETL + MLlib + GraphFrames | Heavy | data quality, MLflow registry |
| Nagy | Infrastructure + monitoring | Foundational | K8s/Helm manifests |
| Hatem | Ingestion & simulation | Moderate | Schema Registry |
| Ziad | Automation (n8n) + dashboard | Moderate | serving-layer UX |

## 7. Testing & production strategy

- **Unit:** pytest + chispa (Spark DF assertions). **Integration:** testcontainers (Kafka/Redis/Spark).
  **E2E:** inject surge → assert price + alert. **Data quality:** Great Expectations gates in the DAG.
- **Production:** GitHub Actions CI per layer; Prometheus metrics + Grafana dashboards; structured
  logging; secrets via env/K8s; MLflow model registry; health checks; idempotent, replayable jobs.

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Resource contention (full stack is heavy) | Compose resource limits; profile-based `up` (core vs full). |
| Contract drift across members | Frozen `contracts/` + PR review by adjacent owners + contract tests. |
| Streaming ↔ batch skew | Redis is the single source of truth for serving; batch writes are atomic per-SKU. |
| LSTM latency in stream | Broadcast model + `pandas_udf` batching; fallback to velocity-only pricing if model unavailable. |

## 9. Out of scope (v1)

Real cloud deployment (manifests only), auth / multi-tenant dashboard, A/B price experimentation,
and real supplier integrations (n8n emails are simulated).
