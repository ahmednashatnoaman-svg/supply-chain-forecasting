# Master Implementation Plan — Supply Chain Forecasting & Dynamic Pricing

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or
> superpowers:executing-plans to implement the per-member plans task-by-task. This master plan is the
> **orchestrator** — it defines contracts, milestones, the dependency graph, and shared standards.
> The five member plans live in [`docs/plans/`](plans/).

**Goal:** Ship a production-grade Lambda-architecture platform that forecasts demand nightly and adjusts
prices in real time, built in parallel by 5 members without integration debt.

**Architecture:** Batch (Emad) trains forecasts + elasticity → publishes to Redis. Speed (Nashat)
streams live traffic through an LSTM + pricing formula → emits price updates. Infra (Nagy) runs the
cluster + monitoring. Ingestion (Hatem) feeds Kafka. Automation (Ziad) serves dashboards + n8n reorder.

**Tech Stack:** Spark 3.5 (SQL/MLlib/GraphFrames/Structured Streaming) · Kafka 3.x + Schema Registry ·
Hadoop 3.3 HDFS · Redis 7 · PyTorch · MLflow · Airflow · Great Expectations · n8n · Streamlit ·
Prometheus/Grafana · Docker Compose (dev) / Helm+Terraform (cloud) · GitHub Actions.

## Global Constraints

- **ZERO-COST / FREE & OPEN-SOURCE ONLY.** No paid APIs, no paid models, no paid SaaS, no billed cloud
  resources. Every component is self-hosted OSS running in local Docker. CI uses GitHub Actions'
  **free unlimited minutes for public repos**. The LSTM is trained & served **locally in PyTorch** (no
  inference API). Supplier emails from n8n are **simulated/logged or sent via a free SMTP tier**. Cloud
  is **manifests-only** (Helm/Terraform are never `apply`-ed against a paid provider). See
  [`docs/reference/cost-and-licensing.md`](reference/cost-and-licensing.md). Any new dependency MUST be
  free/OSS — if it needs a credit card, it is rejected.
- Python **>= 3.10**; PySpark **== 3.5.1**; all versions pinned in `pyproject.toml` (copy verbatim).
- Every cross-layer interface MUST come from `contracts/`. No hard-coded topic names, Redis keys, or
  HDFS paths in layer code — import them from `libs/scf_common/contracts`.
- Kafka payloads are **Avro**, registered in Schema Registry. Redis keys follow `contracts/redis/`.
- Every task is **TDD**: failing test → minimal code → passing test → commit. No task merges red.
- Definition of Done (per task): tests pass, `ruff`+`black`+`mypy` clean, docstring, metric emitted
  where relevant, plan checkbox ticked.
- Commit style: Conventional Commits, one logical change per commit, sign-off
  `Co-Authored-By: Claude <noreply@anthropic.com>` when AI-assisted.

---

## 1. The four frozen contracts (built in Milestone 0, owned collectively)

These are the **only** integration surfaces. Changing one is a PR reviewed by both adjacent owners.

| Contract | Location | Producer → Consumer |
|---|---|---|
| Kafka topics + Avro schemas | `contracts/avro/` | Hatch/Nashat produce → Nashat/Ziad consume |
| Redis key schema | `contracts/redis/redis-keys.md` | Emad/Nashat write → Nashat/Ziad read |
| HDFS medallion paths + Parquet schemas | `contracts/hdfs/` | Hatem stage → Emad transform |
| Model signatures (MLlib + LSTM) | `contracts/models/` | Emad/Nashat train → Nashat serve |

## 2. Milestone timeline (Contract-First Parallel + Walking Skeleton)

| Milestone | Name | Exit criteria | Primary owners |
|---|---|---|---|
| **M0** | Contracts + Walking Skeleton | `contracts/` frozen; `make up` runs; one fake event flows end-to-end through stubs and lands a stub price on `automated_pricing_updates`; `make smoke` green | All (Nagy leads infra, Nashat leads contracts) |
| **M1** | Ingestion real | Traffic generator replays Retailrocket with surges; consumer verifier passes; data staged to HDFS bronze | Hatem (+Nagy) |
| **M2** | Batch brain | ETL→features→MLlib forecast→GraphFrames elasticity→Redis publish; Airflow DAG green; GE data-quality gates pass | Emad |
| **M3** | Speed engine | Structured Streaming computes velocity, LSTM surge flag, pricing formula reads Redis, emits updates + alerts | Nashat |
| **M4** | Serving + automation | Streamlit dashboard live; n8n reorder on low-stock alert | Ziad |
| **M5** | Production hardening | Prometheus/Grafana dashboards; CI green on all layers; runbooks; Helm/Terraform stubs; load/e2e tests | Nagy + Nashat (+all) |

Members M1–M4 run **in parallel** against M0 mocks; integration happens continuously at the contracts.

## 3. Cross-plan dependency graph

```
                 ┌─────────────── M0: contracts + skeleton (ALL) ───────────────┐
                 │                                                                │
        Nagy(infra) ──provides cluster──▶ everyone                                │
        Hatem(ingest) ──live_web_traffic + HDFS bronze──▶ Nashat, Emad            │
        Emad(batch) ──Redis: forecast:*, graph:*──▶ Nashat                        │
        Nashat(speed) ──automated_pricing_updates, system_alerts──▶ Ziad          │
        Ziad(serving) ──dashboard + n8n──▶ business                               │
                 └────────────────────────────────────────────────────────────────┘
```

**Blocking edges:** Emad's Redis publish (M2) blocks Nashat's full pricing (M3). Mitigation: Nashat
codes against a Redis **seed script** (`scripts/seed_redis_stub.py`, delivered in M0) so he is never
idle. Hatem's topic schema (M0) blocks both Nashat and Emad's stream reads — hence M0 freezes it first.

## 4. Integration checkpoints (definition of "integrated")

1. **Contract test suite** (`tests/integration/test_contracts.py`): asserts every Avro schema loads,
   every Redis key pattern parses, every HDFS path resolves. Runs in CI on every PR.
2. **Walking-skeleton e2e** (`tests/e2e/test_walking_skeleton.py`): the canary that proves all five
   layers still talk. Must stay green from M0 onward.
3. **Weekly integration sync:** each owner demos their layer reading/writing real contract data.

## 5. Shared engineering standards (apply to all 5 plans)

- **Testing pyramid:** unit (chispa) → integration (testcontainers) → e2e → data-quality (GE).
- **Config:** `libs/scf_common/config` loads `.env` via pydantic settings; no `os.getenv` scattered.
- **Observability:** `libs/scf_common/observability` exposes `get_logger()` (structlog) and a
  Prometheus `metrics` registry; every job emits `records_processed`, `errors_total`, `latency_seconds`.
- **IO:** `libs/scf_common/io` wraps Redis + Kafka (Avro) + Spark session builders. Layers never
  instantiate raw clients.
- **CI:** `.github/workflows/ci-<layer>.yml` runs lint+type+unit on path-filtered changes; a shared
  `ci-integration.yml` runs contract + e2e tests.

## 6. RACI (who is Responsible / Accountable / Consulted / Informed)

| Area | R | A | C | I |
|---|---|---|---|---|
| Contracts | Nashat | Nashat | Emad, Hatem | Nagy, Ziad |
| Infra/cluster | Nagy | Nagy | Nashat | all |
| Ingestion | Hatem | Hatem | Nagy | Emad, Nashat |
| Batch/ML | Emad | Emad | Nashat | Ziad |
| Speed/pricing | Nashat | Nashat | Emad | Ziad |
| Serving/automation | Ziad | Ziad | Nashat | all |
| CI/CD + release | Nashat | Nagy | all | all |

## 7. Member plans

| Plan | Owner | Milestone focus |
|---|---|---|
| [nagy-plan.md](plans/nagy-plan.md) | Nagy | M0, M5 — infra & monitoring |
| [hatem-plan.md](plans/hatem-plan.md) | Hatem | M1 — ingestion & simulation |
| [emad-plan.md](plans/emad-plan.md) | Emad | M2 — batch brain |
| [nashat-plan.md](plans/nashat-plan.md) | Nashat | M3 — speed engine + contracts/CI |
| [ziad-plan.md](plans/ziad-plan.md) | Ziad | M4 — serving & automation |

## 7a. Financial & pricing-strategy workstream

The pricing engine is the system's commercial core, so it gets an explicit financial workstream layered
on the speed/serving layers (built with the pricing-strategy, price-psychology, and financial-modeling
disciplines). All zero-cost.

| Piece | File / doc | Owner | Consulted |
|---|---|---|---|
| Value-based pricing math + guardrails | `streaming/pricing/formula.py` | Nashat | Emad (elasticity) |
| Honest price presentation (charm + anchoring) | `streaming/pricing/psychology.py` | Nashat | Ziad |
| Pricing strategy doc | [`docs/reference/pricing-strategy.md`](reference/pricing-strategy.md) | Nashat | Ziad |
| Financial model / ROI business case | [`docs/reference/financial-model.md`](reference/financial-model.md) | Ziad | Nashat |
| Optional supplier-payment reconciliation (Plaid Sandbox, free) | `automation/finance/plaid_reconciliation.py` | Ziad | — |

**Ethics rule (non-negotiable):** presentation never charges more than the computed price and never
fabricates a "was" anchor or countdown. See pricing-strategy doc §5.

## 8. Risks (see spec §8) — orchestration-level mitigations

- **Idle-waiting:** M0 stubs + seed scripts unblock every downstream member before upstream is real.
- **Contract drift:** frozen `contracts/` + contract tests in CI + adjacent-owner PR review.
- **Resource contention:** `make up` supports `PROFILE=core` (Kafka+Redis+Spark) vs `PROFILE=full`.
