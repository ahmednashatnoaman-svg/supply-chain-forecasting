# Emad — Batch Brain Plan (ETL + MLlib + GraphFrames)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Parent:
> [`../master-plan.md`](../master-plan.md). Milestone: **M2**. Heaviest data/ML workload.

**Goal:** Nightly, transform HDFS history into per-SKU demand forecasts (MLlib) and a product
cross-elasticity graph (GraphFrames), gate quality with Great Expectations, publish to Redis + MLflow.

**Architecture:** Medallion ETL (bronze→silver→gold) in PySpark. Feature job builds rolling 7d/30d
velocity. MLlib GBT forecasts baseline demand. GraphFrames builds co-purchase graph → PageRank +
communities → elasticity weights. A publisher writes gold → Redis. Airflow orchestrates the DAG.

**Tech Stack:** PySpark SQL/MLlib, GraphFrames, Great Expectations, MLflow, Airflow, Redis.

## Global Constraints
Inherits [master §Global Constraints](../master-plan.md#global-constraints). All paths/keys from
`contracts/`. Models registered in local MLflow (zero-cost). Jobs idempotent & replayable per date.

## File Structure
- `batch/etl/clean_events.py` — bronze→silver cleansing.
- `batch/etl/features.py` — silver→gold rolling velocity features.
- `batch/mllib/forecast.py` — GBT demand forecast → gold + MLflow.
- `batch/graph/elasticity.py` — GraphFrames graph, PageRank, communities → gold.
- `batch/publish/to_redis.py` — gold → Redis serving keys.
- `batch/airflow_dags/nightly_dag.py` + `run_batch_once.py`.
- `tests/unit/test_features.py`, `tests/unit/test_forecast.py`, `tests/unit/test_elasticity.py`,
  `tests/data_quality/test_silver_expectations.py`, `tests/integration/test_publish_redis.py`.

---

### Task 1: Cleanse bronze events → silver (chispa DF test)

**Files:** Create `batch/etl/clean_events.py`; Test `tests/unit/test_clean_events.py`
**Interfaces:**
- Consumes: bronze parquet at `contracts/hdfs` bronze events path; `libs/scf_common.io.spark.get_spark`.
- Produces: `def clean_events(df: DataFrame) -> DataFrame` — drops nulls/dupes, casts `event_time` to
  timestamp, filters events to `{view,addtocart,transaction}`; writes silver.

- [x] **Step 1: Failing test**
```python
# tests/unit/test_clean_events.py
from chispa import assert_df_equality
from batch.etl.clean_events import clean_events
def test_drops_dupes_and_casts(spark):
    raw = spark.createDataFrame(
        [(1609459200000,1,"view",10,None),(1609459200000,1,"view",10,None)],
        ["event_time","visitor_id","event","item_id","price"])
    out = clean_events(raw)
    assert out.count() == 1                       # dedup
    assert dict(out.dtypes)["event_time"] == "timestamp"
```
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Implement `clean_events`** (dropDuplicates, `to_timestamp`, filter, select contract cols).
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(batch): bronze→silver event cleansing`.

### Task 2: Rolling 7d/30d velocity features → gold

**Files:** Create `batch/etl/features.py`; Test `tests/unit/test_features.py`
**Interfaces:** Produces `def build_features(events: DataFrame) -> DataFrame` with columns
`item_id, ds, velocity_7d, velocity_30d, dow, is_weekend` (one row per SKU per day).

- [x] **Step 1: Failing test** — feed 40 days of one SKU's transactions; assert `velocity_7d` on the last
  day equals the mean daily count over the trailing 7 days (compute expected in-test).
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Implement** with a Window partitioned by `item_id` ordered by `ds`, `rowsBetween(-6,0)`
  and `-29,0` averages; add calendar features.
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(batch): rolling velocity feature engineering`.

### Task 3: Great Expectations silver quality gate

**Files:** Create `tests/data_quality/test_silver_expectations.py` + `batch/etl/expectations.py`
**Interfaces:** Produces `def validate_silver(df) -> bool` (raises on failure; used in DAG).

- [x] **Step 1: Failing test** — expect `event_time` not null, `item_id` > 0, `event` in allowed set;
  feed a bad frame → expect raise.
- [x] **Step 2..4:** implement GE suite; PASS on good frame, raise on bad.
- [x] **Step 5: Commit** — `feat(batch): great-expectations silver gate`.

### Task 4: MLlib GBT demand forecast + MLflow registry

**Files:** Create `batch/mllib/forecast.py`; Test `tests/unit/test_forecast.py`
**Interfaces:** Produces `def train_forecast(features: DataFrame) -> (Model, DataFrame)` where the
DataFrame has `item_id, forecast_demand` matching `contracts/models/forecast_output.json`; logs to MLflow.

- [x] **Step 1: Failing test** — train on synthetic linear-trend features; assert output schema matches
  the contract and predictions are finite & non-negative.
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Implement** `VectorAssembler` + `GBTRegressor`, fit, predict, clamp ≥ 0, `mlflow.spark.log_model`.
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(batch): mllib GBT forecast + mlflow`.

### Task 5: GraphFrames cross-elasticity (PageRank + communities)

**Files:** Create `batch/graph/elasticity.py`; Test `tests/unit/test_elasticity.py`
**Interfaces:** Produces `def build_graph(transactions: DataFrame) -> DataFrame` returning edges
`item_id, related_item_id, elasticity_weight` and `def communities(g) -> DataFrame` (`item_id, community`).

- [x] **Step 1: Failing test** — items co-purchased in the same basket get an edge with weight > 0;
  unrelated items have none.
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Implement** basket self-join → co-occurrence counts → GraphFrame; run `pageRank` and
  `labelPropagation`; normalize weights.
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(batch): graphframes cross-elasticity + communities`.

### Task 6: Publish gold → Redis (integration)

**Files:** Create `batch/publish/to_redis.py`; Test `tests/integration/test_publish_redis.py`
**Interfaces:**
- Consumes: forecast + graph gold tables; `libs/scf_common.io.redis_client`.
- Produces: Redis keys `forecast:{sku}`, `graph:elasticity:{sku}`, `graph:community:{sku}` per
  `contracts/redis/redis-keys.md` — **exactly what Nashat's pricing engine reads.**

- [x] **Step 1: Failing test** — with testcontainers Redis, publish a small gold set then assert
  `redis.get("forecast:10")` equals the written value and elasticity is a JSON list.
- [x] **Step 2..4:** implement atomic per-SKU pipeline writes; PASS.
- [x] **Step 5: Commit** — `feat(batch): publish forecasts + elasticity to redis`.

### Task 7: Airflow nightly DAG wiring

**Files:** Create `batch/airflow_dags/nightly_dag.py`, `batch/airflow_dags/run_batch_once.py`;
Test `tests/unit/test_dag_import.py`
**Interfaces:** Produces DAG `nightly_forecast` chaining tasks 1→2→3→4→5→6.

- [x] **Step 1: Failing test** — `DagBag().import_errors == {}` and the DAG has the 6 tasks in order.
- [x] **Step 2..4:** implement DAG + a `run_batch_once` local runner (used by `make batch`); PASS.
- [x] **Step 5: Commit** — `feat(batch): airflow nightly dag + local runner`.
