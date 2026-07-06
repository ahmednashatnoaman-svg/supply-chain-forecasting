# Nashat — Speed Engine Plan (Streaming + LSTM + Pricing) + Contracts/CI

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Parent:
> [`../master-plan.md`](../master-plan.md). Milestone: **M3** + owns **contracts/** & **CI/CD**.

**Goal:** Consume live traffic, detect surges with an LSTM, and emit guard-railed dynamic prices in
real time — reading the batch brain's forecasts/elasticity from Redis. Also own the shared contracts
and CI backbone that keep all five layers integrated.

**Architecture:** Spark Structured Streaming reads `live_web_traffic` (Avro), computes sliding-window
velocity, applies an LSTM surge classifier (PyTorch via `pandas_udf`, model from MLflow), then the
pricing formula (velocity × elasticity × forecast, clamped by margin/uplift) and emits
`automated_pricing_updates` + `system_alerts`.

**Tech Stack:** Spark Structured Streaming, PyTorch, MLflow, Redis, Kafka Avro, GitHub Actions.

## Global Constraints
Inherits [master §Global Constraints](../master-plan.md#global-constraints). **Zero-cost:** LSTM runs
locally, no inference API. Pricing tunables from `.env` via `libs/scf_common.config`. Falls back to
velocity-only pricing if the model is unavailable (never blocks the stream).

## File Structure
- `libs/scf_common/contracts/__init__.py` — typed accessors for topics/keys/paths (M0, shared).
- `streaming/pricing/formula.py` — pure pricing math (unit-tested).
- `streaming/lstm/model.py` — LSTM def + train script; `streaming/lstm/infer.py` — pandas_udf.
- `streaming/pipeline/pricing_stream.py` — the Structured Streaming job.
- `streaming/sinks/kafka_sink.py` — Avro writer for pricing/alerts.
- `.github/workflows/ci-*.yml` — CI backbone.
- Tests: `tests/unit/test_formula.py`, `tests/unit/test_lstm_infer.py`,
  `tests/integration/test_pricing_stream.py`, `tests/e2e/test_walking_skeleton.py`.

---

### Task 1 (M0): Typed contract accessors (shared, unblocks everyone)

**Files:** Create `libs/scf_common/contracts/__init__.py`; Test `tests/unit/test_contract_accessors.py`
**Interfaces:** Produces `Topics`, `RedisKeys`, `HdfsPaths` — e.g. `RedisKeys.forecast("10") ->
"forecast:10"`, `Topics.LIVE_WEB_TRAFFIC -> "live_web_traffic"`. **Every layer imports these.**

- [ ] **Step 1: Failing test**
```python
# tests/unit/test_contract_accessors.py
from libs.scf_common.contracts import Topics, RedisKeys, HdfsPaths
def test_keys_and_topics():
    assert Topics.LIVE_WEB_TRAFFIC == "live_web_traffic"
    assert RedisKeys.forecast("10") == "forecast:10"
    assert RedisKeys.elasticity("10") == "graph:elasticity:10"
    assert HdfsPaths.bronze("events").endswith("/data/bronze/events")
```
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** the three classes reading defaults from env (`libs/scf_common.config`).
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `feat(contracts): typed accessors for topics/keys/paths`.

### Task 2 (M0): Walking-skeleton e2e (the integration canary)

**Files:** Create `tests/e2e/test_walking_skeleton.py`; helper `scripts/seed_redis_stub.py`
**Interfaces:** Consumes the whole stack via contracts. This test stays green from M0 onward.

- [ ] **Step 1: Failing test** — seed Redis with a stub forecast, publish one surge event to
  `live_web_traffic`, run the stream in a short-lived mode, assert a message appears on
  `automated_pricing_updates` within `STREAMING_WINDOW_SECONDS`.
- [ ] **Step 2: Run, expect FAIL** (stream not built yet — expected during M0; mark `xfail` until M3
  then flip to required).
- [ ] **Step 3:** provide `seed_redis_stub.py` so downstream members can run the canary.
- [ ] **Step 5: Commit** — `test(e2e): walking-skeleton canary + redis seed`.

### Task 3: Pricing formula (pure, fully unit-tested)

**Files:** Create `streaming/pricing/formula.py`; Test `tests/unit/test_formula.py`
**Interfaces:** Produces
`def dynamic_price(base_price, velocity, baseline, elasticity, is_surge, cfg) -> float` — raises price
when `velocity/baseline > cfg.surge_threshold` and surge, bounded by `[min_margin, max_uplift]`.

- [ ] **Step 1: Failing test**
```python
# tests/unit/test_formula.py
from streaming.pricing.formula import dynamic_price, PricingConfig
CFG = PricingConfig(elasticity_coeff=0.35, max_uplift_pct=0.25, min_margin_pct=0.10, surge_threshold=2.0)
def test_surge_raises_but_capped():
    p = dynamic_price(base_price=100, velocity=500, baseline=100, elasticity=0.8, is_surge=True, cfg=CFG)
    assert 100 < p <= 125          # uplift capped at +25%
def test_no_surge_returns_base():
    assert dynamic_price(100, 90, 100, 0.8, False, CFG) == 100
```
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** the formula with clamping; no I/O, deterministic.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `feat(pricing): guard-railed dynamic price formula`.

### Task 4: LSTM model + inference UDF

**Files:** Create `streaming/lstm/model.py`, `streaming/lstm/infer.py`; Test `tests/unit/test_lstm_infer.py`
**Interfaces:** Produces `def surge_udf(model)` → a `pandas_udf` mapping a sequence column → surge prob;
input/output per `contracts/models/lstm_signature.json`. Trained locally (`python -m streaming.lstm.model`).

- [ ] **Step 1: Failing test** — load a tiny randomly-initialized model, feed a batch of sequences, assert
  output is a probability in `[0,1]` per row and the UDF returns the right length.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** a 1-layer LSTM + sigmoid; `infer.py` broadcasts weights and wraps a
  `pandas_udf`; graceful fallback returns 0.0 if model missing.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `feat(lstm): surge classifier + pandas_udf inference`.

### Task 5: Structured Streaming pricing job (integration)

**Files:** Create `streaming/pipeline/pricing_stream.py`, `streaming/sinks/kafka_sink.py`;
Test `tests/integration/test_pricing_stream.py`
**Interfaces:** Consumes `live_web_traffic`, Redis `forecast:*`/`graph:*`; Produces
`automated_pricing_updates` + `system_alerts`. CLI `python -m streaming.pipeline.pricing_stream`.

- [ ] **Step 1: Failing test** — testcontainers Kafka+Redis; seed forecast; produce a surge burst;
  run stream in `availableNow`/short trigger; assert a price update lands and, on low `inventory:{sku}`,
  an alert lands on `system_alerts`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** readStream(Avro) → window velocity agg → `surge_udf` → join Redis via
  `mapInPandas` → `dynamic_price` → writeStream Avro. Emit metrics via observability lib.
- [ ] **Step 4: Run, expect PASS**, and **flip the M0 walking-skeleton test from `xfail` to required.**
- [ ] **Step 5: Commit** — `feat(streaming): real-time pricing engine end-to-end`.

### Task 6 (M0/M5): CI/CD backbone

**Files:** Create `.github/workflows/ci-lint-type.yml`, `ci-unit.yml`, `ci-integration.yml`,
`ci-no-paid-deps.yml`; `scripts/check_no_paid_deps.py`
**Interfaces:** Produces the CI that gates every PR (free on public repo).

- [ ] **Step 1: Failing test** — `python scripts/check_no_paid_deps.py` exits non-zero if a denylisted
  paid SDK (e.g. `openai`, `databricks-*`, `datadog`) appears in `pyproject.toml`.
- [ ] **Step 2..4:** implement the checker + workflows (path-filtered per layer; integration job boots
  services). PASS.
- [ ] **Step 5: Commit** — `ci: lint/type/unit/integration + zero-cost dependency guard`.
