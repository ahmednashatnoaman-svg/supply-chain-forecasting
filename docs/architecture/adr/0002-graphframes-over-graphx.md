# ADR 0002 — GraphFrames over GraphX; PyTorch local LSTM

**Status:** Accepted · **Date:** 2026-07-06

## Context
The brief names Spark GraphX and "Deep Learning on Spark." GraphX has no PySpark API, and our stack is
Python-first. We also require **zero paid inference**.

## Decision
- Use **GraphFrames** (PySpark-native, Apache-2.0) for the product knowledge graph instead of GraphX.
- Train and serve the LSTM **locally in PyTorch**, invoked inside Spark via `pandas_udf` with broadcast
  weights and MLflow versioning. No external/paid model API.

## Consequences
- ✅ Whole pipeline stays in Python; one language for the team.
- ✅ Zero inference cost; model runs on the same workers.
- ⚠️ `pandas_udf` batching needs tuning for latency. Mitigated by fallback-to-velocity pricing.
- Alternative: TensorFlowOnSpark. Rejected — heavier setup, no cost benefit.
