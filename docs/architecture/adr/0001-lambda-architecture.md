# ADR 0001 — Lambda architecture (batch + speed + serving)

**Status:** Accepted · **Date:** 2026-07-06

## Context
We need both accurate long-horizon demand forecasts (heavy, periodic) and sub-minute price reactions to
live traffic (light, continuous). These have opposite latency/throughput profiles.

## Decision
Adopt a **Lambda architecture**: a nightly **batch layer** (MLlib forecast + GraphFrames elasticity)
publishes to a **serving layer** (Redis); a continuous **speed layer** (Structured Streaming + LSTM)
reads the serving layer and emits prices. Redis is the single serving source of truth.

## Consequences
- ✅ Each layer scales and fails independently; clean team ownership boundaries.
- ✅ Streaming stays fast by reading precomputed baselines from Redis.
- ⚠️ Two code paths (batch vs stream). Mitigated by shared `contracts/` + `libs/scf_common`.
- Alternative considered: Kappa (stream-only). Rejected — GBT/graph training is inherently batch.
