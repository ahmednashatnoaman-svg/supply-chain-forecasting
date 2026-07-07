# Leader Status — Supply Chain Forecasting & Dynamic Pricing

**Owner:** Nashat (team lead) · **Last updated:** 2026-07-07
**Purpose:** one place to see what's actually done (verified, not just claimed), what's open, and
which open task to hand out next — ranked by RICE (Reach × Impact × Confidence ÷ Effort), not plan
order.

---

## 1. What's genuinely done

All items below are closed on GitHub with a merged, CI-green PR behind them (or, for Nagy's infra
tasks, closed by Nagy directly against real local verification — see note).

| Owner | Plan | Status |
|---|---|---|
| **Nagy** | Tasks 1–5 (compose stack, bootstrap.sh, Prometheus/Grafana, Helm+Terraform stub, runbooks) | ✅ All closed independently by Nagy (`mohamed-nagy11`) — not routed through this session. Real files exist (`infra/helm/`, `infra/terraform/`, `docs/runbooks/`), with follow-up commits fixing port conflicts and volume permissions. |
| **Nashat** | Tasks 1–6 (contracts, walking-skeleton, pricing formula, price psychology, LSTM, streaming pricing job, CI/CD) | ✅ All closed. Task 5 (#23) and Task 2 (#19) closed this session after the real testcontainers e2e (PR #38) proved the merged `pricing_stream.py` (PR #37) actually produces a price update + LOW_STOCK alert against **real** Kafka + Redis. |
| **Emad** | Tasks 1–2 (bronze→silver cleanse, rolling velocity features) | ✅ Closed (#11, #12). |
| **Hatem** | Tasks 1–2 (surge schedule model, Avro serialization) | ✅ Closed (#6, #7, PR #42 merged this session). |
| **Ziad** | Tasks 1, 2, 4 (Redis data source, reorder payload builder, n8n workflow JSON) | ✅ Closed (#25, #26, #28 — PR #43 merged this session). |

**Bugs found and fixed this session** (real bugs, each verified from an actual failing log or
direct execution — never guessed): Avro `logicalType` nesting (silently dropped timestamps),
`datetime.UTC` on Python 3.10, a `structlog` API typo, a missing `spark-sql-kafka-0-10` package in
the shared test fixture, and three of my own test assertions that assumed charm-rounding never
rounds *below* an unchanged price (it does, by design — see `streaming/pricing/psychology.py`).

---

## 2. What's open, ranked by RICE

"Ready" means every upstream dependency is merged — someone could start today with no blockers.

### 🥇 #27 — Ziad Task 3: Alert bridge → n8n webhook (integration)
**Ready now.** Reach: high — closes the last gap in the full alert-to-reorder loop. Impact: high —
both endpoints it connects already exist and are proven (`system_alerts` is now really produced by
the merged pricing job; the n8n workflow JSON it posts to just merged in #43). Confidence: high —
fully specified, both integration points tested independently. Effort: medium (~1 day, same
testcontainers-Kafka + stub-HTTP-server pattern already proven twice this session).
**Why #1:** highest reach-to-effort ratio of anything open — nothing else is this close to being
"just wiring" between two already-working pieces.

### 🥈 #8 — Hatem Task 3: Producer round-trips through real Kafka (integration)
**Ready now** (Task 2 merged this session). Reach: high — validates the ingestion layer end-to-end
and unblocks #9 and #10. Impact: high — this is the production topology proof for the layer every
downstream consumer (Nashat's streaming job, Emad's batch reads) depends on. Confidence: high.
Effort: medium.

### 🥉 #13 — Emad Task 3: Great Expectations silver quality gate
**Ready now** (Tasks 1–2 merged). Reach: high — this is the start of a 4-task sequential chain
(#13 → #14 → #15 → #16 → #17); nothing after it can start in parallel, so it's the critical-path
bottleneck for the entire batch/ML layer. Impact: high (bad silver data silently corrupts every
downstream forecast and price). Confidence: high (Great Expectations is a mature OSS tool).
Effort: medium. **Recommend starting this immediately — it's gating the most valuable work Emad
has left.**

### #14 — Emad Task 4: MLlib GBT demand forecast + MLflow registry
Blocked on #13. Reach/Impact: **critical** — until this and #16 ship, `pricing_stream.py`'s
`baseline` reads Redis key `forecast:{sku}`, finds nothing, and defaults to `0.0`
(`streaming/pipeline/pricing_stream.py:135`), which disables surge detection entirely
(`is_surge` requires `baseline > 0`). **The merged, CI-verified pricing engine is currently running
on a placeholder in any real deployment** until this lands. Confidence: medium (model tuning is
iterative). Effort: high (multi-day).

### #16 — Emad Task 6: Publish gold → Redis (integration)
Blocked on #14 and #15. The plan doc says it outright: this produces "**exactly what Nashat's
pricing engine reads**" (`docs/plans/emad-plan.md` Task 6). Once #14/#15 land, this is the one task
that actually turns on real forecasting and elasticity in production — prioritize it the moment its
blockers clear.

### #15 — Emad Task 5: GraphFrames cross-elasticity (PageRank + communities)
Can run in parallel with #14 (no dependency between them). Reach/Impact: medium — only affects the
cross-SKU elasticity term in the pricing formula, not core surge detection (defaults to `0.0`
harmlessly if missing). Lower urgency than #14. Effort: high.

### #10 — Hatem Task 5: Data staging to HDFS bronze
⚠️ **Flag for verification, not just prioritization:** Emad's Task 1 (#11, "cleanse bronze→silver")
is already closed, but Hatem's Task 5 — the task that actually stages real data *into* bronze — is
still open. Worth confirming with Emad whether Task 1 was verified against Hatem's real staged
output or a sample/synthetic bronze dataset. If the latter, Task 1 may need re-validation once #10
ships.

### #9 — Hatem Task 4: Verifier detects loss/lag
Blocked on #8. Reach/Impact: medium-high (production data-loss monitoring). Confidence: medium
(lag thresholds need a decision). Effort: medium.

### #17 — Emad Task 7: Airflow nightly DAG wiring
Blocked on #13–#16. Needed for scheduled production runs, not for correctness. Effort: medium.

### #29 — Ziad Task 5: Streamlit command center
No hard blockers, but lower reach than #27 — this is stakeholder/ops visibility, not a data-flow
gap. Good next task for Ziad after #27. Effort: medium-high (UI work).

### Tracked technical debt (non-blocking, low urgency)
- **#36** — LSTM sequence input tiles the current window instead of a true per-SKU historical
  sequence via `mapGroupsWithState`. Filed as a known, defensible simplification.
- **#41** — `libs/scf_common/io/__init__.py`'s eager imports force `pyspark` to load even for
  pure-Redis helpers. Minor decoupling opportunity.

---

## 3. One-line recommendation per person

- **Nagy:** infra plan complete — free to help elsewhere or harden monitoring/alerting further.
- **Nashat:** plan complete — available to unblock others or pick up #41/#36.
- **Hatem:** take **#8** next (fully unblocked, unlocks #9 and clarifies #10).
- **Emad:** take **#13** next — it's the single task blocking the most downstream work (4 tasks
  behind it), and until #14+#16 land, production pricing runs on a `0.0` forecast placeholder.
- **Ziad:** take **#27** next — highest RICE score in the entire open backlog, pure wiring between
  two already-proven pieces.
