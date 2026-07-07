# Leader Status — Supply Chain Forecasting & Dynamic Pricing

**Owner:** Nashat (team lead) · **Last updated:** 2026-07-07
**Purpose:** one place to see what's actually done (verified, not just claimed) and what's left.

---

## 1. All 5 plans are complete

Every task across every team member's plan is closed, backed by a merged, CI-green PR.

| Owner | Plan scope | Status |
|---|---|---|
| **Nagy** | Infra: compose stack, bootstrap.sh, Prometheus/Grafana, Helm+Terraform, runbooks | ✅ Complete (extended beyond the original 5 tasks with a fuller infra finalization pass, PR #47/#48) |
| **Nashat** | Contracts, walking-skeleton, pricing formula + psychology, LSTM, streaming pricing job, CI/CD | ✅ Complete |
| **Hatem** | Ingestion: surge schedule, Avro serialization, producer round-trip, verifier, HDFS bronze staging | ✅ Complete (PR #46) |
| **Emad** | Batch/ML: cleanse, features, GE quality gate, MLlib GBT forecast + MLflow, GraphFrames elasticity, Redis publish, Airflow DAG | ✅ Complete (PR #55) |
| **Ziad** | Automation: Redis data source, reorder payload builder, n8n alert bridge, n8n workflow, Streamlit command center | ✅ Complete (PR #43, #53) |

The end-to-end data flow is now real, not placeholder: `pricing_stream.py`'s `baseline`/`elasticity` reads are populated by Emad's Redis publish job, `system_alerts` genuinely reaches the n8n reorder workflow via Ziad's alert bridge, and the Streamlit command center reads live data from both Kafka and Redis.

---

## 2. Bugs found and fixed during final review (verified, not guessed)

A code-review pass across the last few merged PRs turned up 5 real issues, all confirmed by direct reproduction before fixing:

1. **`libs/scf_common/io/kafka.py`** — a refactor to a local `get_settings()` pattern silently dropped the `msg.error()` check in `AvroKafkaConsumer.poll()` (confirmed via `git log -p`, present in the original commit). Genuine Kafka-level errors were falling through to the Avro deserializer and getting misclassified as bad data downstream. Fixed (PR #54).
2. **`ingestion/generator/traffic_generator.py`** — the demo surge-injection tool converts epoch-ms via `datetime.fromtimestamp()` (local timezone) while comparing against naive UTC-intended window bounds, so surges fire off by the host's UTC offset. Confined to demo/test fidelity, not production correctness — filed as **#56**, not yet fixed (low priority).
3. **`automation/dashboard/pages/2_price_ticker.py`** — assumed `event_time` was a raw int; in production it's Avro-decoded to a `datetime`, which would have crashed the Price Ticker page on real data. Fixed (PR #53).
4. **`automation/dashboard/pages/3_automation_log.py`** — same root cause, but silently masked: every real alert's time column rendered `"—"` instead of crashing. Fixed (PR #53).
5. **`automation/n8n/alert_bridge.py`** — non-retryable 4xx responses (bad payload, auth) were retried identically to transient errors. Fixed (PR #53).

This is the third and fourth occurrence this session of the same bug class (Avro `timestamp-millis` decoding to `datetime`, not raw int) — worth a project-wide grep if any other consumer of `event_time`/`*_time` fields gets added in the future.

---

## 3. What's left (all non-blocking technical debt)

- **#36** — LSTM sequence input tiles the current window instead of a true per-SKU historical sequence via `mapGroupsWithState`. Explicitly scoped as a defensible simplification when Task 5 shipped.
- **#41** — `libs/scf_common/io/__init__.py`'s eager imports force `pyspark` to load even for pure-Redis/pure-Kafka helpers. Minor decoupling opportunity, surfaced repeatedly during local (non-CI) verification this session.
- **#56** — timezone bug in the demo surge generator (see above).

No one is blocked. There is no open work tied to a specific person.
