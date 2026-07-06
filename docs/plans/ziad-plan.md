# Ziad — Automation & Serving Plan (n8n + Streamlit)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Parent:
> [`../master-plan.md`](../master-plan.md). Milestone: **M4**.

**Goal:** Turn the platform's live outputs into business action: an n8n workflow that auto-drafts a
purchase order on low-stock alerts, and a Streamlit "command center" visualizing forecast-vs-actual,
a price ticker, and the automation log.

**Architecture:** A Kafka consumer bridges `system_alerts` → n8n webhook, which composes a simulated
supplier email (zero-cost). Streamlit reads Redis (`price:current:*`, `forecast:*`, `inventory:*`) and
tails `automated_pricing_updates` for the live ticker.

**Tech Stack:** n8n, Streamlit, Plotly, Redis, confluent-kafka.

## Global Constraints
Inherits [master §Global Constraints](../master-plan.md#global-constraints). All keys/topics from
`contracts/`. n8n email is **simulated/logged** or free SMTP — never a paid service.

## File Structure
- `automation/n8n/reorder_workflow.json` — importable n8n workflow.
- `automation/n8n/alert_bridge.py` — Kafka `system_alerts` → n8n webhook.
- `automation/dashboard/app.py` — Streamlit entrypoint.
- `automation/dashboard/pages/{1_forecast_vs_actual.py,2_price_ticker.py,3_automation_log.py}`.
- `automation/dashboard/components/redis_source.py` — cached Redis reads.
- Tests: `tests/unit/test_redis_source.py`, `tests/integration/test_alert_bridge.py`,
  `tests/unit/test_reorder_payload.py`.

---

### Task 1: Redis data source with caching (unit-tested)

**Files:** Create `automation/dashboard/components/redis_source.py`; Test `tests/unit/test_redis_source.py`
**Interfaces:**
- Consumes: `libs/scf_common.io.redis_client`, `libs/scf_common.contracts.RedisKeys`.
- Produces: `def get_price(sku)->float`, `def get_forecast(sku)->float`, `def get_inventory(sku)->int`.

- [ ] **Step 1: Failing test** — with a fake Redis (fakeredis, free), set `price:current:10` and assert
  `get_price("10")` returns it; missing key returns `None`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** typed getters using `RedisKeys`; add `st.cache_data`-friendly plain funcs.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `feat(dashboard): cached redis data source`.

### Task 2: Reorder payload builder (pure logic)

**Files:** Create `automation/n8n/alert_bridge.py` (payload fn first); Test `tests/unit/test_reorder_payload.py`
**Interfaces:** Produces `def build_reorder(alert: dict) -> dict` → `{sku, qty, supplier_email,
subject, body}` where `qty` refills to target stock.

- [ ] **Step 1: Failing test** — given an alert with `inventory=5, reorder_threshold=50,
  target=200`, assert `qty == 195` and email fields present.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** `build_reorder`.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `feat(automation): reorder payload builder`.

### Task 3: Alert bridge → n8n webhook (integration)

**Files:** Modify `automation/n8n/alert_bridge.py`; Test `tests/integration/test_alert_bridge.py`
**Interfaces:** Consumes `system_alerts` (Avro); posts `build_reorder(...)` JSON to `N8N_WEBHOOK_URL`.

- [ ] **Step 1: Failing test** — testcontainers Kafka + a stub HTTP server; produce a low-stock alert;
  assert the stub receives one POST with the expected `qty`.
- [ ] **Step 2..4:** implement consumer loop + `requests.post`; retry/backoff; PASS.
- [ ] **Step 5: Commit** — `feat(automation): system_alerts → n8n reorder bridge`.

### Task 4: n8n reorder workflow (importable, simulated email)

**Files:** Create `automation/n8n/reorder_workflow.json`; Test `tests/unit/test_workflow_valid.py`
**Interfaces:** Webhook trigger → Set node → (simulated) Email/Log node.

- [ ] **Step 1: Failing test** — load the JSON and assert it has a `webhook` trigger node and an email
  node, and is valid JSON.
- [ ] **Step 2..4:** author the workflow JSON (email node configured to a free SMTP or "no-op log"). PASS.
- [ ] **Step 5: Commit** — `feat(automation): n8n reorder workflow (zero-cost email)`.

### Task 5: Streamlit command center

**Files:** Create `automation/dashboard/app.py` + the 3 pages; Test `tests/unit/test_dashboard_smoke.py`
**Interfaces:** Reads Redis + tails `automated_pricing_updates`. `make dashboard` launches it.

- [ ] **Step 1: Failing test** — import each page module and call its `render()` with a fake Redis;
  assert no exception and that it produced at least one Plotly figure object.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** `app.py` (nav) + pages: forecast-vs-actual line, live price ticker, automation
  log table. Keep data access in `redis_source` (testable), rendering thin.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `feat(dashboard): streamlit command center with 3 views`.
