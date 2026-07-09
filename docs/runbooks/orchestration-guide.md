# Runbook — Full Orchestration Guide

> **Purpose:** one place that explains what every container does, how to watch each one work in
> real time, and what state the stack is actually in. For raw start/stop commands see
> [`startup-shutdown.md`](startup-shutdown.md); for fixing a broken service see
> [`troubleshooting.md`](troubleshooting.md). This doc answers *"what am I looking at, and is it
> actually working?"*

**Snapshot taken:** live-checked against the running stack — every URL below was curled and every
Redis count was scanned, not assumed. Live status will drift after you restart things; re-run the
checks in [§6](#6-how-to-verify-a-service-yourself) if you want fresh numbers.

---

## 1. One-command startup

```bash
make up          # boots all 23 services (--profile full) + runs bootstrap.sh
make ps           # container health
make ingest       # start producing live clickstream traffic (Kafka)
make stream       # start the pricing engine (consumes traffic, writes Redis)
make batch        # run the nightly forecast once (writes forecast + graph to Redis)
make dashboard    # only needed if scf-dashboard-run isn't already up
```

`make up` starts infrastructure only — nothing generates data until you also run `ingest`,
`stream`, and `batch` (or their Docker equivalents, see §3).

---

## 2. What every component does (business view)

| # | Component | Business role | Team owner |
|---|---|---|---|
| 1 | **Traffic generator** | Simulates shoppers browsing/buying (replays the real Retailrocket clickstream at accelerated speed) | Hatem |
| 2 | **Kafka + Schema Registry** | The nervous system — carries every click, price update, and alert between components as schema-validated Avro events | Hatem/Nagy |
| 3 | **HDFS (Namenode/Datanode)** | The data lake — stores raw, cleaned, and feature-engineered history in bronze/silver/gold zones | Nagy |
| 4 | **Spark (Master/Worker)** | The compute engine — runs both the nightly batch forecast and the always-on streaming pricing job | Emad/Nashat |
| 5 | **Batch pipeline** (`run_batch_once.py`) | Nightly job: cleans yesterday's events, computes demand forecast (MLlib) and product-affinity graph (GraphFrames), publishes to Redis | Emad |
| 6 | **Streaming pricing engine** (`pricing_stream.py`) | Always-on: watches live traffic, classifies demand surges (LSTM), computes a new price per SKU every window, checks stock, raises alerts | Nashat |
| 7 | **Redis** | The serving layer — the single source of truth every dashboard/API reads from (current price, velocity, forecast, elasticity graph, inventory) | Nashat |
| 8 | **Airflow** | Schedules the batch pipeline nightly in production (the manual `make batch` bypasses this for dev) | Emad |
| 9 | **MLflow** | Model registry/tracking — stores every trained forecast model + the LSTM surge classifier, versioned | Emad/Nashat |
| 10 | **n8n + alert bridge** | Business automation — when the streaming engine raises a low-stock alert, n8n receives a webhook and (in this zero-cost build) simulates sending a supplier reorder email | Ziad |
| 11 | **Streamlit dashboard** | The human-facing command center — KPIs, forecast-vs-actual, live price ticker, automation log | Ziad |
| 12 | **Prometheus + exporters** | Infrastructure health metrics (Kafka lag, Redis ops/sec, error counters) | Nagy |
| 13 | **Grafana** | Visualizes the Prometheus metrics on the "Platform Overview" dashboard | Nagy |

---

## 3. Every container: port, URL, current live status

Checked just now with `curl` against each URL and `docker ps` for container state — this is what's
actually running, not the intended design.

| Container | Port(s) | URL to view it | Live status right now |
|---|---|---|---|
| `scf-dashboard-run` | 8501 | http://localhost:8501 | ✅ Up 3h, HTTP 200 |
| `scf-airflow-webserver-1` | 8082 | http://localhost:8082 (`admin`/`admin`) | ✅ Up 8h healthy, HTTP 302→/home (normal, means login page) |
| `scf-airflow-scheduler-1` | — (internal) | via Airflow UI | ✅ Up 8h |
| `scf-mlflow-1` | 5000 | http://localhost:5000 | ✅ Up 8h, HTTP 200 |
| `scf-spark-master-1` | 7077 (Spark), 8088 (UI) | http://localhost:8088 | ✅ Up 8h, HTTP 200 |
| `scf-spark-worker-1` | — (internal, 2 cores total) | via Spark Master UI | ✅ Up 8h |
| `scf-namenode-1` | 9000 (RPC), 9870 (UI) | http://localhost:9870 | ✅ Up 8h healthy, HTTP 302 |
| `scf-datanode-1` | — (internal) | via Namenode UI → Datanodes tab | ✅ Up 8h |
| `scf-kafka-1` | 9092 (internal), 29092 (host tools) | n/a (no web UI; use CLI, see §5) | ✅ Up 8h healthy |
| `scf-zookeeper-1` | 2181 (internal) | n/a | ✅ Up 8h healthy |
| `scf-schema-registry-1` | 8081 | http://localhost:8081/subjects | ✅ Up 6h healthy, HTTP 200 |
| `scf-redis-1` | 6379 | n/a (use `redis-cli`, see §5) | ✅ Up 8h healthy |
| `scf-n8n-1` | 5678 | http://localhost:5678 | ✅ Up 8h, HTTP 200 |
| `scf-prometheus-1` | 9090 | http://localhost:9090/targets | ✅ Up 8h, HTTP 302 |
| `scf-grafana-1` | 3001→3000 | http://localhost:3001 (`admin`/`admin`) | ✅ Up 8h, HTTP 302 |
| `scf-redis-exporter-1` | 9121 | http://localhost:9121/metrics | ✅ Up 8h |
| `scf-kafka-exporter-1` | 9308 | http://localhost:9308/metrics | ✅ Up 6h |
| `scf-pricing-stream-run` | — | logs only (`docker logs -f`) | 🔴 **Stopped** (exit 137 — deliberately killed to free the 2-core Spark worker for a batch run; needs restart, see §7) |
| `scf-batch-pipeline-run` | — | logs only | 🔴 **Crashed** (exit 1 — hit the now-fixed `label does not exist` bug; the fix is in the code but the image hasn't been rebuilt yet, see §7) |

**Note on ports 3000/3001 and 8080/8088/8082:** Grafana's internal port 3000 is remapped to host
3001, and both Spark Master and Airflow internally use 8080, remapped to 8088 and 8082
respectively — this avoids the three services fighting over the same host port. Always use the
host-side port from this table, not the container's internal port.

---

## 4. Data freshness — is there actually data flowing?

Scanned Redis directly (`redis-cli --scan`) rather than trusting any UI:

| Redis key pattern | What it means | Count right now | Verdict |
|---|---|---|---|
| `price:current:*` | Live price per SKU, written every streaming window | 3,811 | ✅ Real, populated by the streaming run before it was stopped |
| `velocity:*:60s` | Rolling 60s transaction velocity per SKU | 3,811 | ✅ Real |
| `forecast:*` | Nightly MLlib demand forecast per SKU | **0** | 🔴 Empty — no batch run has completed successfully yet |
| `inventory:*` | Live stock level per SKU | **0** | 🔴 Empty — see gap below |

**Known gap (not yet fixed):** nothing in the real pipeline ever writes `inventory:*`. The only
writer anywhere in the codebase is the manual `scripts/seed_redis_stub.py` (hardcodes 5 SKUs to
500 units each) — it hasn't even been run since the last Redis restart, hence the zero count. This
means:
- `check_low_stock_alert()` in `streaming/pipeline/pricing_stream.py` can never fire in the live
  system (it always reads `None`).
- The n8n auto-reorder workflow (§2, row 10) will never actually trigger from real traffic.
- The dashboard's low-stock KPI has no real backing data.

The Retailrocket dataset's own `item_properties_part1/2.csv` (already staged in HDFS bronze —
confirmed via `hdfs dfs -ls /data/bronze`) contains a real, time-varying `available` (0/1) field
per item, which is the correct real source for this — it's just never been wired into
`build_features` or the batch publish step. This is the next fix planned for the pipeline, not yet
implemented.

---

## 5. How to watch each layer live

### Streaming pricing engine (the "is it happening right now" view)
```bash
docker logs -f scf-pricing-stream-run          # structured JSON logs, one line per micro-batch
```
Or watch Redis values change in real time without touching logs:
```bash
watch -n 2 'docker exec scf-redis-1 redis-cli GET price:current:10'
```
Or read the raw Kafka topic the engine produces to:
```bash
docker exec scf-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic automated_pricing_updates --from-beginning --max-messages 10
```

### Traffic generator
```bash
docker logs -f scf-traffic-generator-run   # or: docker exec scf-kafka-1 kafka-run-class \
                                            #   kafka.tools.GetOffsetShell --broker-list localhost:9092 \
                                            #   --topic live_web_traffic
```

### Batch pipeline
```bash
docker logs -f scf-batch-pipeline-run
```
Also visible as Spark jobs at http://localhost:8088 while running.

### n8n automation
Open http://localhost:5678 → **Executions** tab shows every webhook the alert bridge has posted,
with the exact reorder payload (SKU, qty, simulated email body).

### Grafana
http://localhost:3001 → **Platform Overview** dashboard (Kafka consumer lag, Redis ops/sec,
container-level error counters from Prometheus).

### Streamlit dashboard
http://localhost:8501 — landing page auto-refreshes KPIs every 5s
(`st.fragment(run_every="5s")`); **Price Ticker** and **Automation Log** pages read directly off
the live Kafka topics rather than Redis, so they show events the instant they're produced.

---

## 6. How to verify a service yourself

Don't trust this table forever — re-run these to get current truth:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
for p in 8501 8082 5000 8088 9870 8081 9090 3001 5678; do
  echo "$p -> $(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:$p/)"
done
docker exec scf-redis-1 redis-cli --scan --pattern "price:current:*" | wc -l
```

---

## 7. Pending actions to get to a fully green state

1. **Rebuild the batch pipeline image** — `run_batch_once.py`, `features.py`, and `forecast.py`
   were fixed (missing `label` column, wrong bronze read format) but the running image predates
   the fix. Rebuild + rerun `make batch` (or the container equivalent).
2. **Restart streaming** — `scf-pricing-stream-run` was intentionally stopped to free the
   single 2-core Spark worker for the batch job. Restart it once batch finishes, or raise
   `spark-worker` core count in `docker-compose.yml` so both can run concurrently.
3. **Wire real inventory data** — see the gap in §4. Needs `item_properties` (`available` field)
   read into a Redis-writing step, either in the batch publish stage or a small dedicated job.
4. Commit and push the batch-pipeline fixes (currently only applied locally, not yet committed).

---

## Related docs

- [`startup-shutdown.md`](startup-shutdown.md) — start/stop commands, full service URL table
- [`troubleshooting.md`](troubleshooting.md) — fixing a broken service
- [`../architecture/system-design.md`](../architecture/system-design.md) — full architecture design
- [`../architecture/data-contracts.md`](../architecture/data-contracts.md) — Kafka/Redis/HDFS contracts
