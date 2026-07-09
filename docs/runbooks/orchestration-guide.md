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
| `scf-pricing-stream-run` | — | logs only (`docker logs -f`) | ✅ Always-on; restart after any `spark-worker` recreate (breaks its Spark RPC session — not a bug, just how Spark Standalone handles a worker replacement) |
| `scf-batch-pipeline-run` | — | logs only | ✅ One-shot; Prometheus correctly shows it "down" between runs. Exits 0 on success — check `docker inspect --format '{{.State.ExitCode}}'` |
| `scf-traffic-generator-run` | — | logs only | ✅ One-shot/long-running depending on `generator.yaml` `limit`; needs `data/raw` mounted (see §7 gotchas) |

**Note on ports 3000/3001 and 8080/8088/8082:** Grafana's internal port 3000 is remapped to host
3001, and both Spark Master and Airflow internally use 8080, remapped to 8088 and 8082
respectively — this avoids the three services fighting over the same host port. Always use the
host-side port from this table, not the container's internal port.

---

## 4. Data freshness — is there actually data flowing?

Scanned Redis directly (`redis-cli --scan`) rather than trusting any UI:

| Redis key pattern | What it means | Count right now | Verdict |
|---|---|---|---|
| `price:current:*` | Live price per SKU, written every streaming window | 6,927 | ✅ Real, written by the streaming engine every window |
| `velocity:*:60s` | Rolling 60s transaction velocity per SKU | 6,927 | ✅ Real |
| `forecast:*` | Nightly MLlib demand forecast per SKU | **10,083** | ✅ Real — batch run completed clean end-to-end |
| `inventory:*` | Live stock level per SKU | **9,786** | ✅ Real — derived from Retailrocket's own `item_properties` `available` signal |
| `elasticity:*` / `community:*` | Cross-product co-purchase graph | populated after the `publish_forecasts` SKU-union fix (see below) | ✅ Fixed |

**All four gaps above are now fixed** (previously this section documented them as broken):
1. **`forecast:*`/`inventory:*` were 0** — traced to `build_features` never producing the `label`
   column `train_forecast` needed, and nothing writing inventory at all. Fixed in
   `batch/etl/features.py`, `batch/mllib/forecast.py`, and the new `batch/etl/inventory.py`
   (derives real stock from `item_properties`' `available` field, already staged in HDFS bronze).
2. **`build_graph` OOM'd on the real ~2.2M-row dataset** — root cause wasn't insufficient memory
   (two rounds of Spark-worker memory bumps didn't fix it) but a real data-contract bug:
   `cooccurrence_edges` self-joins on `visitor_id` and its own docstring says it expects
   transaction-only rows, but `run_batch_once` was passing it the *entire* silver events table
   (views + addtocart + transactions). Fixed by filtering to transactions first.
3. **`elasticity:*`/`community:*` stayed empty even after the graph itself had real data** —
   `publish_forecasts` only looped over `forecast_df`'s SKUs (each item's *latest* unlabeled day
   only — a much smaller set), so it silently never reached most of the graph's item_ids. Fixed by
   unioning all four data sources (forecast/elasticity/community/inventory) before looping, so
   every SKU gets whichever data actually exists for it.

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

## 7. Operational gotchas learned the hard way

**Docker's data lives on an external volume — if it's unmounted, Docker Desktop won't start.**
`~/Library/Group Containers/group.com.docker/settings-store.json` has `DataFolder` pointed at
`/Volumes/SCFWork/docker-data` (moved there during an earlier disk-space crisis on the main boot
drive). If that external volume gets ejected/unmounted for any reason, Docker Desktop fails with
either a generic mount permission error or `"Invalid virtual machine configuration"`. Fix:
```bash
diskutil list                                          # find the disk (look for "SCFWork")
hdiutil attach "/path/to/scf-work.sparseimage"          # cleanly re-attach + auto-mount
```
If Docker's own VM disk (`Docker.raw` inside that DataFolder) is itself corrupted — a genuinely
invalid config, not just an unmounted volume — the only reliable fix found was deleting that one
file and letting Docker Desktop recreate it fresh (equivalent to "Reset to factory defaults" when
using a custom data location): `rm /Volumes/SCFWork/docker-data/Docker.raw`, then relaunch. This
wipes all images/containers/volumes — expect to rebuild everything.

**`traffic-generator`'s `data/raw` mount used to be a symlink to that same external volume** —
converted to real local files in the repo (`data/raw/*.csv`) specifically because Docker's mount
namespace can't follow a host-side symlink out to a separate volume; only bind-mounting the
*real* target path works.

**BuildKit can wedge under sustained heavy load** (long Spark image builds + concurrent containers
running for hours) — symptoms: `docker build` hangs indefinitely with zero CPU usage, but
`docker ps`/`docker logs` on existing containers still work fine. Workaround: `DOCKER_BUILDKIT=0
docker build ...` (legacy builder, bypasses the wedged daemon-side BuildKit session entirely)
rather than immediately restarting Docker Desktop.

**Docker Hub anonymous pull rate limits** — rebuilding the whole stack from a fresh/wiped Docker
Desktop VM re-pulls every base image at once and can exhaust the ~100-pulls/6h anonymous limit,
surfacing as `UNAUTHORIZED: authentication required` on totally public images. Fix: `docker login`
with a real account (in your own terminal — never paste credentials into an agent session).

**Grafana's `GF_SECURITY_ADMIN_PASSWORD` only applies on first-ever startup** — once its SQLite DB
exists (on the persisted `grafana-data` volume), the env var is ignored on every subsequent
restart. To reset a forgotten/stale password: stop the container, then run the CLI against the
*same* volume with the server not running (it needs an exclusive DB lock):
```bash
docker stop scf-grafana-1
docker run --rm --entrypoint grafana-cli -v scf_grafana-data:/var/lib/grafana \
  grafana/grafana:11.1.0 admin reset-admin-password <newpassword>
docker start scf-grafana-1
```

**Grafana's dashboard/datasource provisioning silently does nothing if the YAML sits at the wrong
depth** — Grafana only reads `provisioning/dashboards/*.yml` and `provisioning/datasources/*.yml`
(one level deeper than you'd guess); a `dashboards.yml` sitting directly in `provisioning/` is
never read, with only an easy-to-miss log line (`can't read dashboard provisioning files from
directory`) as the tell.

## 8. Pending actions to reach a fully green state

- [x] ~~Rebuild the batch pipeline image~~ — done, verified end-to-end (exit 0, 10,083 SKUs
      published, forecast/inventory/elasticity/community all populated).
- [x] ~~Restart streaming~~ — done; note it needs restarting again after any `spark-worker`
      recreate (raising cores/memory requires recreating the container, which drops the
      streaming job's active Spark session).
- [x] ~~Wire real inventory data~~ — done via `batch/etl/inventory.py`.
- [x] ~~Fix `build_graph` and `publish_forecasts`~~ — done (§4).
- [ ] **Re-push the 4 Docker Hub images** — `ahmednashat1/scf-{dashboard,pricing-stream,
      batch-pipeline,traffic-generator}` were pushed once, but multiple fixes have landed since
      (transaction-filter graph fix, publish SKU-union fix, `serve_metrics` wiring). Rebuild
      locally and `docker push` all 4 again once the current full-stack verification pass
      completes.
- [ ] Run `make smoke` / the full test suite one more time post-rebuild to confirm no regressions
      from the Docker Desktop VM reset.

---

## Related docs

- [`startup-shutdown.md`](startup-shutdown.md) — start/stop commands, full service URL table
- [`troubleshooting.md`](troubleshooting.md) — fixing a broken service
- [`../architecture/system-design.md`](../architecture/system-design.md) — full architecture design
- [`../architecture/data-contracts.md`](../architecture/data-contracts.md) — Kafka/Redis/HDFS contracts
