# Nagy — Infrastructure & Monitoring Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Steps use `- [x]` tracking.
> Parent: [`../master-plan.md`](../master-plan.md). Milestones: **M0** (foundation) and **M5** (harden).

**Goal:** Provide a one-command, zero-cost local cluster (HDFS, Spark, Kafka, Redis, Schema Registry,
MLflow, n8n) plus Prometheus/Grafana monitoring, and cloud-ready Helm/Terraform manifests.

**Architecture:** A single Docker Compose network where every engine is reachable by service name.
`make up` boots it; `bootstrap.sh` creates topics + HDFS dirs. Monitoring scrapes each service.

**Tech Stack:** Docker Compose, Bitnami/Apache images, Prometheus, Grafana, Helm, Terraform.

## Global Constraints
Inherits [master §Global Constraints](../master-plan.md#global-constraints) — **zero-cost OSS only**,
pinned versions, contracts-driven.

## File Structure
- `infra/docker/docker-compose.yml` — full stack (profiles: `core`, `full`).
- `infra/hadoop/config/*` — core-site/hdfs-site.
- `infra/monitoring/prometheus/prometheus.yml` — scrape config.
- `infra/monitoring/grafana/provisioning/*` + `dashboards/*.json`.
- `scripts/bootstrap.sh` — create Kafka topics + HDFS dirs after boot.
- `infra/helm/supply-chain/*` + `infra/terraform/*` — cloud-ready stubs.
- `tests/integration/test_infra_up.py` — asserts every service is healthy.

---

### Task 1: Compose stack boots and every service is healthy

**Files:**
- Create: `infra/docker/docker-compose.yml`, `infra/hadoop/config/core-site.xml`,
  `infra/hadoop/config/hdfs-site.xml`
- Test: `tests/integration/test_infra_up.py`

**Interfaces:**
- Produces: service endpoints consumed by ALL layers — `kafka:9092`, `schema-registry:8081`,
  `namenode:9000`, `spark-master:7077`, `redis:6379`, `mlflow:5000`, `prometheus:9090`, `grafana:3000`.

- [x] **Step 1: Write the failing test**
```python
# tests/integration/test_infra_up.py
import socket, pytest
SERVICES = {"kafka":9092,"redis":6379,"namenode":9000,"spark-master":7077,"schema-registry":8081}
@pytest.mark.integration
@pytest.mark.parametrize("host,port", SERVICES.items())
def test_service_reachable(host, port):
    with socket.create_connection((host, port), timeout=5):
        pass  # connection success = healthy
```
- [x] **Step 2: Run it, expect FAIL** — `pytest tests/integration/test_infra_up.py -v` → connection refused.
- [x] **Step 3: Write `docker-compose.yml`** with zookeeper, kafka, schema-registry (Apicurio), namenode,
  datanode, spark-master, spark-worker, redis, mlflow, prometheus, grafana, n8n — each on network
  `scf-net`, with `healthcheck:` blocks and Compose `profiles: [core]` / `[full]`.
- [x] **Step 4: Boot & run** — `docker compose -f infra/docker/docker-compose.yml up -d` then the test → PASS.
- [x] **Step 5: Commit** — `git commit -m "feat(infra): docker-compose stack with health checks"`

### Task 2: bootstrap.sh creates topics + HDFS medallion dirs (idempotent)

**Files:** Create `scripts/bootstrap.sh`; Test `tests/integration/test_bootstrap.py`
**Interfaces:** Produces topics `live_web_traffic, inventory_updates, system_alerts,
automated_pricing_updates` (partitions per `contracts/`) and HDFS `/data/{bronze,silver,gold}`.

- [x] **Step 1: Failing test** — assert `kafka-topics --list` contains all 4 topics and
  `hdfs dfs -test -d /data/bronze` exits 0.
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Write `bootstrap.sh`** — loop `kafka-topics --create --if-not-exists` reading partition
  counts from `contracts/avro/topics.yml`; `hdfs dfs -mkdir -p` the three zones. Idempotent (re-runnable).
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(infra): idempotent bootstrap for topics + HDFS dirs`.

### Task 3: Prometheus scrapes all services; Grafana provisions dashboards

**Files:** `infra/monitoring/prometheus/prometheus.yml`,
`infra/monitoring/grafana/provisioning/{datasources,dashboards}.yml`,
`infra/monitoring/grafana/dashboards/platform-overview.json`; Test `tests/integration/test_monitoring.py`
**Interfaces:** Consumes `/metrics` from each layer (see `libs/scf_common/observability`).

- [x] **Step 1: Failing test** — query `http://prometheus:9090/api/v1/targets` and assert every job is `up`.
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Write scrape config** with jobs for kafka-exporter, redis-exporter, spark, and the four
  Python layer apps; add Grafana provisioning + one overview dashboard JSON.
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(infra): prometheus + grafana provisioning`.

### Task 4 (M5): Cloud-ready Helm chart + Terraform stub (never applied to paid cloud)

**Files:** `infra/helm/supply-chain/{Chart.yaml,values.yaml,templates/*}`, `infra/terraform/{main.tf,
variables.tf,README.md}`; Test `tests/unit/test_helm_lint.py`
**Interfaces:** Produces deployable manifests mirroring the compose services.

- [x] **Step 1: Failing test** — `helm lint infra/helm/supply-chain` exits 0 (skip if helm absent).
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Write chart** (Deployments/Services for each engine) + Terraform stub with a big
  `# COST WARNING: do not apply against a billed provider` banner and all counts defaulted to free tiers.
- [x] **Step 4: `helm lint` → PASS.**
- [x] **Step 5: Commit** — `feat(infra): cloud-ready helm chart + terraform stub (manifests only)`.

### Task 5 (M5): Runbooks

**Files:** `docs/runbooks/{startup-shutdown.md,troubleshooting.md,scaling.md}`
- [x] Write runbooks: how to `make up/down`, common failures (port clashes, low RAM, HDFS safemode),
  and how to scale Spark workers via Compose `--scale spark-worker=N`. Commit
  `docs(infra): operational runbooks`.
