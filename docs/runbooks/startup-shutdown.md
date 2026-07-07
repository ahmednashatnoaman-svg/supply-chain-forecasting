# Runbook — Startup & Shutdown

> **Owner:** Nagy | **Milestone:** M0, M5

## Prerequisites

- Docker Desktop ≥ 4.30 (or Docker Engine 26+) with **≥ 12 GB RAM** allocated.
- `docker compose` v2 plugin (`docker compose version` → v2.x).
- Ports free: 2181, 6379, 7077, 8080, 8081, 8082, 9000, 9090, 9092, 9121, 9308, 9870, 29092, 3000, 5000, 5678.

---

## Start the Stack

### Full profile (all services — default)

```bash
cp .env.example .env          # once: fill in any local overrides
make up                       # boots --profile full + runs bootstrap.sh
make ps                       # verify all services are healthy
```

### Core profile (lighter laptop — Kafka + HDFS + Spark + Redis only)

```bash
make up PROFILE=core
```

Or directly with Docker Compose:

```bash
docker compose -f infra/docker/docker-compose.yml --profile core up -d
bash scripts/bootstrap.sh
```

## Seed + Smoke Test

```bash
python scripts/seed_redis_stub.py   # unblocks streaming without needing the batch layer
make smoke                          # walking-skeleton e2e (all stubs green)
```

## Service URLs (local)

| Service | URL | Notes |
|---|---|---|
| Spark Master UI | http://localhost:8088 | Worker count, jobs (remapped from 8080) |
| HDFS Namenode UI | http://localhost:9870 | Datanode health |
| Kafka (host tools) | localhost:29092 | Use 29092 from host; containers use `kafka:9092` |
| Schema Registry | http://localhost:8081 | Avro schema browser |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| MLflow | http://localhost:5000 | Model registry |
| n8n | http://localhost:5678 | Automation workflows |
| Prometheus | http://localhost:9090 | Raw metrics / targets |
| Grafana | http://localhost:3000 | `admin` / `admin` — Platform Overview dashboard |
| Streamlit dashboard | http://localhost:8501 | `make dashboard` |

---

## Shutdown

### Graceful (volumes persist — data survives restart)

```bash
make down            # stops all containers; named volumes kept
```

### Full reset (wipe all volumes — fresh HDFS + Kafka state)

```bash
docker compose -f infra/docker/docker-compose.yml --profile full down -v
```

> ⚠ `-v` removes named volumes (`hdfs-name`, `hdfs-data`, `grafana-data`, `airflow-db`).
> Re-run `make up` and `bash scripts/bootstrap.sh` after a full reset.

---

## Restart a single service

```bash
docker compose -f infra/docker/docker-compose.yml restart <service-name>
# e.g.:
docker compose -f infra/docker/docker-compose.yml restart kafka
```
