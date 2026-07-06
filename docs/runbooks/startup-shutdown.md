# Runbook — Startup & Shutdown

## Start the stack
```bash
cp .env.example .env          # once
make up                       # boots compose (full profile) + runs bootstrap.sh
make ps                       # verify all services healthy
```
Core-only (lighter laptop): `PROFILE=core docker compose -f infra/docker/docker-compose.yml up -d`.

## Seed + smoke test
```bash
python scripts/seed_redis_stub.py   # unblock streaming without the batch layer
make smoke                          # walking-skeleton e2e
```

## Shutdown
```bash
make down            # stop containers (volumes persist)
docker compose -f infra/docker/docker-compose.yml down -v   # also wipe volumes (fresh HDFS)
```

## Service URLs (local)
| Service | URL |
|---|---|
| Spark master UI | http://localhost:8080 |
| HDFS namenode UI | http://localhost:9870 |
| Schema Registry | http://localhost:8081 |
| MLflow | http://localhost:5000 |
| n8n | http://localhost:5678 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Dashboard | `make dashboard` → http://localhost:8501 |
