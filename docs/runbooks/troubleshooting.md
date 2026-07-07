# Runbook — Troubleshooting

> **Owner:** Nagy | **Milestone:** M5

## Quick Diagnostics

```bash
make ps                      # show container status + health
make logs                    # tail all container logs (Ctrl-C to stop)

# Per-service logs:
docker compose -f infra/docker/docker-compose.yml logs --tail=50 kafka
docker compose -f infra/docker/docker-compose.yml logs --tail=50 namenode

# Kafka topic list:
docker compose -f infra/docker/docker-compose.yml exec kafka \
  kafka-topics --list --bootstrap-server localhost:9092

# Redis key inspection:
docker compose -f infra/docker/docker-compose.yml exec redis redis-cli keys '*'

# Prometheus targets:
curl -s http://localhost:9090/api/v1/targets | python -m json.tool | grep -E '"health"|"job"'
```

---

## Common Failures

| Symptom | Likely Cause | Fix |
|---|---|---|
| `make up` starts 0 containers | No profile passed to compose | Use `make up` (sets `--profile full`) or `make up PROFILE=core` |
| Namenode exits with `InconsistentFSStateException` | Volume permissions issue (hadoop user can't write Docker volume) | `docker-compose.yml` sets `user: root` on namenode/datanode — this is the correct config; if you changed it revert |
| Host tools can't reach Kafka on 9092 | Wrong listener | Connect to **`localhost:29092`** (external); containers use `kafka:9092` |
| `make up` OOM / containers killed | Docker RAM too low | Give Docker Desktop ≥ 12 GB RAM, or run `make up PROFILE=core` |
| HDFS writes fail — "safemode on" | Namenode still initialising | Wait 30 s, then: `docker compose ... exec namenode hdfs dfsadmin -safemode leave` |
| Schema Registry 5xx on start | Kafka not ready yet | Schema Registry depends on Kafka health-check; wait 60 s and retry |
| Topics missing — streaming reads nothing | `bootstrap.sh` not run / failed | Re-run (idempotent): `bash scripts/bootstrap.sh` |
| Pricing returns base price only | No forecast keys in Redis | `python scripts/seed_redis_stub.py` (or run the batch layer M2) |
| LSTM errors in streaming layer | Model artifact missing | Expected behaviour — falls back to velocity-only (`surge_prob=0.0`) |
| Prometheus targets DOWN | `/metrics` not served | Ensure `serve_metrics(port)` is called in the layer; verify `host.docker.internal` resolves |
| Grafana shows "No data" | Prometheus datasource misconfigured | Check `infra/monitoring/grafana/provisioning/datasources.yml`; URL must be `http://prometheus:9090` |
| Airflow DB errors on start | `airflow-init` didn't complete | Run `docker compose ... restart airflow-webserver airflow-scheduler` |
| Port already in use | Previous stack not stopped | `make down` then `make up`; or find the process: `netstat -ano | findstr <PORT>` |

---

## Checking Service Health Manually

```bash
# Kafka broker responding?
docker compose -f infra/docker/docker-compose.yml exec kafka \
  kafka-broker-api-versions --bootstrap-server localhost:9092

# HDFS namenode healthy?
curl -s http://localhost:9870 | grep -i "namenode"

# Redis ping?
docker compose -f infra/docker/docker-compose.yml exec redis redis-cli ping
# Expected: PONG

# Schema Registry responding?
curl -s http://localhost:8081/subjects
```

---

## Recovering from a Full Wipe

If the stack is in an irrecoverable state:

```bash
docker compose -f infra/docker/docker-compose.yml --profile full down -v
docker volume prune -f           # remove dangling volumes
make up                          # fresh start
```
