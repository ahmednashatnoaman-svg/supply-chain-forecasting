# Runbook — Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Host tools can't reach Kafka | Using internal listener from host | Connect to `localhost:29092` (external), containers use `kafka:9092` |
| `make up` OOM / containers killed | Docker RAM too low | Give Docker Desktop 12–16GB; or run `PROFILE=core` |
| HDFS writes fail, "safemode" | Namenode still starting | Wait; `hdfs dfsadmin -safemode leave` inside namenode |
| Schema Registry 5xx | Kafka not ready yet | Wait for kafka healthcheck; SR depends on it |
| Streaming reads nothing | Topics not created | Re-run `bash scripts/bootstrap.sh` (idempotent) |
| Pricing returns base only | No forecast in Redis | `python scripts/seed_redis_stub.py` or run the batch layer |
| LSTM errors in stream | Model artifact missing | Expected → falls back to velocity-only (surge_prob=0.0) |
| Prometheus targets down | App `/metrics` not served | Ensure `serve_metrics(port)` is called; check `host.docker.internal` |

## Diagnostics
```bash
make logs                    # tail everything
make ps                      # health status
docker compose -f infra/docker/docker-compose.yml exec kafka kafka-topics --list --bootstrap-server localhost:9092
docker compose -f infra/docker/docker-compose.yml exec redis redis-cli keys '*'
```
