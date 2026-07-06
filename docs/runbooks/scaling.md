# Runbook — Scaling (stay zero-cost)

## Local horizontal scaling (free)
```bash
# More Spark workers on your machine:
docker compose -f infra/docker/docker-compose.yml up -d --scale spark-worker=3
```
- Kafka throughput: raise a topic's partitions (contract change → update `contracts/avro/topics.yml`
  and re-run bootstrap). Keep consumer group parallelism ≤ partitions.
- Streaming: tune `spark.sql.shuffle.partitions` and `maxOffsetsPerTrigger` for backpressure.

## Vertical
- Give Docker more RAM/CPU. Raise `SPARK_WORKER_MEMORY` / `SPARK_EXECUTOR_MEMORY` in `.env`.

## Cloud (only on a FREE self-managed cluster)
Use `kind`/`minikube` locally with `infra/helm/supply-chain`. Do **not** deploy to a paid managed
cluster — see [cost-and-licensing](../reference/cost-and-licensing.md). The Terraform stub is inert by
design (no provider configured) to prevent accidental billed provisioning.
