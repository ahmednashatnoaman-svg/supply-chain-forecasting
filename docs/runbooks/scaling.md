# Runbook — Scaling (Stay Zero-Cost)

> **Owner:** Nagy | **Milestone:** M5

## Local Horizontal Scaling (free)

### More Spark Workers

```bash
# Scale to 3 workers on your machine (free — uses existing Docker resources):
docker compose -f infra/docker/docker-compose.yml --profile full up -d --scale spark-worker=3
make ps    # verify 3 spark-worker containers are running
```

Tune memory/CPU per worker in `.env`:

```
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=4G
SPARK_EXECUTOR_MEMORY=3G
```

Then restart workers:

```bash
docker compose -f infra/docker/docker-compose.yml restart spark-worker
```

### More Kafka Partitions

Partition count is a **contract change** — requires a PR reviewed by adjacent owners
(see [master-plan §1](../master-plan.md#1-the-four-frozen-contracts)):

1. Update `contracts/avro/topics.yml` — bump `partitions` for the relevant topic.
2. Re-run bootstrap (idempotent, adds partitions to existing topics):
   ```bash
   bash scripts/bootstrap.sh
   ```
3. Ensure your consumer group parallelism ≤ new partition count.

### Streaming Backpressure Tuning

In `streaming/pipeline/pricing_stream.py` (Nashat's layer):

```python
# Tune in SparkSession config:
"spark.sql.shuffle.partitions": "12",      # match partition count
"spark.streaming.kafka.maxRatePerPartition": "1000",
"maxOffsetsPerTrigger": "5000",            # backpressure ceiling
```

---

## Vertical Scaling

Give Docker more resources:

- **Docker Desktop** → Settings → Resources → raise CPU cores and Memory.
- Raise worker memory in `.env`:
  ```
  SPARK_WORKER_MEMORY=8G
  SPARK_EXECUTOR_MEMORY=6G
  ```

---

## Cloud Scaling (only on a FREE self-managed cluster)

Use `kind` or `minikube` locally with the Helm chart:

```bash
kind create cluster --name scf
helm install scf infra/helm/supply-chain \
  --set sparkWorker.replicas=3 \
  --set global.imagePullPolicy=IfNotPresent
```

Scale workers at runtime:

```bash
kubectl scale deployment scf-spark-worker --replicas=5
```

> ⚠ **Do NOT deploy to a paid managed cluster** — the Terraform stub (`infra/terraform/`) is
> intentionally inert (no provider configured) to prevent accidental billed provisioning.
> See [cost-and-licensing](../reference/cost-and-licensing.md).

---

## Capacity Reference

| Component | Min (core profile) | Recommended (full profile) |
|---|---|---|
| Docker RAM | 8 GB | 16 GB |
| Spark workers | 1 | 2–3 |
| Kafka partitions (`live_web_traffic`) | 6 | 6–12 |
| Kafka partitions (others) | 3 | 3–6 |
