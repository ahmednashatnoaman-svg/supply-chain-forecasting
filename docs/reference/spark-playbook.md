# Spark Optimization Playbook (project-specific)

Concrete guidance for `libs/scf_common/io/spark.py`, `batch/`, and `streaming/`. Owner: **Emad**
(batch), consulted: **Nashat** (streaming), **Nagy** (cluster sizing).

## 1. Session config (already applied in `get_spark()`)

`libs/scf_common/io/spark.py` now enables:
- **AQE** (`spark.sql.adaptive.enabled`) with `coalescePartitions` + `skewJoin` — right-sizes shuffle
  partitions after seeing real data volumes and auto-splits skewed partitions.
- **Kryo serializer** — cheaper CPU than Java serialization for the DataFrame/UDF-heavy pipeline.
- **`spark.sql.shuffle.partitions=8`** as the local-dev floor (AQE coalesces up from here, never below).
  Raise this on a real multi-worker cluster; 8 keeps local test runs from spawning hundreds of
  near-empty tasks on toy datasets.

## 2. The known skew hotspot: GraphFrames co-purchase join

`batch/graph/elasticity.py`'s `cooccurrence_edges` self-joins transactions on `visitor_id`. Popular
SKUs (bought by thousands of visitors) create a classic **skewed join** — one partition doing
disproportionate work while others idle. AQE's `skewJoin` (enabled above) auto-splits it, but if you
see one task taking 10x longer in the Spark UI (`spark-master:8080` in this stack), the manual fallback
is salting:

```python
# Only if AQE skew handling isn't enough — see spark-optimization skill's salt_join pattern.
df_salted = transactions.withColumn("salt", (F.rand() * 10).cast("int"))
```

## 3. Broadcast joins — where they apply here

`batch/publish/to_redis.py` currently `.collect()`s the forecast/graph DataFrames (small after
aggregation — one row per SKU). That's fine at Retailrocket sample scale. If the SKU catalog grows
large enough that `.collect()` becomes a driver memory risk, switch to a broadcast join pattern instead:
join the small side (e.g., `graph_df` aggregated per SKU) with `F.broadcast()` rather than collecting.
`spark.sql.autoBroadcastJoinThreshold=50m` (set in `get_spark()`) auto-broadcasts small tables under
50MB without an explicit hint.

## 4. Streaming-specific: `pricing_stream.py` (Nashat)

- **`maxOffsetsPerTrigger`**: bound how much of `live_web_traffic` one micro-batch reads, so a burst
  (a surge!) doesn't overwhelm one trigger. Set this explicitly once Task 5 wires the real readStream —
  don't rely on the default (unbounded).
- **Watermarking**: the sliding-window velocity aggregation needs `withWatermark("event_time", "2
  minutes")` (or similar) so late-arriving events don't keep windows open forever and blow up state.
- **Checkpointing**: `writeStream.option("checkpointLocation", ...)` must point at a path that survives
  restarts (HDFS, not local disk) — otherwise a restart replays from the earliest Kafka offset.
- **`pandas_udf` batching**: `build_surge_pandas_udf` (streaming/lstm/infer.py) reconstructs the model
  from broadcast weights per batch — this is correct but not free. If profiling shows this dominating
  latency, increase `spark.sql.execution.arrow.maxRecordsPerBatch` to amortize the reconstruction cost
  over more rows per call.

## 5. Debugging checklist (Spark UI at `localhost:8088` / driver logs)

1. `df.explain(mode="cost")` before assuming a join is the bottleneck — verify what plan Spark actually
   chose (broadcast vs sort-merge).
2. Check for skew: partition-count histogram (`spark-optimization` skill's `check_partition_skew`
   pattern) — >2x ratio between max and avg partition size means skew, not just "more data."
3. Never `.count()` to check "is this DataFrame empty" — use `.take(1)` or `.isEmpty()` (matters in the
   data-quality gate, `batch/etl/expectations.py`, which runs per-DAG-run).

## 6. Production config reference (not yet needed at Retailrocket-sample scale)

Full tuning cheat sheet (executor memory fractions, `spark.sql.files.maxPartitionBytes`, etc.) is in
the `spark-optimization` skill — apply it if/when `infra/helm` is ever actually deployed to a real
multi-node cluster (see [cost-and-licensing.md](cost-and-licensing.md) — not done today, zero-cost).
