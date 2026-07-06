# Kafka Playbook (project-specific)

Concrete guidance for `contracts/avro/*` and `libs/scf_common/io/kafka.py`. Owner: **Hatem**
(ingestion/producer), consulted: **Nashat** (streaming consumer), **Ziad** (alert consumer).

## 1. Producer reliability (Hatem's traffic generator, `ingestion/generator/traffic_generator.py`)

`AvroKafkaProducer` (in `libs/scf_common/io/kafka.py`) should be constructed with reliability
settings baked in — add these to the underlying `confluent_kafka.Producer` config:

```python
{
    "bootstrap.servers": settings.kafka.bootstrap_servers,
    "acks": "all",                 # wait for all in-sync replicas
    "enable.idempotence": True,    # prevents duplicate messages on retry
    "retries": 2147483647,         # let idempotence + acks=all handle retry safety
    "compression.type": "lz4",     # fast compression, cheap on a laptop
}
```

**Partition key = `item_id`** (already the contract in `contracts/avro/topics.yml`) — this guarantees
all events for one SKU land in the same partition, so `pricing_stream`'s windowed velocity aggregation
sees them in order. Never key on `visitor_id` for these topics; it would scatter one SKU's signal
across partitions and break the sliding-window velocity math.

## 2. Consumer offset handling (Nashat's stream, Ziad's alert bridge)

- **Manual commit, not auto-commit.** `enable.auto.commit=false`; commit only after Redis/Kafka writes
  succeed (mirrors `AvroKafkaConsumer` usage in `automation/n8n/alert_bridge.py`).
- **`auto.offset.reset=earliest`** for the walking-skeleton/backfill case (we want to replay history
  during testing); switch to `latest` only for a long-running production consumer that should skip
  backlog on restart.
- **Idempotent processing.** `dynamic_price` and `build_reorder` are pure functions of their input —
  re-processing a message on redelivery produces the same output, so at-least-once delivery is safe
  without a dedup table.

## 3. Consumer groups

| Consumer | Group ID | Why its own group |
|---|---|---|
| `pricing_stream` (Nashat) | `pricing-engine` | Must see every `live_web_traffic` event |
| `alert_bridge` (Ziad) | `alert-bridge` | Independently consumes `system_alerts` |
| `consumer_verifier` (Hatem, testing only) | `verifier-<run-id>` | Unique per test run — never shares a group with production consumers |

## 4. Dead-letter handling

None of our topics currently route to a DLT — a malformed Avro record fails deserialization loudly
(the `AvroDeserializer` raises). For `system_alerts`/`automated_pricing_updates`, that's acceptable
(schema is producer-controlled by our own code). If a future ingestion source is less trusted, add a
`*_dlq` topic and catch `SerializationError` in the consumer loop before committing the offset.

## 5. Monitoring

`libs/scf_common/observability` metrics (`RECORDS_PROCESSED`, `ERRORS_TOTAL`, `LATENCY_SECONDS`) should
be incremented in every producer/consumer loop. The `kafka-exporter` service (already in
`infra/docker/docker-compose.yml`, profile `full`) exposes consumer lag to Prometheus — watch
`kafka_consumergroup_lag` for `pricing-engine`; sustained growth means the streaming job can't keep up
with `STREAMING_WINDOW_SECONDS`.

## 6. Schema evolution rule

Only add **optional fields with defaults** to `contracts/avro/*.avsc` (backward compatible). Never
remove or rename a field — per the contract-change process in
[git-workflow.md](../runbooks/git-workflow.md), a schema change needs sign-off from every consumer's
owner (Hatem produces, Nashat + Ziad consume — see `.github/CODEOWNERS`).
