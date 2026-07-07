"""Walking-skeleton e2e — the integration canary (Nashat plan Task 2 / master-plan §4).

Proves the real pipeline works against REAL Kafka + Redis (via testcontainers, not fakes): produce a
burst of transaction events for one SKU with a low seeded forecast baseline -> parse_events ->
compute_velocity -> process_batch -> assert a priced update and a LOW_STOCK alert are produced.

SCOPE NOTE: testcontainers-python has no first-party Schema Registry module, and hand-rolling one via
a generic container wired to Kafka's internal Docker network is a nontrivial, version-sensitive task.
This test therefore monkeypatches `pricing_producer`/`alert_producer` (which normally go through
AvroKafkaProducer's SchemaRegistryClient) to plain in-memory sinks, while using REAL Kafka
(testcontainers) for input and REAL Redis (testcontainers) for all state — covering every piece of
actual business logic. The registry-dependent producer/consumer wrapper itself is exercised by
`ingestion`'s own tests once implemented (hatem-plan.md Tasks 2-3).
"""

from __future__ import annotations

import io
import json

import pytest

pytest.importorskip("testcontainers")
fastavro = pytest.importorskip("fastavro")

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def kafka_container():
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        yield kafka


@pytest.fixture(scope="module")
def redis_container():
    from testcontainers.redis import RedisContainer

    with RedisContainer("redis:7-alpine") as redis_c:
        yield redis_c


class _FakeSink:
    """In-memory stand-in for AvroKafkaProducer — records what would have been produced."""

    def __init__(self, records: list[dict]):
        self._records = records

    def produce(self, record: dict) -> None:
        self._records.append(record)

    def flush(self, timeout: float = 10.0) -> None:  # noqa: ARG002
        pass


def test_surge_event_produces_price_update_and_low_stock_alert(
    spark, kafka_container, redis_container, monkeypatch
):
    pytest.importorskip("pyspark")
    import redis as redis_lib
    from confluent_kafka import Producer

    from libs.scf_common.contracts import RedisKeys, Topics
    from libs.scf_common.io.kafka import load_schema
    from streaming.pipeline import pricing_stream as ps

    bootstrap = kafka_container.get_bootstrap_server()

    # --- seed real Redis with a low baseline so a modest burst reads as a surge ---
    r = redis_lib.Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    sku = 10
    r.set(RedisKeys.forecast(sku), 5.0)
    r.set(RedisKeys.elasticity(sku), json.dumps([]))
    r.set(RedisKeys.inventory(sku), 5)  # below default reorder threshold -> alert expected
    monkeypatch.setattr("streaming.pipeline.pricing_stream.get_redis", lambda: r)

    # --- produce a burst of raw Kafka messages, fastavro-encoded (see SCOPE NOTE above) ---
    schema_str = load_schema("live_web_traffic.avsc")
    schema = fastavro.parse_schema(json.loads(schema_str))

    def encode(record: dict) -> bytes:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, record)
        return b"\x00" + (1).to_bytes(4, "big") + buf.getvalue()

    producer = Producer({"bootstrap.servers": bootstrap})
    for i in range(50):
        record = {
            "event_time": 1700000000000 + i,
            "visitor_id": i,
            "event": "transaction",
            "item_id": sku,
            "price": 20.0,
        }
        producer.produce(Topics.LIVE_WEB_TRAFFIC, key=str(sku), value=encode(record))
    producer.flush(10)

    # --- substitute the registry-dependent output sinks (see SCOPE NOTE) ---
    produced_prices: list[dict] = []
    produced_alerts: list[dict] = []
    monkeypatch.setattr(ps, "pricing_producer", lambda: _FakeSink(produced_prices))
    monkeypatch.setattr(ps, "alert_producer", lambda: _FakeSink(produced_alerts))

    # --- exercise the real pipeline against the real Kafka topic (batch read is sufficient: the
    # same parse_events/compute_velocity code path is shared by the streaming `run()`) ---
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", Topics.LIVE_WEB_TRAFFIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    events = ps.parse_events(raw, schema_str)
    velocity = ps.compute_velocity(events, window_seconds=60)
    ps.process_batch(velocity, batch_id=0, model=None)

    assert len(produced_prices) == 1
    priced = produced_prices[0]
    assert priced["item_id"] == sku
    assert priced["velocity"] == 50
    assert priced["baseline"] == 5.0
    # model=None -> surge_prob 0.0 -> no surge -> dynamic_price returns base_price (20.0) unchanged,
    # but present_price ALWAYS charm-rounds what it displays, even a genuinely unchanged price -- by
    # design this can round DOWN (20.00 -> 19.99), see streaming/pricing/psychology.py and
    # test_present_price_unchanged_has_no_anchor in test_psychology.py.
    assert priced["new_price"] == 19.99

    assert len(produced_alerts) == 1
    assert produced_alerts[0]["alert_type"] == "LOW_STOCK"
    assert produced_alerts[0]["item_id"] == sku
