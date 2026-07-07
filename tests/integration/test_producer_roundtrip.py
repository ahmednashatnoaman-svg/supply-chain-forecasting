"""Integration test: Kafka Avro producer round-trip (Hatem plan Task 3, exec Task 3).

Produce N contract-conformant records from an in-memory test split via ``run(...)``, then consume
with ``AvroKafkaConsumer`` and assert the count and per-partition timestamp ordering are preserved.
Needs real Kafka + Apicurio Registry (testcontainers) -- see tests/integration/conftest.py.
"""

from __future__ import annotations

import uuid

import pytest

pytest.importorskip("confluent_kafka")
pytestmark = pytest.mark.integration


def _row(i: int) -> dict:
    # String-typed CSV columns, matching what to_avro_record expects (pandas reads CSV as str
    # when dtype=str). Close timestamps -> negligible sleep so 50 records stream fast.
    return {
        "timestamp": str(1442300000000 + i),
        "visitorid": str(100 + (i % 5)),
        "event": "view" if i % 2 == 0 else "transaction",
        "itemid": str(10 + (i % 6)),  # 6 partition keys -> spreads across the 6 partitions
        "price": "9.99" if i % 2 else "",
    }


def test_producer_roundtrip_preserves_order_and_count(kafka_env, live_topic):
    from ingestion.generator.traffic_generator import run
    from libs.scf_common.io.kafka import AvroKafkaConsumer

    n = 50
    rows = [_row(i) for i in range(n)]
    produced = run(rows=rows, speed=100, surge=None)
    assert produced == n

    consumer = AvroKafkaConsumer(
        live_topic, "live_web_traffic.avsc", group_id=f"verifier-{uuid.uuid4().hex[:8]}"
    )
    received: list[dict] = []
    try:
        deadline = _now_plus(60)
        while len(received) < n and _now() < deadline:
            rec = consumer.poll(1.0)
            if rec is not None:
                received.append(rec)
    finally:
        consumer.close()

    assert len(received) == n, f"expected {n} records, got {len(received)}"
    # event_time is non-decreasing across the consumed sequence (ordering preserved within
    # each partition; with 6 partition keys across 6 partitions a fully-global sort is not
    # guaranteed, so assert per-partition monotonicity instead).
    by_partition: dict[str, list[int]] = {}
    for rec in received:
        by_partition.setdefault(str(rec["item_id"]), []).append(rec["event_time"])
    for key, times in by_partition.items():
        assert times == sorted(times), f"event_time not ordered for item_id={key}: {times}"
    assert received[0]["event_time"] <= received[-1]["event_time"]


def _now() -> float:
    import time

    return time.time()


def _now_plus(seconds: float) -> float:
    return _now() + seconds
