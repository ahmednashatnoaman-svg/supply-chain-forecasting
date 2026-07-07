"""Integration test: dashboard Kafka tail collectors (Ziad plan Task 5, live data path).

Produces pricing updates and alerts onto real Kafka (testcontainers + Apicurio ccompat), then calls
``collect_price_updates`` / ``collect_alerts`` and asserts the dashboard receives them. Proves the
live tailing path used by the Streamlit pages actually works against the broker.
"""

from __future__ import annotations

import time

import pytest

pytest.importorskip("confluent_kafka")
pytestmark = pytest.mark.integration


def _ensure_topic(bootstrap: str, name: str, partitions: int):
    """Create a topic idempotently (ignore already-exists so the fixture is reusable)."""
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka.error import KafkaException

    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        fs = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
        for _, future in fs.items():
            future.result(timeout=30)
    except KafkaException as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
    return name


@pytest.fixture()
def pricing_updates_topic(kafka_env, kafka_bootstrap: str):
    yield _ensure_topic(kafka_bootstrap, "automated_pricing_updates", 6)


@pytest.fixture()
def system_alerts_topic(kafka_env, kafka_bootstrap: str):
    yield _ensure_topic(kafka_bootstrap, "system_alerts", 3)


def test_collect_price_updates_tails_automated_pricing_updates(kafka_env, pricing_updates_topic):
    from automation.dashboard.components.kafka_source import collect_price_updates
    from libs.scf_common.io.kafka import AvroKafkaProducer

    producer = AvroKafkaProducer(
        pricing_updates_topic, "automated_pricing_updates.avsc", key_field="item_id"
    )
    for i in range(3):
        producer.produce(
            {
                "event_time": int(time.time() * 1000) + i,
                "item_id": 10 + i,
                "base_price": 20.0,
                "new_price": 19.99 + i,
                "velocity": 5.0 * (i + 1),
                "baseline": 10.0,
                "surge_prob": 0.1 * i,
                "reason": f"update {i}",
            }
        )
    producer.flush(10)

    events = collect_price_updates(max_messages=10, poll_timeout=1.0)

    assert len(events) == 3
    assert {e["item_id"] for e in events} == {10, 11, 12}
    assert all("new_price" in e for e in events)


def test_collect_alerts_tails_system_alerts(kafka_env, system_alerts_topic):
    from automation.dashboard.components.kafka_source import collect_alerts
    from libs.scf_common.io.kafka import AvroKafkaProducer

    producer = AvroKafkaProducer(system_alerts_topic, "system_alerts.avsc", key_field="item_id")
    producer.produce(
        {
            "event_time": int(time.time() * 1000),
            "item_id": 42,
            "alert_type": "LOW_STOCK",
            "stock_level": 5,
            "reorder_threshold": 50,
            "message": "low stock during surge",
        }
    )
    producer.flush(10)

    alerts = collect_alerts(max_messages=10, poll_timeout=1.0)

    assert len(alerts) == 1
    assert alerts[0]["item_id"] == 42
    assert alerts[0]["alert_type"] == "LOW_STOCK"


def test_collect_returns_empty_when_no_messages(kafka_env, kafka_bootstrap: str):
    # Use a dedicated empty topic so `auto.offset.reset=earliest` has nothing to replay (the shared
    # system_alerts topic may carry leftover records from the test above).
    empty_topic = _ensure_topic(kafka_bootstrap, "empty_alerts_test", 1)
    from automation.dashboard.components.kafka_source import _collect

    events = _collect(
        empty_topic,
        "system_alerts.avsc",
        group_id="dashboard-empty-test",
        max_messages=5,
        poll_timeout=0.5,
    )
    assert events == []
