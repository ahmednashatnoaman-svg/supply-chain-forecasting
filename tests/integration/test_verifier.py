"""Integration test: consumer verifier detects loss/lag (Hatem plan Task 4, exec Task 4).

Produce 500 records via ``run(...)``, then ``verify(500, timeout_s=60)`` and assert the verifier
reports every record received, zero missing, and a non-negative lag. Needs real Kafka + Apicurio
(testcontainers) -- see tests/integration/conftest.py.
"""

from __future__ import annotations

import pytest

pytest.importorskip("confluent_kafka")
pytestmark = pytest.mark.integration


def _row(i: int) -> dict:
    return {
        "timestamp": str(1442300000000 + i),  # ascending -> ordered replay
        "visitorid": str(100 + (i % 5)),
        "event": "view" if i % 2 == 0 else "transaction",
        "itemid": str(10 + (i % 6)),
        "price": "9.99" if i % 2 else "",
    }


def test_verifier_reports_no_loss_after_producer(kafka_env, live_topic):
    from ingestion.generator.traffic_generator import run
    from ingestion.verifier.consumer_verifier import verify

    n = 500
    produced = run(rows=[_row(i) for i in range(n)], speed=100, surge=None)
    assert produced == n

    report = verify(expected_count=n, timeout_s=60)
    assert report.received == n, f"expected {n}, received {report.received}"
    assert report.missing == 0
    assert report.max_lag_ms >= 0
