"""Consumer verifier — detect message loss/lag. Implements Hatem plan Task 4."""
from __future__ import annotations

from dataclasses import dataclass

from libs.scf_common.contracts import Topics


@dataclass
class VerifyReport:
    received: int
    missing: int
    max_lag_ms: int


def verify(expected_count: int, timeout_s: int = 30) -> VerifyReport:  # pragma: no cover - needs Kafka
    """Consume up to `expected_count` messages and report loss/lag.

    TODO(hatem-plan Task 4): consume from Topics.LIVE_WEB_TRAFFIC, count messages, track offsets and
    the max (now - event_time) as lag; missing = expected_count - received.
    """
    from libs.scf_common.io.kafka import AvroKafkaConsumer

    consumer = AvroKafkaConsumer(Topics.LIVE_WEB_TRAFFIC, "live_web_traffic.avsc", group_id="verifier")
    received = 0
    try:
        # (poll loop until expected_count or timeout)
        pass
    finally:
        consumer.close()
    return VerifyReport(received=received, missing=expected_count - received, max_lag_ms=0)
