"""Integration test: Kafka Avro producer round-trip (Hatem plan Task 3). Needs Kafka + SR containers."""
import pytest

pytest.importorskip("testcontainers")
pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="Scaffold — implement with testcontainers Kafka + schema registry per hatem-plan Task 3")
def test_producer_roundtrip_preserves_order_and_count():
    """
    Plan:
      1. Start Kafka + Schema Registry via testcontainers.
      2. Produce N contract-conformant records to live_web_traffic.
      3. Consume and assert count == N and first/last event_time ordering preserved.
    """
