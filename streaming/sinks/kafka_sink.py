"""Avro Kafka sinks for the streaming layer. Implements Nashat plan Task 5 (sink side)."""
from __future__ import annotations

from libs.scf_common.contracts import Topics
from libs.scf_common.io.kafka import AvroKafkaProducer


def pricing_producer() -> AvroKafkaProducer:
    """Producer for automated_pricing_updates."""
    return AvroKafkaProducer(Topics.AUTOMATED_PRICING_UPDATES, "automated_pricing_updates.avsc")


def alert_producer() -> AvroKafkaProducer:
    """Producer for system_alerts."""
    return AvroKafkaProducer(Topics.SYSTEM_ALERTS, "system_alerts.avsc")
