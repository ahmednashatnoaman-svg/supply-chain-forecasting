"""Retailrocket traffic generator. Implements Hatem plan Tasks 2 & 3.

Replays events in timestamp order, applies a SurgeSchedule to compress inter-event delays during
surges, and produces contract-conformant Avro to `live_web_traffic`.
"""

from __future__ import annotations

import argparse

from ingestion.generator.surge import SurgeSchedule
from libs.scf_common.contracts import Topics

EVENT_ENUM = {"view", "addtocart", "transaction"}


def to_avro_record(row: dict) -> dict:
    """Map a Retailrocket CSV row to the live_web_traffic Avro record.

    Args:
        row: dict with keys timestamp, visitorid, event, itemid, (optional) price.

    Returns:
        Avro-conformant dict {event_time, visitor_id, event, item_id, price}.
    """
    event = str(row["event"])
    if event not in EVENT_ENUM:
        raise ValueError(f"unknown event type: {event}")
    price = row.get("price")
    return {
        "event_time": int(row["timestamp"]),
        "visitor_id": int(row["visitorid"]),
        "event": event,
        "item_id": int(row["itemid"]),
        "price": float(price) if price not in (None, "") else None,
    }


def run(speed: int, limit: int | None, surge: SurgeSchedule | None) -> int:  # pragma: no cover
    """Replay the dataset to Kafka. Returns number of records produced.

    TODO(hatem-plan Task 3): read CSV in timestamp order; sleep (interval/speed/multiplier) between
    events; produce via AvroKafkaProducer(Topics.LIVE_WEB_TRAFFIC, "live_web_traffic.avsc").
    """
    from libs.scf_common.io.kafka import AvroKafkaProducer

    producer = AvroKafkaProducer(Topics.LIVE_WEB_TRAFFIC, "live_web_traffic.avsc")
    # (replay loop goes here)
    producer.flush()
    return 0


def _cli() -> None:  # pragma: no cover
    p = argparse.ArgumentParser(description="Retailrocket -> Kafka traffic generator")
    p.add_argument("--speed", type=int, default=100, help="playback speed multiplier")
    p.add_argument("--limit", type=int, default=None, help="max events to produce")
    args = p.parse_args()
    run(args.speed, args.limit, surge=None)


if __name__ == "__main__":
    _cli()
