"""Retailrocket traffic generator. Implements Hatem plan Tasks 2 & 3.

Replays events in timestamp order, applies a SurgeSchedule to compress inter-event delays during
surges, and produces contract-conformant Avro to ``live_web_traffic``. Reliability config (acks=all,
idempotence, lz4) is baked into ``AvroKafkaProducer`` (kafka-playbook §1).
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

from ingestion.generator.surge import SurgeSchedule
from libs.scf_common.contracts import Topics
from libs.scf_common.observability import (
    ERRORS_TOTAL,
    RECORDS_PROCESSED,
    get_logger,
    serve_metrics,
)

EVENT_ENUM = {"view", "addtocart", "transaction"}

_COMPONENT = "traffic-generator"
log = get_logger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "generator.yaml"

# Sleep clamp: never wait longer than this between events, even when the real gap between
# dataset timestamps is huge (keeps the live stream responsive on a laptop).
_MAX_SLEEP_S = 0.1


def to_avro_record(row: dict) -> dict:
    """Map a Retailrocket CSV row to the live_web_traffic Avro record.

    Args:
        row: dict with keys timestamp, visitorid, event, itemid, (optional) price.
            Values may be str (pandas dtype=str) or int/float -- both are coerced.

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


def run(
    rows: list[dict],
    speed: int,
    limit: int | None = None,
    surge: SurgeSchedule | None = None,
    topic: str = Topics.LIVE_WEB_TRAFFIC,
    schema_file: str = "live_web_traffic.avsc",
    max_sleep_s: float = _MAX_SLEEP_S,
) -> int:
    """Replay ``rows`` (already timestamp-sorted) to Kafka. Returns records produced.

    Args:
        rows: list of CSV-row dicts with an integer-valued ``timestamp`` (epoch millis),
            sorted ascending by timestamp (use ``ingestion.generator.split`` + the CLI).
        speed: base playback multiplier (100x real time means a 1s real gap sleeps ~10ms).
        limit: if set, produce at most this many events.
        surge: optional SurgeSchedule; the active multiplier shrinks the inter-event sleep
            during surge windows (keyed on event time, not wall-clock).
        topic / schema_file: contract target (defaults to live_web_traffic).
        max_sleep_s: clamp on the inter-event sleep so a huge real gap can't stall the stream.

    Bad rows are logged + counted (ERRORS_TOTAL) and skipped -- a single malformed event must
    not crash the stream.
    """
    from libs.scf_common.io.kafka import AvroKafkaProducer

    producer = AvroKafkaProducer(topic, schema_file)
    produced = 0
    selected = rows if limit is None else rows[:limit]
    try:
        for idx, row in enumerate(selected):
            try:
                record = to_avro_record(row)
                producer.produce(record)
                produced += 1
                RECORDS_PROCESSED.labels(_COMPONENT).inc()
            except Exception:  # noqa: BLE001 -- a bad row must not kill the stream
                ERRORS_TOTAL.labels(_COMPONENT).inc()
                log.exception("producer.skip_bad_row", row_index=idx)
                continue
            # Sleep proportional to the gap to the *next* event's timestamp, compressed by
            # speed and the active surge multiplier. Surges (>1.0) shrink the wait -> faster.
            _maybe_sleep(selected, idx, speed, surge, max_sleep_s)
    finally:
        producer.flush()
    log.info("producer.done", produced=produced, speed=speed)
    return produced


def _maybe_sleep(
    rows: list[dict], idx: int, speed: int, surge: SurgeSchedule | None, max_sleep_s: float
) -> None:
    """Sleep to pace the replay, derived from the gap to the next event's timestamp."""
    if idx + 1 >= len(rows):
        return
    cur_ts = int(rows[idx]["timestamp"])
    next_ts = int(rows[idx + 1]["timestamp"])
    gap_s = max(0.0, (next_ts - cur_ts) / 1000.0)
    # The dataset's epoch millis are UTC, and SurgeSchedule's window bounds are naive datetimes
    # intended to represent that same UTC event time (contract per surge.py's own tests) -- convert
    # to a naive-but-UTC datetime rather than datetime.fromtimestamp()'s local-timezone conversion,
    # which would silently shift every surge-window comparison by the host's UTC offset.
    event_dt = datetime.fromtimestamp(cur_ts / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    mult = surge.multiplier(event_dt) if surge else 1.0
    if mult <= 0:
        mult = 1.0
    sleep_s = gap_s / (speed * mult)
    sleep_s = max(0.0, min(sleep_s, max_sleep_s))
    if sleep_s > 0:
        time.sleep(sleep_s)


def load_config(path: Path = _CONFIG_PATH) -> dict:
    """Load generator.yaml (speed, surges, dataset paths). Returns the parsed dict."""
    import yaml  # local import: pyyaml is a declared dep but keeps surge-only unit tests light

    with open(path) as f:
        return yaml.safe_load(f)


def build_surge(config: dict) -> SurgeSchedule:
    """Build a SurgeSchedule from the ``surges`` list in generator.yaml (D-E)."""
    windows = [(w["start"], w["end"], float(w["multiplier"])) for w in (config.get("surges") or [])]
    return SurgeSchedule(windows=windows)


def run_from_config(
    config_path: Path = _CONFIG_PATH, limit: int | None = None, speed_override: int | None = None
) -> int:
    """CLI entry: load generator.yaml, read the test split, replay to Kafka."""
    cfg = load_config(config_path)
    speed = speed_override if speed_override is not None else int(cfg["playback"]["speed"])
    surge = build_surge(cfg)
    # Replay the 20% TEST split (the live-stream tail); the 80% train split goes to HDFS bronze.
    csv_path = cfg["dataset"]["test_csv"]
    rows = _read_csv_rows(csv_path)
    return run(rows=rows, speed=speed, limit=limit, surge=surge)


def _read_csv_rows(csv_path: str) -> list[dict]:
    """Read a Retailrocket CSV as string-typed rows (pandas dtype=str)."""
    import pandas as pd

    df = pd.read_csv(csv_path, dtype=str)
    df = df.sort_values("timestamp", kind="stable")
    return df.to_dict("records")


def _cli() -> None:  # pragma: no cover
    p = argparse.ArgumentParser(description="Retailrocket -> Kafka traffic generator")
    p.add_argument("--speed", type=int, default=None, help="playback speed multiplier")
    p.add_argument("--limit", type=int, default=None, help="max events to produce")
    p.add_argument("--config", type=Path, default=_CONFIG_PATH, help="path to generator.yaml")
    args = p.parse_args()
    serve_metrics(port=8002)
    run_from_config(config_path=args.config, limit=args.limit, speed_override=args.speed)


if __name__ == "__main__":  # pragma: no cover
    _cli()
