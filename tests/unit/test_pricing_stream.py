"""Unit tests for the streaming pricing job's testable pieces (Nashat plan Task 5).

`parse_events`/`compute_velocity` need pyspark (local `spark` fixture, no Kafka needed).
`price_row`/`check_low_stock_alert` need fakeredis (no real Redis needed). `run()`'s orchestration
(readStream/writeStream/foreachBatch wiring) is integration-only — covered by the walking-skeleton
e2e test once a real cluster is available.
"""

from __future__ import annotations

import io
import json

import pytest

fastavro = pytest.importorskip("fastavro")
fakeredis = pytest.importorskip("fakeredis")

from libs.scf_common.io.kafka import load_schema  # noqa: E402
from streaming.lstm.model import N_FEATURES, SEQ_LEN, SurgeLSTM  # noqa: E402
from streaming.pipeline.pricing_stream import (  # noqa: E402
    build_lstm_sequence,
    check_low_stock_alert,
    compute_velocity,
    parse_events,
    price_row,
)

pytestmark = pytest.mark.unit


def _confluent_encode(record: dict, schema: dict) -> bytes:
    """Encode a record with a fake 5-byte Confluent wire-format prefix (magic byte + schema id)."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return b"\x00" + (7).to_bytes(4, "big") + buf.getvalue()


def test_parse_events_strips_confluent_prefix_and_decodes(spark):
    pytest.importorskip("pyspark")
    schema_str = load_schema("live_web_traffic.avsc")
    schema = fastavro.parse_schema(json.loads(schema_str))
    record = {
        "event_time": 1609459200000,
        "visitor_id": 1,
        "event": "view",
        "item_id": 10,
        "price": None,
    }
    wire_bytes = _confluent_encode(record, schema)

    raw = spark.createDataFrame([(wire_bytes,)], ["value"])
    out = parse_events(raw, schema_str).collect()

    assert len(out) == 1
    assert out[0]["item_id"] == 10
    assert out[0]["event"] == "view"
    assert out[0]["visitor_id"] == 1


def test_compute_velocity_aggregates_per_sku_per_window(spark):
    pytest.importorskip("pyspark")
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("event_time", LongType()),
            StructField("visitor_id", LongType()),
            StructField("event", StringType()),
            StructField("item_id", LongType()),
            StructField("price", DoubleType()),
        ]
    )
    rows = [
        (1609459200000, 1, "transaction", 10, 9.99),
        (1609459200000, 2, "transaction", 10, 9.99),
        (1609459200000, 3, "view", 10, None),
        (1609459200000, 4, "addtocart", 10, None),
        (1609459200000, 5, "transaction", 20, 5.00),
    ]
    df = spark.createDataFrame(rows, schema)
    out = {r["item_id"]: r for r in compute_velocity(df, window_seconds=60).collect()}

    assert out[10]["velocity"] == 2
    assert out[10]["view_count"] == 1
    assert out[10]["addtocart_count"] == 1
    assert out[20]["velocity"] == 1


def test_build_lstm_sequence_shape():
    seq = build_lstm_sequence(velocity=5.0, view_count=10.0, addtocart_count=2.0, price_delta=0.0)
    assert seq.shape == (SEQ_LEN, N_FEATURES)
    assert (seq[0] == seq[-1]).all()  # tiled -> every row identical


@pytest.fixture
def fake_redis():
    return fakeredis.FakeStrictRedis(decode_responses=True)


def test_price_row_reads_forecast_and_elasticity_from_redis(fake_redis):
    fake_redis.set("forecast:10", 100.0)
    fake_redis.set("graph:elasticity:10", json.dumps([{"related": 11, "weight": 0.5}]))

    result = price_row(
        item_id=10,
        velocity=500.0,
        view_count=50.0,
        addtocart_count=10.0,
        last_price=100.0,
        redis=fake_redis,
        model=None,  # fallback -> surge_prob 0.0 -> no surge -> base price returned
    )

    assert result["item_id"] == 10
    assert result["baseline"] == 100.0
    # model unavailable -> no surge signal -> dynamic_price returns the base unchanged, but
    # present_price ALWAYS applies charm rounding (even to an unchanged price) by design -- see
    # test_present_price_unchanged_has_no_anchor in test_psychology.py. 100.00 -> 99.99.
    assert result["new_price"] == 99.99
    assert result["surge_prob"] == 0.0


def test_price_row_missing_forecast_defaults_to_zero_baseline(fake_redis):
    result = price_row(10, 500.0, 50.0, 10.0, 100.0, fake_redis, None)
    assert result["baseline"] == 0.0
    assert result["new_price"] == 99.99  # dynamic_price safe at baseline<=0; still charm-rounded


def test_price_row_with_real_model_can_signal_surge(fake_redis):
    fake_redis.set("forecast:10", 50.0)  # baseline well below velocity -> ratio > surge_threshold
    model = SurgeLSTM()
    result = price_row(10, 500.0, 400.0, 100.0, 100.0, fake_redis, model)
    assert 0.0 <= result["surge_prob"] <= 1.0
    # The model is randomly initialized, so surge may or may not trigger -- bound the charm-rounded
    # result to the valid range either way: [99.99 no-surge, ~124.99 max +25% uplift charm-rounded].
    assert 95.0 <= result["new_price"] <= 125.0


def test_check_low_stock_alert_fires_below_threshold(fake_redis):
    fake_redis.set("inventory:10", 5)
    alert = check_low_stock_alert(10, fake_redis, reorder_threshold=50)
    assert alert is not None
    assert alert["alert_type"] == "LOW_STOCK"
    assert alert["stock_level"] == 5


def test_check_low_stock_alert_silent_above_threshold(fake_redis):
    fake_redis.set("inventory:10", 500)
    assert check_low_stock_alert(10, fake_redis, reorder_threshold=50) is None


def test_check_low_stock_alert_silent_when_no_inventory_data(fake_redis):
    assert check_low_stock_alert(999, fake_redis, reorder_threshold=50) is None
