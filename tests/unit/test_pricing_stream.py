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
    check_low_stock_alert,
    compute_velocity,
    parse_events,
    price_row,
    reshape_lstm_sequence,
    update_sequence_buffer,
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


def test_update_sequence_buffer_cold_start_tiles_the_only_known_step():
    """A SKU's first-ever window has no history yet -- tiled, same as the old simplification."""
    buffer = update_sequence_buffer(
        [], velocity=5.0, view_count=10.0, addtocart_count=2.0, price_delta=0.0
    )
    assert len(buffer) == SEQ_LEN * N_FEATURES
    seq = reshape_lstm_sequence(buffer)
    assert seq.shape == (SEQ_LEN, N_FEATURES)
    assert (seq[0] == seq[-1]).all()
    assert list(seq[-1]) == [5.0, 10.0, 2.0, 0.0]


def test_update_sequence_buffer_accumulates_real_history_across_windows():
    """Issue #36's actual fix: a second real window pushes in genuine history, not a re-tile."""
    buffer = update_sequence_buffer(
        [], velocity=1.0, view_count=1.0, addtocart_count=1.0, price_delta=0.0
    )
    buffer = update_sequence_buffer(
        buffer, velocity=9.0, view_count=9.0, addtocart_count=9.0, price_delta=0.0
    )
    seq = reshape_lstm_sequence(buffer)

    assert seq.shape == (SEQ_LEN, N_FEATURES)
    # The most recent step is the real second window's data...
    assert list(seq[-1]) == [9.0, 9.0, 9.0, 0.0]
    # ...and the second-to-last step is still the real first window's data (not re-tiled to match
    # the second window) -- proof this is genuine accumulated history, not a per-batch snapshot.
    assert list(seq[-2]) == [1.0, 1.0, 1.0, 0.0]
    # Everything before that is still the cold-start padding from window 1.
    assert list(seq[0]) == [1.0, 1.0, 1.0, 0.0]


def test_update_sequence_buffer_evicts_oldest_step_once_full():
    """After SEQ_LEN real windows, the buffer is 100% genuine history and slides forward."""
    buffer: list[float] = []
    for i in range(SEQ_LEN + 1):
        buffer = update_sequence_buffer(
            buffer, velocity=float(i), view_count=0.0, addtocart_count=0.0, price_delta=0.0
        )
    seq = reshape_lstm_sequence(buffer)

    assert seq.shape == (SEQ_LEN, N_FEATURES)
    # Step 0 (the very first window) has been evicted; the oldest remaining step is window 1.
    assert seq[0][0] == 1.0
    # The newest step is the last window pushed in (index SEQ_LEN).
    assert seq[-1][0] == float(SEQ_LEN)


def test_reshape_lstm_sequence_matches_model_contract():
    flat = [0.0] * (SEQ_LEN * N_FEATURES)
    seq = reshape_lstm_sequence(flat)
    assert seq.shape == (SEQ_LEN, N_FEATURES)


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


def test_price_row_persists_real_sequence_history_to_redis(fake_redis):
    """Issue #36's actual fix, exercised through price_row: a second call for the same SKU reads
    back genuine accumulated history from Redis, not a re-tiled single point.
    """
    price_row(10, 1.0, 1.0, 1.0, 100.0, fake_redis, None)
    stored_after_first = json.loads(fake_redis.get("lstm:sequence:10"))
    assert stored_after_first[-4:] == [1.0, 1.0, 1.0, 0.0]
    assert stored_after_first[-8:-4] == [1.0, 1.0, 1.0, 0.0]  # cold-start tile

    price_row(10, 9.0, 9.0, 9.0, 100.0, fake_redis, None)
    stored_after_second = json.loads(fake_redis.get("lstm:sequence:10"))
    # The most recent step is the second call's real data...
    assert stored_after_second[-4:] == [9.0, 9.0, 9.0, 0.0]
    # ...and the step before it is the FIRST call's real data, not re-tiled -- proof this is
    # genuine accumulated history read back from Redis, not a per-call snapshot.
    assert stored_after_second[-8:-4] == [1.0, 1.0, 1.0, 0.0]


def test_price_row_sequence_history_is_isolated_per_sku(fake_redis):
    price_row(10, 5.0, 5.0, 5.0, 100.0, fake_redis, None)
    price_row(20, 1.0, 1.0, 1.0, 100.0, fake_redis, None)

    seq_10 = json.loads(fake_redis.get("lstm:sequence:10"))
    seq_20 = json.loads(fake_redis.get("lstm:sequence:20"))
    assert seq_10[-4:] == [5.0, 5.0, 5.0, 0.0]
    assert seq_20[-4:] == [1.0, 1.0, 1.0, 0.0]


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
