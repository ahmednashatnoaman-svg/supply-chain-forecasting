"""Unit test for bronze->silver cleansing (Emad plan Task 1). Uses the local spark fixture."""

import pytest

pytestmark = pytest.mark.unit


def test_drops_dupes_and_casts(spark):
    from batch.etl.clean_events import clean_events

    raw = spark.createDataFrame(
        [
            (1609459200000, 1, "view", 10, None),
            (1609459200000, 1, "view", 10, None),  # duplicate
            (1609459200000, 2, "bogus", 11, None),  # filtered (bad event)
            (1609459200000, 3, "transaction", 0, None),  # filtered (item_id <= 0)
        ],
        ["event_time", "visitor_id", "event", "item_id", "price"],
    )
    out = clean_events(raw)
    assert out.count() == 1
    assert dict(out.dtypes)["event_time"] == "timestamp"
