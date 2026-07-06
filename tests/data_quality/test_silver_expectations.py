"""Data-quality gate for the silver zone (Emad plan Task 3). Uses the spark fixture.

Asserts contract invariants: event_time non-null, item_id > 0, event in the allowed set.
"""

import pytest

pytestmark = pytest.mark.data_quality


def test_silver_invariants_hold_on_good_frame(spark):
    from batch.etl.clean_events import ALLOWED_EVENTS, clean_events

    raw = spark.createDataFrame(
        [
            (1609459200000, 1, "view", 10, None),
            (1609459200000, 2, "transaction", 20, 9.99),
        ],
        ["event_time", "visitor_id", "event", "item_id", "price"],
    )
    silver = clean_events(raw)
    rows = silver.collect()
    assert all(r["event_time"] is not None for r in rows)
    assert all(r["item_id"] > 0 for r in rows)
    assert all(r["event"] in ALLOWED_EVENTS for r in rows)
