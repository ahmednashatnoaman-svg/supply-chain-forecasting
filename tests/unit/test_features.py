"""Unit test for rolling velocity feature engineering (Emad plan Task 2).

Feeds 40 consecutive days of one SKU's transactions where day `d` has exactly `d` transactions, then
verifies `velocity_7d`/`velocity_30d` on the last day match hand-computed rolling means.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pytestmark = pytest.mark.unit


def test_velocity_7d_and_30d_match_hand_computed_rolling_mean(spark):
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    from batch.etl.features import build_features

    schema = StructType(
        [
            StructField("event_time", TimestampType()),
            StructField("visitor_id", LongType()),
            StructField("event", StringType()),
            StructField("item_id", LongType()),
            StructField("price", DoubleType()),
        ]
    )

    base = datetime(2024, 1, 1)
    rows = []
    for day in range(1, 41):  # 40 days; day d has exactly d transactions
        ts = base + timedelta(days=day - 1)
        for visitor in range(day):
            rows.append((ts, visitor, "transaction", 100, 9.99))

    df = spark.createDataFrame(rows, schema)
    out = {r["ds"]: r for r in build_features(df).collect()}

    last_day = (base + timedelta(days=39)).date()
    last_row = out[last_day]

    expected_7d = sum(range(34, 41)) / 7  # days 34..40 -> 37.0
    expected_30d = sum(range(11, 41)) / 30  # days 11..40 -> 25.5

    assert last_row["velocity_7d"] == pytest.approx(expected_7d)
    assert last_row["velocity_30d"] == pytest.approx(expected_30d)
    assert last_row["dow"] is not None
    assert isinstance(last_row["is_weekend"], bool)


def test_early_days_have_shorter_rolling_window(spark):
    """Day 1 has no history: both rolling means equal day 1's own count."""
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    from batch.etl.features import build_features

    schema = StructType(
        [
            StructField("event_time", TimestampType()),
            StructField("visitor_id", LongType()),
            StructField("event", StringType()),
            StructField("item_id", LongType()),
            StructField("price", DoubleType()),
        ]
    )
    base = datetime(2024, 1, 1)
    rows = [(base, 1, "transaction", 200, 5.0), (base, 2, "transaction", 200, 5.0)]
    df = spark.createDataFrame(rows, schema)
    out = build_features(df).collect()

    assert len(out) == 1
    assert out[0]["velocity_7d"] == 2.0
    assert out[0]["velocity_30d"] == 2.0
