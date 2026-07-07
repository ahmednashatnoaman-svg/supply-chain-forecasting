"""Data-quality gate for the silver zone (Emad plan Task 3). Uses the spark fixture.

Asserts contract invariants: event_time non-null, item_id > 0, event in the allowed set.
"""

import pytest

pytestmark = pytest.mark.data_quality


def test_silver_invariants_hold_on_good_frame(spark):
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    from batch.etl.clean_events import clean_events
    from batch.etl.expectations import validate_silver

    schema = StructType(
        [
            StructField("event_time", LongType()),
            StructField("visitor_id", LongType()),
            StructField("event", StringType()),
            StructField("item_id", LongType()),
            StructField("price", DoubleType()),
        ]
    )

    raw = spark.createDataFrame(
        [
            (1609459200000, 1, "view", 10, None),
            (1609459200000, 2, "transaction", 20, 9.99),
        ],
        schema,
    )
    silver = clean_events(raw)

    # Should not raise any exceptions
    assert validate_silver(silver) is True


def test_silver_invariants_fails_on_bad_frame(spark):
    import pytest
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    from batch.etl.expectations import validate_silver

    schema = StructType(
        [
            StructField("event_time", LongType()),
            StructField("visitor_id", LongType()),
            StructField("event", StringType()),
            StructField("item_id", LongType()),
            StructField("price", DoubleType()),
        ]
    )

    bad = spark.createDataFrame(
        [
            (None, 1, "invalid_event", -10, None),
        ],
        schema,
    )
    with pytest.raises(ValueError, match="Silver data quality check failed"):
        validate_silver(bad)
