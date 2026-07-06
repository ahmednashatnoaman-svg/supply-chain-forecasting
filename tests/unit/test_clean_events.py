"""Unit test for bronze->silver cleansing (Emad plan Task 1). Uses the local spark fixture."""

import pytest

pytestmark = pytest.mark.unit


def test_drops_dupes_and_casts(spark):
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    from batch.etl.clean_events import clean_events

    # Explicit schema: the `price` column is all-null here, so type must be declared (Spark can't
    # infer an all-None column). This mirrors how the real ETL reads typed Parquet.
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
            (1609459200000, 1, "view", 10, None),  # duplicate
            (1609459200000, 2, "bogus", 11, None),  # filtered (bad event)
            (1609459200000, 3, "transaction", 0, None),  # filtered (item_id <= 0)
        ],
        schema,
    )
    out = clean_events(raw)
    assert out.count() == 1
    assert dict(out.dtypes)["event_time"] == "timestamp"
