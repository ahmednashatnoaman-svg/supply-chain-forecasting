"""Unit test for inventory derivation from Retailrocket's `available` property (fills the
previously-unfilled RedisKeys.inventory gap -- see batch/etl/inventory.py)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def test_build_inventory_takes_latest_available_value_per_item(spark):
    from pyspark.sql.types import StringType, StructField, StructType

    from batch.etl.inventory import IN_STOCK_BASELINE, build_inventory

    schema = StructType(
        [
            StructField("timestamp", StringType()),
            StructField("itemid", StringType()),
            StructField("property", StringType()),
            StructField("value", StringType()),
        ]
    )
    rows = [
        # item 1: goes out of stock -> latest value (available=0) should win over the earlier 1
        ("1000", "1", "available", "1"),
        ("2000", "1", "available", "0"),
        # item 2: restocked -> latest value (available=1) should win over the earlier 0
        ("1000", "2", "available", "0"),
        ("2000", "2", "available", "1"),
        # item 3: unrelated property must be ignored, not mistaken for availability
        ("3000", "3", "categoryid", "42"),
    ]
    df = spark.createDataFrame(rows, schema)
    out = {r["item_id"]: r["stock"] for r in build_inventory(df).collect()}

    assert out[1] == 0
    assert out[2] == IN_STOCK_BASELINE
    assert 3 not in out  # no `available` row for item 3 at all
