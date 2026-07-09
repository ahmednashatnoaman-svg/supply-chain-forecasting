"""Integration end-to-end test for Emad's Batch ETL/ML Pipeline."""

from unittest.mock import MagicMock, patch

import fakeredis
import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from libs.scf_common.contracts import HdfsPaths, RedisKeys

pytestmark = pytest.mark.integration


@pytest.fixture
def temp_hdfs(tmp_path, monkeypatch):
    """Override HDFS base paths to point to a local temp directory."""

    class MockHdfsSettings:
        namenode = f"file://{tmp_path}"
        bronze = "/bronze"
        silver = "/silver"
        gold = "/gold"

    class MockSettings:
        hdfs = MockHdfsSettings()
        log_level = "INFO"

    monkeypatch.setattr("libs.scf_common.contracts.settings", MockSettings())
    return tmp_path


@pytest.fixture
def fake_redis_client(monkeypatch):
    """Override get_redis to return a fakeredis instance."""
    r = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("batch.publish.to_redis.get_redis", lambda: r)
    return r


def test_emad_batch_e2e(temp_hdfs, fake_redis_client, spark):
    """Run the entire batch pipeline end-to-end using run_batch_once."""
    # 1. Create synthetic bronze events matching Retailrocket's *raw* CSV shape (the real
    # contract _read_bronze_events reads -- timestamp/visitorid/itemid, no price column, staged
    # as CSV by hdfs_load.sh, not the pre-cleaned silver column names/Parquet this fixture used
    # to write).
    events_schema = StructType(
        [
            StructField("timestamp", LongType()),
            StructField("visitorid", LongType()),
            StructField("event", StringType()),
            StructField("itemid", LongType()),
        ]
    )
    events_data = [
        (1609459200000, 1, "view", 10),
        (1609459200000, 1, "transaction", 10),
        (1609459200000, 1, "transaction", 11),  # co-purchased with 10
        (1609545600000, 2, "transaction", 10),
        (1609545600000, 3, "transaction", 12),
        (1609545600000, 4, "bogus", 10),
        (1609545600000, 5, "transaction", -1),
    ]
    events_df = spark.createDataFrame(events_data, events_schema)
    events_df.write.option("header", "true").csv(HdfsPaths.bronze("events"), mode="overwrite")

    # build_inventory reads item_properties from bronze too -- same raw Retailrocket shape
    # (timestamp/itemid/property/value); one "available" row per item is enough to exercise it.
    props_schema = StructType(
        [
            StructField("timestamp", StringType()),
            StructField("itemid", StringType()),
            StructField("property", StringType()),
            StructField("value", StringType()),
        ]
    )
    props_data = [
        ("1609459200000", "10", "available", "1"),
        ("1609459200000", "11", "available", "1"),
        ("1609459200000", "12", "available", "0"),
    ]
    props_df = spark.createDataFrame(props_data, props_schema)
    props_df.write.option("header", "true").csv(
        HdfsPaths.bronze("item_properties"), mode="overwrite"
    )

    def fake_train_forecast(features_df):
        import pyspark.sql.functions as F

        preds = features_df.select("item_id").distinct().withColumn("forecast_demand", F.lit(42.0))
        return MagicMock(), preds

    with patch("batch.mllib.forecast.train_forecast", side_effect=fake_train_forecast):
        from batch.airflow_dags.run_batch_once import main

        with patch("batch.airflow_dags.run_batch_once.get_spark", return_value=spark):
            main()

    assert fake_redis_client.get(RedisKeys.forecast(10)) == "42.0"
    assert fake_redis_client.get(RedisKeys.forecast(11)) == "42.0"

    import json

    elasticity_10 = json.loads(fake_redis_client.get(RedisKeys.elasticity(10)))
    assert len(elasticity_10) == 1
    assert elasticity_10[0]["related"] == 11

    assert fake_redis_client.get(RedisKeys.community(10)) is not None
