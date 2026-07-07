"""Integration end-to-end test for Emad's Batch ETL/ML Pipeline."""

from unittest.mock import MagicMock, patch

import fakeredis
import pytest
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

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
    # 1. Create synthetic bronze events matching the expected schema
    schema = StructType(
        [
            StructField("event_time", LongType()),
            StructField("visitor_id", LongType()),
            StructField("event", StringType()),
            StructField("item_id", LongType()),
            StructField("price", DoubleType()),
        ]
    )

    data = [
        (1609459200000, 1, "view", 10, None),
        (1609459200000, 1, "transaction", 10, 19.99),
        (1609459200000, 1, "transaction", 11, 5.99),  # co-purchased with 10
        (1609545600000, 2, "transaction", 10, 19.99),
        (1609545600000, 3, "transaction", 12, 1.99),
        (1609545600000, 4, "bogus", 10, 19.99),
        (1609545600000, 5, "transaction", -1, 19.99),
    ]

    bronze_df = spark.createDataFrame(data, schema)

    # Write to bronze path
    bronze_path = HdfsPaths.bronze("events")
    bronze_df.write.parquet(bronze_path, mode="overwrite")

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
