"""Silver -> Gold feature engineering (rolling velocity). Implements Emad plan Task 2."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_features(events: DataFrame) -> DataFrame:
    """Build per-SKU-per-day rolling 7d/30d transaction velocity + calendar features.

    Args:
        events: silver events (event_time timestamp, item_id, event, ...).

    Returns:
        Gold features: item_id, ds(date), velocity_7d, velocity_30d, dow, is_weekend.
    """
    daily = (
        events.filter(F.col("event") == "transaction")
        .withColumn("ds", F.to_date("event_time"))
        .groupBy("item_id", "ds")
        .agg(F.count("*").alias("daily_txn"))
    )
    w7 = Window.partitionBy("item_id").orderBy("ds").rowsBetween(-6, 0)
    w30 = Window.partitionBy("item_id").orderBy("ds").rowsBetween(-29, 0)
    return (
        daily.withColumn("velocity_7d", F.avg("daily_txn").over(w7))
        .withColumn("velocity_30d", F.avg("daily_txn").over(w30))
        .withColumn("dow", F.dayofweek("ds"))
        .withColumn("is_weekend", F.col("dow").isin(1, 7))
        .select("item_id", "ds", "velocity_7d", "velocity_30d", "dow", "is_weekend")
    )
