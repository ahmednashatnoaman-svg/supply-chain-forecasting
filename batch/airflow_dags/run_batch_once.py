"""Run the nightly batch pipeline once, locally, without Airflow. Used by `make batch`.

Wires the batch modules end-to-end against the live cluster. Implements Emad plan Task 7 (runner).
"""

from __future__ import annotations

from pyspark.sql import functions as F

from libs.scf_common.contracts import HdfsPaths
from libs.scf_common.io import get_spark
from libs.scf_common.observability import get_logger, serve_metrics

log = get_logger("batch.run_once")


def _read_bronze_events(spark):
    """Read the raw Retailrocket events CSV staged in bronze and rename to the silver contract.

    scripts/hdfs_load.sh stages the dataset's own CSV columns as-is (timestamp/visitorid/itemid);
    clean_events expects event_time/visitor_id/item_id, so this does that one rename + cast.
    """
    raw = spark.read.option("header", "true").csv(HdfsPaths.bronze("events"))
    return raw.select(
        F.col("timestamp").cast("long").alias("event_time"),
        F.col("visitorid").cast("long").alias("visitor_id"),
        F.col("event"),
        F.col("itemid").cast("long").alias("item_id"),
    )


def main() -> None:  # pragma: no cover - needs Spark + HDFS
    """Execute clean -> features -> forecast -> graph -> publish once."""
    from batch.etl.clean_events import clean_events
    from batch.etl.expectations import validate_silver
    from batch.etl.features import build_features
    from batch.etl.inventory import build_inventory
    from batch.graph.elasticity import build_graph
    from batch.mllib.forecast import train_forecast
    from batch.publish.to_redis import publish_forecasts

    serve_metrics(port=8001)
    spark = get_spark("batch")
    log.info("batch.run_once.start", bronze=HdfsPaths.bronze("events"))

    # 1. Clean Events
    bronze = _read_bronze_events(spark)
    silver = clean_events(bronze)
    silver.write.parquet(HdfsPaths.silver("events"), mode="overwrite")

    # 2. Build Features
    silver_df = spark.read.parquet(HdfsPaths.silver("events"))
    features = build_features(silver_df)
    features.write.parquet(HdfsPaths.gold("features"), mode="overwrite")

    # 3. Data Quality Gate
    validate_silver(silver_df)

    # 4. Train Forecast
    features_df = spark.read.parquet(HdfsPaths.gold("features"))
    model, preds = train_forecast(features_df)
    preds.write.parquet(HdfsPaths.gold("forecast"), mode="overwrite")

    # 5. Build Graph
    # cooccurrence_edges self-joins on visitor_id -- it must only see transaction rows (its own
    # docstring's contract). Passing the full silver_df (views + addtocart + transactions) blows
    # the join up combinatorially across every click a visitor ever made, not just their
    # purchases, and OOMs the executor on the real dataset's view-heavy event mix.
    transactions = silver_df.filter(F.col("event") == "transaction")
    edges = build_graph(transactions)
    edges.write.parquet(HdfsPaths.gold("graph"), mode="overwrite")

    # 6. Build Inventory (from Retailrocket's real item_properties `available` signal)
    item_properties = spark.read.option("header", "true").csv(HdfsPaths.bronze("item_properties"))
    inventory = build_inventory(item_properties)
    inventory.write.parquet(HdfsPaths.gold("inventory"), mode="overwrite")

    # 7. Publish to Redis
    preds_df = spark.read.parquet(HdfsPaths.gold("forecast"))
    graph_df = spark.read.parquet(HdfsPaths.gold("graph"))
    inventory_df = spark.read.parquet(HdfsPaths.gold("inventory"))
    publish_forecasts(preds_df, graph_df, inventory_df)

    log.info("batch.run_once.done")
    spark.stop()


if __name__ == "__main__":
    main()
