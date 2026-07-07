"""Run the nightly batch pipeline once, locally, without Airflow. Used by `make batch`.

Wires the batch modules end-to-end against the live cluster. Implements Emad plan Task 7 (runner).
"""

from __future__ import annotations

from libs.scf_common.contracts import HdfsPaths
from libs.scf_common.io import get_spark
from libs.scf_common.observability import get_logger

log = get_logger("batch.run_once")


def main() -> None:  # pragma: no cover - needs Spark + HDFS
    """Execute clean -> features -> forecast -> graph -> publish once.

    TODO(emad-plan): fill readers/writers for each HDFS zone; call publish_forecasts at the end.
    """
    from batch.etl.clean_events import clean_events
    from batch.etl.expectations import validate_silver
    from batch.etl.features import build_features
    from batch.graph.elasticity import build_graph
    from batch.mllib.forecast import train_forecast
    from batch.publish.to_redis import publish_forecasts

    spark = get_spark("batch")
    log.info("batch.run_once.start", bronze=HdfsPaths.bronze("events"))

    # 1. Clean Events
    bronze = spark.read.parquet(HdfsPaths.bronze("events"))
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
    edges = build_graph(silver_df)
    edges.write.parquet(HdfsPaths.gold("graph"), mode="overwrite")

    # 6. Publish to Redis
    preds_df = spark.read.parquet(HdfsPaths.gold("forecast"))
    graph_df = spark.read.parquet(HdfsPaths.gold("graph"))
    publish_forecasts(preds_df, graph_df)

    log.info("batch.run_once.done")
    spark.stop()


if __name__ == "__main__":
    main()
