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
    spark = get_spark("batch")
    log.info("batch.run_once.start", bronze=HdfsPaths.bronze("events"))
    # bronze = spark.read.parquet(HdfsPaths.bronze("events"))
    # silver = clean_events(bronze); silver.write.parquet(HdfsPaths.silver("events_cleaned"))
    # features = build_features(silver); ...
    # model, preds = train_forecast(features)
    # edges = build_graph(silver)
    # publish_forecasts(preds, edges)
    log.info("batch.run_once.todo", note="wire readers/writers per emad-plan Task 7")
    spark.stop()


if __name__ == "__main__":
    main()
