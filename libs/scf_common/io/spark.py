"""Spark session builder with the GraphFrames + Kafka + Avro packages preconfigured."""
from __future__ import annotations

from pyspark.sql import SparkSession

from libs.scf_common.config import settings

# Free OSS packages pulled at runtime (Maven central); pinned for reproducibility.
_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.spark:spark-avro_2.12:3.5.1",
        "graphframes:graphframes:0.8.3-spark3.5-s_2.12",
    ]
)


def get_spark(app_suffix: str = "") -> SparkSession:
    """Build (or get) a SparkSession for a layer job.

    Args:
        app_suffix: appended to the base app name to distinguish jobs in the UI.
    """
    name = settings.spark.app_name + (f"-{app_suffix}" if app_suffix else "")
    builder = (
        SparkSession.builder.appName(name)
        .master(settings.spark.master)
        .config("spark.jars.packages", _PACKAGES)
        .config("spark.executor.memory", settings.spark.executor_memory)
        .config("spark.driver.memory", settings.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", "8")  # local-friendly default
    )
    return builder.getOrCreate()
