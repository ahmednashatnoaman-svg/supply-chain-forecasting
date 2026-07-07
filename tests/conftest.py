"""Shared pytest fixtures. The `spark` fixture is session-scoped and local; integration/e2e tests
that need real services use testcontainers within their own modules.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for chispa DataFrame tests. Skips if pyspark is unavailable."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("scf-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # spark-avro (from_avro, e.g. parse_events) and spark-sql-kafka (readStream/read.format
        # "kafka", e.g. the walking-skeleton e2e test) are both needed here -- production code gets
        # these via get_spark()'s _PACKAGES, but tests use this lighter fixture instead.
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1,graphframes:graphframes:0.8.3-spark3.5-s_2.12",
        )
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")
        .getOrCreate()
    )
    yield session
    session.stop()
