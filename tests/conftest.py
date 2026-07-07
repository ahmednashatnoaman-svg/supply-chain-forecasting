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
        # spark-avro is needed by any test exercising from_avro (e.g. parse_events) -- production
        # code gets this via get_spark(), but tests use this lighter fixture instead.
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1")
        .getOrCreate()
    )
    yield session
    session.stop()
