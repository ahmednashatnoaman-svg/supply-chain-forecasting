"""Typed configuration loaded once from the environment (.env). Import `settings`.

Uses dataclasses + python-dotenv (zero extra deps) instead of pydantic-settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()  # load .env if present; real env always wins


def _get(key: str, default: str) -> str:
    return os.getenv(key, default)


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = field(
        default_factory=lambda: _get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    schema_registry_url: str = field(
        default_factory=lambda: _get("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    )


@dataclass(frozen=True)
class HdfsSettings:
    namenode: str = field(default_factory=lambda: _get("HDFS_NAMENODE", "hdfs://localhost:9000"))
    bronze: str = field(default_factory=lambda: _get("HDFS_BRONZE", "/data/bronze"))
    silver: str = field(default_factory=lambda: _get("HDFS_SILVER", "/data/silver"))
    gold: str = field(default_factory=lambda: _get("HDFS_GOLD", "/data/gold"))


@dataclass(frozen=True)
class RedisSettings:
    host: str = field(default_factory=lambda: _get("REDIS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(_get("REDIS_PORT", "6379")))
    db: int = field(default_factory=lambda: int(_get("REDIS_DB", "0")))
    password: str | None = field(default_factory=lambda: _get("REDIS_PASSWORD", "") or None)


@dataclass(frozen=True)
class SparkSettings:
    master: str = field(default_factory=lambda: _get("SPARK_MASTER", "local[*]"))
    app_name: str = field(
        default_factory=lambda: _get("SPARK_APP_NAME", "supply-chain-forecasting")
    )
    executor_memory: str = field(default_factory=lambda: _get("SPARK_EXECUTOR_MEMORY", "2g"))
    driver_memory: str = field(default_factory=lambda: _get("SPARK_DRIVER_MEMORY", "2g"))
    # Kryo is the default for RDD-heavy jobs (GraphFrames self-joins in batch/graph/elasticity.py
    # benefit most). NOTE: an EOFException in KryoDeserializationStream.readObject on any collect()
    # was previously (mis)attributed to "Kryo on arm64" here -- the real cause was the driver-side
    # image running a different major JVM than the cluster's executors (see infra/docker/
    # pricing-stream.Dockerfile and batch-pipeline.Dockerfile, which now copy /opt/java/openjdk
    # straight from the same apache/spark:3.5.1 image spark-master/spark-worker run, guaranteeing a
    # byte-identical JVM). SPARK_SERIALIZER is kept as an escape hatch, not a fix for that bug.
    serializer: str = field(
        default_factory=lambda: _get(
            "SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer"
        )
    )
    # Standalone mode gives one app ALL cores on a worker by default (spreadOut). With a single
    # 4-core worker shared between the always-on streaming job and occasional batch/debug runs,
    # that starves the second app indefinitely (Spark master queues it WAITING, no error, no
    # timeout -- see docs/runbooks/orchestration-guide.md §7). Capping each app's share lets both
    # run concurrently instead of requiring manual sequencing.
    cores_max: str | None = field(default_factory=lambda: _get("SPARK_CORES_MAX", "2") or None)


@dataclass(frozen=True)
class PricingSettings:
    elasticity_coeff: float = field(
        default_factory=lambda: float(_get("PRICE_ELASTICITY_COEFF", "0.35"))
    )
    max_uplift_pct: float = field(
        default_factory=lambda: float(_get("PRICE_MAX_UPLIFT_PCT", "0.25"))
    )
    min_margin_pct: float = field(
        default_factory=lambda: float(_get("PRICE_MIN_MARGIN_PCT", "0.10"))
    )
    surge_threshold: float = field(
        default_factory=lambda: float(_get("SURGE_VELOCITY_THRESHOLD", "2.0"))
    )
    window_seconds: int = field(default_factory=lambda: int(_get("STREAMING_WINDOW_SECONDS", "60")))
    checkpoint_dir: str = field(
        default_factory=lambda: _get(
            "STREAMING_CHECKPOINT_DIR", "/tmp/scf-checkpoints/pricing_stream"
        )
    )


@dataclass(frozen=True)
class AutomationSettings:
    n8n_webhook_url: str = field(
        default_factory=lambda: _get("N8N_WEBHOOK_URL", "http://localhost:5678/webhook/reorder")
    )
    n8n_webhook_secret: str = field(
        # Empty by default (no header sent) -- matches the zero-config local demo, where the
        # workflow ships `"active": false` and only alert_bridge.py knows the URL. Set
        # N8N_WEBHOOK_SECRET (both here and as n8n's own WEBHOOK_SHARED_SECRET env var) before
        # wiring this to a real downstream action -- see automation/n8n/reorder_workflow.json.
        default_factory=lambda: _get("N8N_WEBHOOK_SECRET", "")
    )
    supplier_email: str = field(
        default_factory=lambda: _get("SUPPLIER_EMAIL", "supplier@example.com")
    )
    reorder_threshold: int = field(
        default_factory=lambda: int(_get("INVENTORY_REORDER_THRESHOLD", "50"))
    )


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    hdfs: HdfsSettings = field(default_factory=HdfsSettings)
    redis: RedisSettings = field(default_factory=RedisSettings)
    spark: SparkSettings = field(default_factory=SparkSettings)
    pricing: PricingSettings = field(default_factory=PricingSettings)
    automation: AutomationSettings = field(default_factory=AutomationSettings)
    log_level: str = field(default_factory=lambda: _get("LOG_LEVEL", "INFO"))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide singleton settings."""
    return Settings()


settings = get_settings()
