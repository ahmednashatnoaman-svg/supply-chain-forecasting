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
    bootstrap_servers: str = field(default_factory=lambda: _get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    schema_registry_url: str = field(default_factory=lambda: _get("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081"))


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
    app_name: str = field(default_factory=lambda: _get("SPARK_APP_NAME", "supply-chain-forecasting"))
    executor_memory: str = field(default_factory=lambda: _get("SPARK_EXECUTOR_MEMORY", "2g"))
    driver_memory: str = field(default_factory=lambda: _get("SPARK_DRIVER_MEMORY", "2g"))


@dataclass(frozen=True)
class PricingSettings:
    elasticity_coeff: float = field(default_factory=lambda: float(_get("PRICE_ELASTICITY_COEFF", "0.35")))
    max_uplift_pct: float = field(default_factory=lambda: float(_get("PRICE_MAX_UPLIFT_PCT", "0.25")))
    min_margin_pct: float = field(default_factory=lambda: float(_get("PRICE_MIN_MARGIN_PCT", "0.10")))
    surge_threshold: float = field(default_factory=lambda: float(_get("SURGE_VELOCITY_THRESHOLD", "2.0")))
    window_seconds: int = field(default_factory=lambda: int(_get("STREAMING_WINDOW_SECONDS", "60")))


@dataclass(frozen=True)
class AutomationSettings:
    n8n_webhook_url: str = field(default_factory=lambda: _get("N8N_WEBHOOK_URL", "http://localhost:5678/webhook/reorder"))
    supplier_email: str = field(default_factory=lambda: _get("SUPPLIER_EMAIL", "supplier@example.com"))
    reorder_threshold: int = field(default_factory=lambda: int(_get("INVENTORY_REORDER_THRESHOLD", "50")))


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
