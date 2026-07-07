"""Typed accessors for the four frozen contracts. EVERY layer imports from here — never hard-code a
topic name, Redis key, or HDFS path. See `contracts/` and `docs/architecture/data-contracts.md`.
"""

from __future__ import annotations

from libs.scf_common.config import settings


class Topics:
    """Kafka topic names. Keep in sync with contracts/avro/topics.yml."""

    LIVE_WEB_TRAFFIC = "live_web_traffic"
    INVENTORY_UPDATES = "inventory_updates"
    SYSTEM_ALERTS = "system_alerts"
    AUTOMATED_PRICING_UPDATES = "automated_pricing_updates"

    ALL = (LIVE_WEB_TRAFFIC, INVENTORY_UPDATES, SYSTEM_ALERTS, AUTOMATED_PRICING_UPDATES)


class RedisKeys:
    """Serving-layer key builders. See contracts/redis/redis-keys.md."""

    @staticmethod
    def forecast(sku: str | int) -> str:
        return f"forecast:{sku}"

    @staticmethod
    def elasticity(sku: str | int) -> str:
        return f"graph:elasticity:{sku}"

    @staticmethod
    def community(sku: str | int) -> str:
        return f"graph:community:{sku}"

    @staticmethod
    def price(sku: str | int) -> str:
        return f"price:current:{sku}"

    @staticmethod
    def inventory(sku: str | int) -> str:
        return f"inventory:{sku}"

    @staticmethod
    def velocity(sku: str | int, window: str) -> str:
        return f"velocity:{sku}:{window}"

    @staticmethod
    def lstm_sequence(sku: str | int) -> str:
        return f"lstm:sequence:{sku}"


class HdfsPaths:
    """Medallion path builders. Base dirs come from settings.hdfs. See contracts/hdfs/medallion.md."""

    @staticmethod
    def _join(base: str, name: str) -> str:
        return f"{settings.hdfs.namenode}{base}/{name}"

    @classmethod
    def bronze(cls, name: str) -> str:
        return cls._join(settings.hdfs.bronze, name)

    @classmethod
    def silver(cls, name: str) -> str:
        return cls._join(settings.hdfs.silver, name)

    @classmethod
    def gold(cls, name: str) -> str:
        return cls._join(settings.hdfs.gold, name)


__all__ = ["Topics", "RedisKeys", "HdfsPaths"]
