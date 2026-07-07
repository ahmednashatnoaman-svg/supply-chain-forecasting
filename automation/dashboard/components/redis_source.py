"""Cached Redis reads for the dashboard. Implements Ziad plan Task 1.

Keeps all serving-layer access in one testable place (rendering stays thin). Uses contract accessors
so a key rename never breaks the dashboard silently.
"""

from __future__ import annotations

import json

from libs.scf_common.contracts import RedisKeys
from libs.scf_common.io import get_redis


def get_price(sku: str | int) -> float | None:
    val = get_redis().get(RedisKeys.price(sku))
    return float(val) if val is not None else None


def get_forecast(sku: str | int) -> float | None:
    val = get_redis().get(RedisKeys.forecast(sku))
    return float(val) if val is not None else None


def get_inventory(sku: str | int) -> int | None:
    val = get_redis().get(RedisKeys.inventory(sku))
    return int(val) if val is not None else None


def get_velocity(sku: str | int, window: str = "60s") -> float | None:
    val = get_redis().get(RedisKeys.velocity(sku, window))
    return float(val) if val is not None else None


def get_elasticity(sku: str | int) -> list[dict]:
    val = get_redis().get(RedisKeys.elasticity(sku))
    return json.loads(val) if val else []


def list_skus() -> list[str]:
    """Discover active SKUs by scanning the ``price:current:*`` keyspace.

    Prices are written by the speed layer for every SKU the stream has priced, so this is the
    authoritative set of "live" SKUs. Returns sorted unique SKU id strings; empty list when no
    prices have been published yet (e.g. before the speed layer runs).
    """
    skus: set[str] = set()
    for key in get_redis().scan_iter(match="price:current:*"):
        # key looks like "price:current:10" -> take the trailing segment
        skus.add(key.rsplit(":", 1)[-1])
    return sorted(skus, key=lambda s: int(s) if s.isdigit() else s)
