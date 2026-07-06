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


def get_elasticity(sku: str | int) -> list[dict]:
    val = get_redis().get(RedisKeys.elasticity(sku))
    return json.loads(val) if val else []
