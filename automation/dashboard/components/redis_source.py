"""Cached Redis reads for the dashboard. Implements Ziad plan Task 1.

Keeps all serving-layer access in one testable place (rendering stays thin). Uses contract accessors
so a key rename never breaks the dashboard silently.
"""

from __future__ import annotations

import json

from libs.scf_common.config import settings
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


def get_kpi_summary(window: str = "60s") -> dict:
    """Aggregate the executive-summary KPI row on the landing page (``app.py``).

    Reuses the same Redis-backed reads as the per-page views rather than a bespoke query, so the
    landing page never needs its own Kafka/Redis wiring -- it is one `scan_iter` over `list_skus()`
    plus one Redis GET per SKU per metric, which is cheap at demo scale (tens of SKUs).

    ``surge_threshold``/``reorder_threshold`` mirror the exact thresholds the speed layer itself
    uses (``streaming.pipeline.pricing_stream.price_row`` / ``check_low_stock_alert``), so a SKU
    counted here as "surging" or "low stock" is one the live pipeline would also flag -- not a
    separately-tuned dashboard heuristic that could drift from production behavior.
    """
    skus = list_skus()
    prices = [p for s in skus if (p := get_price(s)) is not None]
    forecasts = {s: f for s in skus if (f := get_forecast(s)) is not None}
    velocities = {s: v for s in skus if (v := get_velocity(s, window)) is not None}

    surge_threshold = settings.pricing.surge_threshold
    reorder_threshold = settings.automation.reorder_threshold

    surge_count = sum(
        1
        for s, baseline in forecasts.items()
        if baseline > 0 and s in velocities and (velocities[s] / baseline) > surge_threshold
    )
    low_stock_count = sum(
        1 for s in skus if (inv := get_inventory(s)) is not None and inv < reorder_threshold
    )

    return {
        "active_skus": len(skus),
        "avg_price": (sum(prices) / len(prices)) if prices else None,
        "total_forecast": sum(forecasts.values()) if forecasts else None,
        "surge_count": surge_count,
        "low_stock_count": low_stock_count,
    }
