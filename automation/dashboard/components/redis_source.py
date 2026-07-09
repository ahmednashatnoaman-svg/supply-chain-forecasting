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


_SCAN_COUNT = 5000  # SCAN's per-call batch hint; default of 10 turns a ~1M-key DB scan into
# ~100K network round-trips (observed: 37s). 5000 cuts that to a handful of round-trips (<1s).


def list_skus(window: str = "60s") -> list[str]:
    """Discover SKUs the streaming layer has actually processed recently.

    Scoped to ``velocity:*:{window}`` -- written only by the speed layer's per-window
    ``price_row`` -- rather than ``price:current:*``, which ``batch.etl.pricing`` also seeds for
    the *entire* catalog (hundreds of thousands of SKUs, the vast majority never touched by live
    traffic). Scanning that turned every KPI/chart call into a full-catalog operation instead of
    reflecting what's actually live right now. Returns sorted unique SKU id strings; empty list
    before the speed layer has processed its first window.
    """
    skus: set[str] = set()
    for key in get_redis().scan_iter(match=f"velocity:*:{window}", count=_SCAN_COUNT):
        # key looks like "velocity:10:60s" -> take the middle segment
        skus.add(key.split(":")[1])
    return sorted(skus, key=lambda s: int(s) if s.isdigit() else s)


def _bulk(keys: list[str]) -> list[str | None]:
    """One MGET round-trip instead of len(keys) individual GETs."""
    return get_redis().mget(keys) if keys else []


def get_prices(skus: list[str]) -> dict[str, float]:
    vals = _bulk([RedisKeys.price(s) for s in skus])
    return {s: float(v) for s, v in zip(skus, vals, strict=True) if v is not None}


def get_forecasts(skus: list[str]) -> dict[str, float]:
    vals = _bulk([RedisKeys.forecast(s) for s in skus])
    return {s: float(v) for s, v in zip(skus, vals, strict=True) if v is not None}


def get_velocities(skus: list[str], window: str = "60s") -> dict[str, float]:
    vals = _bulk([RedisKeys.velocity(s, window) for s in skus])
    return {s: float(v) for s, v in zip(skus, vals, strict=True) if v is not None}


def get_inventories(skus: list[str]) -> dict[str, int]:
    vals = _bulk([RedisKeys.inventory(s) for s in skus])
    return {s: int(v) for s, v in zip(skus, vals, strict=True) if v is not None}


def get_kpi_summary(window: str = "60s") -> dict:
    """Aggregate the executive-summary KPI row on the landing page (``app.py``).

    Batches each metric into a single MGET across all active SKUs (see the bulk ``get_*``
    helpers above) instead of one Redis round-trip per SKU per metric -- at real dataset scale
    (tens of thousands of active SKUs) the naive per-SKU-loop version took minutes; this takes a
    handful of round-trips regardless of SKU count.

    ``surge_threshold``/``reorder_threshold`` mirror the exact thresholds the speed layer itself
    uses (``streaming.pipeline.pricing_stream.price_row`` / ``check_low_stock_alert``), so a SKU
    counted here as "surging" or "low stock" is one the live pipeline would also flag -- not a
    separately-tuned dashboard heuristic that could drift from production behavior.
    """
    skus = list_skus(window)
    if not skus:
        return {
            "active_skus": 0,
            "avg_price": None,
            "total_forecast": None,
            "surge_count": 0,
            "low_stock_count": 0,
        }

    prices = get_prices(skus)
    forecasts = get_forecasts(skus)
    velocities = get_velocities(skus, window)
    inventories = get_inventories(skus)

    surge_threshold = settings.pricing.surge_threshold
    reorder_threshold = settings.automation.reorder_threshold

    surge_count = sum(
        1
        for s, baseline in forecasts.items()
        if baseline > 0
        and s in velocities
        and (velocities[s] / max(baseline * (settings.pricing.window_seconds / 86400.0), 0.01))
        > surge_threshold
    )
    low_stock_count = sum(1 for inv in inventories.values() if inv < reorder_threshold)

    return {
        "active_skus": len(skus),
        "avg_price": (sum(prices.values()) / len(prices)) if prices else None,
        "total_forecast": sum(forecasts.values()) if forecasts else None,
        "surge_count": surge_count,
        "low_stock_count": low_stock_count,
    }
