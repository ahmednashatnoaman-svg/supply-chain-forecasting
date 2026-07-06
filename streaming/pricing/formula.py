"""Dynamic pricing formula — pure, deterministic, fully unit-tested (no I/O).

Implements Nashat plan Task 3. The streaming job calls `dynamic_price` per SKU per window.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PricingConfig:
    """Business tunables (mirrors libs.scf_common.config.PricingSettings)."""

    elasticity_coeff: float = 0.35
    max_uplift_pct: float = 0.25
    min_margin_pct: float = 0.10
    surge_threshold: float = 2.0


def dynamic_price(
    base_price: float,
    velocity: float,
    baseline: float,
    elasticity: float,
    is_surge: bool,
    cfg: PricingConfig,
) -> float:
    """Compute the new price for a SKU.

    Logic:
      * If not a surge, or velocity is at/below `surge_threshold * baseline`, return `base_price`.
      * On a surge, uplift scales with how far velocity exceeds baseline, damped by cross-elasticity
        (highly connected items move less to avoid cannibalization), capped at `max_uplift_pct`.
      * Never price below `base_price * (1 + min_margin_pct - max_uplift_pct)` floor safety.

    Args:
        base_price: current catalog price.
        velocity: windowed sales velocity (units/window).
        baseline: MLlib baseline demand for the SKU (>0).
        elasticity: normalized cross-elasticity weight in [0,1]; higher = more connected = move less.
        is_surge: LSTM surge flag.
        cfg: pricing configuration.

    Returns:
        The new price, rounded to 2 decimals.
    """
    if baseline <= 0:
        return round(base_price, 2)

    ratio = velocity / baseline
    if not is_surge or ratio <= cfg.surge_threshold:
        return round(base_price, 2)

    # How far past the surge threshold we are (>=0), damped by elasticity connectivity.
    excess = ratio - cfg.surge_threshold
    raw_uplift = cfg.elasticity_coeff * excess * (1.0 - min(max(elasticity, 0.0), 1.0))
    uplift = min(raw_uplift, cfg.max_uplift_pct)
    return round(base_price * (1.0 + uplift), 2)
