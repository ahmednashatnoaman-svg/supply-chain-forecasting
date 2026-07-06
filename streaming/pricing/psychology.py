"""Psychological price *presentation* — charm endings + honest anchoring.

Separated from `formula.py` on purpose: the formula decides the economically optimal price; this module
only governs how that number is *presented*. Ethical guardrails (from the price-psychology skill):

  * NEVER present a price higher than the computed price (charm rounding only goes down/equal → trust).
  * NEVER fabricate a "was" anchor. The anchor is the real catalog `base_price`, shown only when the
    dynamic price genuinely differs.
  * Protect the quality signal: `min_display_floor_pct` prevents presentation from cheapening a premium.

References: Ariely et al. 2003 (anchoring/decoys), left-digit effect (charm pricing).
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class PsychologyConfig:
    """Presentation tunables."""

    enabled: bool = True
    charm_ending: float = 0.99  # 0.99/0.95 for value framing; 0.00 for premium/round-number signal
    min_display_floor_pct: float = 0.0  # never present below base*(1+this); protects quality signal


def apply_charm_ending(price: float, ending: float = 0.99) -> float:
    """Round a price to a charm ending WITHOUT ever exceeding the input price.

    Examples: 121.00 -> 120.99 · 100.00 -> 99.99 · 121.99 -> 121.99 · ending 0.00: 121.37 -> 121.00.
    """
    whole = math.floor(price)
    candidate = whole + ending
    if candidate > price:
        candidate = whole - 1 + ending
    return round(max(candidate, 0.0), 2)


@dataclass(frozen=True)
class PricePresentation:
    display_price: float
    anchor_price: float | None  # real base price shown as reference, or None if unchanged
    pct_change: float
    note: str


def present_price(new_price: float, base_price: float, cfg: PsychologyConfig) -> PricePresentation:
    """Produce an honest, psychology-aware presentation of a computed price.

    Args:
        new_price: the economically computed price from `dynamic_price`.
        base_price: the real catalog price (used as the ONLY legitimate anchor).
        cfg: presentation configuration.

    Returns:
        PricePresentation with a charm-rounded display price and, when the price genuinely moved, the
        real base price as an anchor.
    """
    if not cfg.enabled:
        return PricePresentation(round(new_price, 2), None, _pct(new_price, base_price), "raw")

    floor = base_price * (1 + cfg.min_display_floor_pct)
    guarded = max(new_price, floor)  # quality-signal protection
    display = apply_charm_ending(guarded, cfg.charm_ending)

    # "changed" is judged on the ECONOMIC price (pre-charm), so charm rounding never invents an anchor.
    changed = abs(guarded - base_price) >= 0.01
    anchor = round(base_price, 2) if changed else None
    note = (
        "surge-premium"
        if changed and guarded > base_price
        else ("adjusted" if changed else "baseline")
    )
    return PricePresentation(display, anchor, _pct(display, base_price), note)


def _pct(new: float, base: float) -> float:
    return round((new - base) / base * 100, 2) if base else 0.0
