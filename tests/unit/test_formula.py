"""Unit tests for the dynamic pricing formula (Nashat plan Task 3). Pure."""

import pytest

from streaming.pricing.formula import PricingConfig, dynamic_price

pytestmark = pytest.mark.unit

CFG = PricingConfig(
    elasticity_coeff=0.35, max_uplift_pct=0.25, min_margin_pct=0.10, surge_threshold=2.0
)


def test_surge_raises_but_capped():
    p = dynamic_price(
        base_price=100, velocity=500, baseline=100, elasticity=0.8, is_surge=True, cfg=CFG
    )
    assert 100 < p <= 125  # uplift capped at +25%


def test_no_surge_returns_base():
    assert dynamic_price(100, 90, 100, 0.8, False, CFG) == 100


def test_below_threshold_returns_base_even_if_surge_flag():
    # velocity only 1.5x baseline < surge_threshold 2.0 -> no move
    assert dynamic_price(100, 150, 100, 0.2, True, CFG) == 100


def test_uplift_capped_at_max():
    # extreme velocity, low elasticity -> hits the +25% cap exactly
    assert dynamic_price(100, 100000, 100, 0.0, True, CFG) == 125.0


def test_zero_baseline_is_safe():
    assert dynamic_price(100, 500, 0, 0.5, True, CFG) == 100
