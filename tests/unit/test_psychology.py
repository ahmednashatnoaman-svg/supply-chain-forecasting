"""Unit tests for honest price-presentation psychology (Nashat plan Task 3b). Pure."""
import pytest

from streaming.pricing.psychology import PsychologyConfig, apply_charm_ending, present_price

pytestmark = pytest.mark.unit

CFG = PsychologyConfig(enabled=True, charm_ending=0.99, min_display_floor_pct=0.0)


def test_charm_ending_basic():
    assert apply_charm_ending(121.00) == 120.99
    assert apply_charm_ending(100.00) == 99.99
    assert apply_charm_ending(121.99) == 121.99


def test_charm_premium_round_number():
    assert apply_charm_ending(121.37, 0.00) == 121.00


def test_charm_never_exceeds_input():
    for p in (10.0, 10.5, 99.99, 250.01, 7.0):
        assert apply_charm_ending(p) <= p + 1e-9


def test_present_price_surge_has_honest_anchor():
    pr = present_price(121.00, 100.0, CFG)
    assert pr.display_price == 120.99
    assert pr.anchor_price == 100.0  # the REAL base price, not fabricated
    assert pr.note == "surge-premium"
    assert pr.display_price <= 121.00  # never charge more than computed


def test_present_price_unchanged_has_no_anchor():
    pr = present_price(100.00, 100.0, CFG)
    assert pr.anchor_price is None  # no fake anchor when the price did not move
    assert pr.note == "baseline"
