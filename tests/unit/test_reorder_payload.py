"""Unit tests for the reorder payload builder (Ziad plan Task 2). Pure."""

import pytest

from automation.n8n.alert_bridge import build_reorder

pytestmark = pytest.mark.unit


def test_build_reorder_refills_to_target():
    alert = {"item_id": 42, "stock_level": 5, "reorder_threshold": 50}
    po = build_reorder(alert, target_stock=200)
    assert po["sku"] == 42
    assert po["qty"] == 195
    assert "42" in po["subject"]
    assert po["supplier_email"]


def test_build_reorder_no_negative_qty():
    alert = {"item_id": 1, "stock_level": 500, "reorder_threshold": 50}
    assert build_reorder(alert, target_stock=200)["qty"] == 0
