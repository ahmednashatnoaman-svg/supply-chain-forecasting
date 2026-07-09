"""Smoke + behavior tests for the Streamlit dashboard pages (Ziad plan Task 5).

Two layers:
- ``test_*_renders_a_figure_from_fake_redis`` — call ``render()`` with a fake Redis snapshot
  (no Kafka), exactly as the plan's Step 1 specifies.
- ``test_*_renders_live_events`` — call ``render(events=[...synthetic...])`` and assert the figure
  / table reflects the live Kafka data the page is meant to tail.
"""

from __future__ import annotations

import importlib

import pytest

fakeredis = pytest.importorskip("fakeredis")
plotly = pytest.importorskip("plotly.graph_objects")

pytestmark = pytest.mark.unit


@pytest.fixture()
def fake_source(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    client.set("price:current:10", 19.99)
    client.set("price:current:20", 24.50)
    client.set("price:current:30", 9.99)
    client.set("forecast:10", 120.0)
    client.set("forecast:20", 80.0)
    client.set("forecast:30", 40.0)
    client.set("velocity:10:60s", 15.0)
    client.set("velocity:20:60s", 90.0)
    client.set("velocity:30:60s", 5.0)  # list_skus() keys off velocity:*, not price:current:*

    from automation.dashboard.components import redis_source

    monkeypatch.setattr(redis_source, "get_redis", lambda: client, raising=True)
    return redis_source


def _page(name: str):
    return importlib.import_module(f"automation.dashboard.pages.{name}")


# ---- page 1: forecast vs actual ----


def test_forecast_vs_actual_renders_grouped_bars_from_fake_redis(fake_source):
    fig = _page("1_forecast_vs_actual").render(fake_source)
    assert isinstance(fig, plotly.Figure)
    # two bars: baseline forecast + live velocity
    assert len(fig.data) == 2
    names = {tr.name for tr in fig.data}
    assert "Baseline forecast" in names
    assert "Live velocity (60s)" in names


def test_forecast_vs_actual_uses_discovered_skus(fake_source):
    fig = _page("1_forecast_vs_actual").render(fake_source)
    # the fake Redis has velocity for SKUs 10/20/30, shown most-active-by-velocity first
    # (10->15.0, 20->90.0, 30->5.0)
    assert list(fig.data[0].x) == ["20", "10", "30"]


def test_forecast_vs_actual_respects_window(fake_source):
    (
        fake_source.get_velocity.cache_clear()
        if hasattr(fake_source.get_velocity, "cache_clear")
        else None
    )
    fig = _page("1_forecast_vs_actual").render(fake_source, window="300s")
    assert "300s" in fig.data[1].name


# ---- page 2: price ticker ----


def test_price_ticker_falls_back_to_redis_snapshot(fake_source):
    fig = _page("2_price_ticker").render(source=fake_source)
    assert isinstance(fig, plotly.Figure)
    assert len(fig.data) == 1  # one bar series (snapshot)
    # most-active-by-velocity first (10->15.0, 20->90.0, 30->5.0), same ordering as page 1
    assert list(fig.data[0].x) == ["20", "10", "30"]


def test_price_ticker_renders_live_time_series():
    events = [
        {"event_time": 1700000000000, "item_id": 10, "new_price": 19.99},
        {"event_time": 1700000001000, "item_id": 10, "new_price": 21.49},
        {"event_time": 1700000002000, "item_id": 20, "new_price": 30.0},
    ]
    fig = _page("2_price_ticker").render(events=events)
    assert isinstance(fig, plotly.Figure)
    # one scatter trace per SKU
    assert len(fig.data) == 2
    by_name = {tr.name: tr for tr in fig.data}
    assert "SKU 10" in by_name
    assert len(by_name["SKU 10"].y) == 2  # two price points for SKU 10
    assert list(by_name["SKU 10"].y) == [19.99, 21.49]


def test_price_ticker_empty_events_falls_back_to_redis(fake_source):
    fig = _page("2_price_ticker").render(events=[], source=fake_source)
    assert isinstance(fig, plotly.Figure)


# ---- page 3: automation log ----


def test_automation_log_idle_when_no_events():
    rows = _page("3_automation_log").render()
    assert rows[0]["message"].startswith("waiting for system_alerts")


def test_automation_log_renders_live_alerts():
    events = [
        {
            "event_time": 1700000000000,
            "item_id": 10,
            "alert_type": "LOW_STOCK",
            "message": "stock critically low during surge",
        },
        {
            "event_time": 1700000005000,
            "item_id": 20,
            "alert_type": "SURGE",
            "message": "velocity spike detected",
        },
    ]
    rows = _page("3_automation_log").render(events=events)
    assert len(rows) == 2
    assert rows[0]["sku"] == 10
    assert rows[0]["alert"] == "LOW_STOCK"
    assert rows[1]["alert"] == "SURGE"


def test_automation_log_formats_event_time():
    events = [{"event_time": 1700000000000, "item_id": 1, "alert_type": "SURGE", "message": "x"}]
    rows = _page("3_automation_log").render(events=events)
    assert rows[0]["time"] != "—"


# ---- at least one page produces a plotly figure (plan Step 1) ----


def test_at_least_one_page_produces_a_plotly_figure(fake_source):
    figures = [
        isinstance(_page("1_forecast_vs_actual").render(fake_source), plotly.Figure),
        isinstance(_page("2_price_ticker").render(source=fake_source), plotly.Figure),
    ]
    assert any(figures), "expected at least one page to produce a plotly Figure"
