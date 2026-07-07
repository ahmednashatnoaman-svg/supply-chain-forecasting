"""Smoke test: each Streamlit page renders without error against a fake Redis (Ziad plan Task 5).

Imports every page module, calls its ``render()`` with a fake source, and asserts no exception is
raised and that the figure-producing pages return a plotly Figure. Keeps rendering thin and data
access in ``redis_source`` (unit-tested separately).
"""

from __future__ import annotations

import pytest

fakeredis = pytest.importorskip("fakeredis")
plotly = pytest.importorskip("plotly.graph_objects")

pytestmark = pytest.mark.unit


@pytest.fixture()
def fake_source(monkeypatch):
    """A fake Redis-backed source so render() never touches a real server."""
    import importlib

    client = fakeredis.FakeStrictRedis(decode_responses=True)
    client.set("price:current:10", 19.99)
    client.set("price:current:20", 24.50)
    client.set("price:current:30", 9.99)
    client.set("forecast:10", 120.0)
    client.set("forecast:20", 80.0)
    client.set("forecast:30", 40.0)

    from automation.dashboard.components import redis_source

    monkeypatch.setattr(redis_source, "get_redis", lambda: client, raising=True)

    # Pages import redis_source at call-time via `from ... import redis_source as source`, so the
    # module-level monkeypatch is enough. Numeric filenames require importlib (not `from ... import`).
    importlib.import_module("automation.dashboard.pages.1_forecast_vs_actual")
    importlib.import_module("automation.dashboard.pages.2_price_ticker")
    importlib.import_module("automation.dashboard.pages.3_automation_log")


def _page(name: str):
    import importlib

    return importlib.import_module(f"automation.dashboard.pages.{name}")


def test_forecast_vs_actual_page_renders_a_figure(fake_source):
    fig = _page("1_forecast_vs_actual").render(fake_source)
    assert isinstance(fig, plotly.Figure)
    assert len(fig.data) >= 1


def test_price_ticker_page_renders_a_figure(fake_source):
    fig = _page("2_price_ticker").render(fake_source)
    assert isinstance(fig, plotly.Figure)
    assert len(fig.data) >= 1


def test_automation_log_page_renders_without_error(fake_source):
    # page 3 returns a list-of-dicts table, not a figure
    rows = _page("3_automation_log").render()
    assert isinstance(rows, list)
    assert len(rows) >= 1


def test_at_least_one_page_produces_a_plotly_figure(fake_source):
    figures = [
        isinstance(_page("1_forecast_vs_actual").render(fake_source), plotly.Figure),
        isinstance(_page("2_price_ticker").render(fake_source), plotly.Figure),
    ]
    assert any(figures), "expected at least one page to produce a plotly Figure"
