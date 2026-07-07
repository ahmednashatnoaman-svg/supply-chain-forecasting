"""Forecast-vs-actual view. Implements Ziad plan Task 5 (page 1).

Baseline forecast (nightly, from Redis ``forecast:*``) vs live demand (windowed sales velocity from
Redis ``velocity:*:60s``). SKUs are discovered from the live ``price:current:*`` keyspace so the
view tracks whatever the speed layer is actually pricing.
"""

from __future__ import annotations

import plotly.graph_objects as go

DEFAULT_WINDOW = "60s"
_FALLBACK_SKUS = ["10", "20", "30"]


def render(source=None, skus: list[str] | None = None, window: str = DEFAULT_WINDOW) -> go.Figure:
    """Build the forecast-vs-actual grouped bar chart.

    Args:
        source: a redis_source-like module (defaults to the real one). Unit-testable via fakeredis.
        skus: override the SKU list; when ``None`` it is discovered via ``source.list_skus()`` and
            falls back to a small default set when no prices have been published yet.
        window: the velocity window label to read (default ``60s``).
    """
    if source is None:
        from automation.dashboard.components import redis_source as source

    if skus is None:
        skus = source.list_skus() or _FALLBACK_SKUS

    labels = [str(s) for s in skus]
    forecast = [source.get_forecast(s) or 0 for s in skus]
    actual = [source.get_velocity(s, window) or 0 for s in skus]

    fig = go.Figure()
    fig.add_bar(x=labels, y=forecast, name="Baseline forecast")
    fig.add_bar(x=labels, y=actual, name=f"Live velocity ({window})")
    fig.update_layout(
        title="Forecast vs live demand by SKU",
        xaxis_title="SKU",
        yaxis_title="Units",
        barmode="group",
    )
    return fig


if __name__ != "__main__":  # rendered by Streamlit
    try:
        import streamlit as st

        st.header("Forecast vs Actual")
        st.plotly_chart(render(), use_container_width=True)
    except Exception:  # pragma: no cover - importable without a running Streamlit server
        pass
