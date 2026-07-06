"""Forecast-vs-actual view. Implements Ziad plan Task 5 (page 1)."""
from __future__ import annotations

import plotly.graph_objects as go


def render(source=None):
    """Build the forecast-vs-actual figure. `source` defaults to the real redis_source.

    Returns a plotly Figure so the page is unit-testable with a fake source.

    TODO(ziad-plan Task 5): stream live actuals from automated_pricing_updates/velocity keys.
    """
    if source is None:
        from automation.dashboard.components import redis_source as source

    skus = [10, 20, 30]  # TODO: derive from Redis keyspace
    forecast = [source.get_forecast(s) or 0 for s in skus]
    fig = go.Figure()
    fig.add_bar(x=[str(s) for s in skus], y=forecast, name="Baseline forecast")
    fig.update_layout(title="Baseline demand forecast by SKU", xaxis_title="SKU", yaxis_title="Demand")
    return fig


if __name__ != "__main__":  # rendered by Streamlit
    try:
        import streamlit as st

        st.header("Forecast vs Actual")
        st.plotly_chart(render(), use_container_width=True)
    except Exception:  # pragma: no cover - importable without a running Streamlit server
        pass
