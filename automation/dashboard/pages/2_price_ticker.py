"""Live price ticker view. Implements Ziad plan Task 5 (page 2)."""

from __future__ import annotations

import plotly.graph_objects as go


def render(source=None):
    """Build a current-price bar per SKU. Returns a plotly Figure (unit-testable).

    TODO(ziad-plan Task 5): tail automated_pricing_updates for a rolling time-series ticker.
    """
    if source is None:
        from automation.dashboard.components import redis_source as source

    skus = [10, 20, 30]
    prices = [source.get_price(s) or 0 for s in skus]
    fig = go.Figure(go.Bar(x=[str(s) for s in skus], y=prices, name="Current price"))
    fig.update_layout(title="Current dynamic price by SKU", xaxis_title="SKU", yaxis_title="Price")
    return fig


if __name__ != "__main__":
    try:
        import streamlit as st

        st.header("Autonomous Price Ticker")
        st.plotly_chart(render(), use_container_width=True)
    except Exception:  # pragma: no cover
        pass
