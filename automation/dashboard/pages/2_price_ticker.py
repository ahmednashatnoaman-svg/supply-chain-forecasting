"""Live price ticker view. Implements Ziad plan Task 5 (page 2).

Tails ``automated_pricing_updates`` (the speed layer's price decisions) into a rolling time-series
of ``new_price`` per SKU. The Streamlit run calls :func:`collect_price_updates` then
:func:`render`; unit tests call :func:`render` directly with synthetic events or a fake Redis
snapshot so no broker is required.
"""

from __future__ import annotations

import plotly.graph_objects as go

_FALLBACK_SKUS = ["10", "20", "30"]


def render(events: list[dict] | None = None, source=None) -> go.Figure:
    """Build the price ticker figure.

    Args:
        events: a list of ``PricingUpdate`` dicts (from :func:`collect_price_updates`). When
            provided, a time-series of ``new_price`` over ``event_time`` is drawn per SKU.
        source: a redis_source-like module used only when ``events`` is empty/None to draw the
            current snapshot of ``price:current:*`` as a bar chart.
    """
    fig = go.Figure()

    if events:
        by_sku: dict[str, list[tuple[int, float]]] = {}
        for e in events:
            by_sku.setdefault(str(e["item_id"]), []).append(
                (int(e["event_time"]), float(e["new_price"]))
            )
        for sku, pts in by_sku.items():
            pts.sort(key=lambda p: p[0])
            fig.add_scatter(
                x=[p[0] for p in pts],
                y=[p[1] for p in pts],
                mode="lines+markers",
                name=f"SKU {sku}",
            )
        fig.update_layout(
            title="Live autonomous price changes",
            xaxis_title="Event time (ms)",
            yaxis_title="Price",
        )
        return fig

    # No live events yet — fall back to the current Redis snapshot.
    if source is None:
        from automation.dashboard.components import redis_source as source

    skus = source.list_skus() or _FALLBACK_SKUS
    prices = [source.get_price(s) or 0 for s in skus]
    fig.add_bar(x=[str(s) for s in skus], y=prices, name="Current price")
    fig.update_layout(
        title="Current dynamic price by SKU (no live updates yet)",
        xaxis_title="SKU",
        yaxis_title="Price",
    )
    return fig


if __name__ != "__main__":  # rendered by Streamlit
    try:
        import streamlit as st

        from automation.dashboard.components.kafka_source import collect_price_updates

        st.header("Autonomous Price Ticker")
        updates = collect_price_updates()
        st.plotly_chart(render(events=updates), use_container_width=True)
    except Exception:  # pragma: no cover - importable without a running Streamlit server
        pass
