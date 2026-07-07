"""Streamlit executive command center. Implements Ziad plan Task 5.

Thin entrypoint: navigation + shared config. Each page module owns one view and reads data via
`components.redis_source` (unit-tested). Launch with `make dashboard`.
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Supply Chain Command Center", page_icon="📈", layout="wide")

st.title("📈 Supply Chain — Demand Forecasting & Dynamic Pricing")
st.caption("Live view of forecasts, autonomous price changes, and supply-chain automation.")


@st.fragment(run_every="5s")
def _kpi_row() -> None:
    """Executive KPI summary, refreshed independently of the rest of the page every 5s.

    `st.fragment(run_every=...)` is native to streamlit>=1.33 (pinned 1.37.1 here) -- it reruns just
    this function on a timer without a full-page rerun or any extra autorefresh dependency, keeping
    the project's zero-cost/OSS-only constraint intact.
    """
    from automation.dashboard.components import redis_source

    kpi = redis_source.get_kpi_summary()
    cols = st.columns(5)
    cols[0].metric("Active SKUs", kpi["active_skus"])
    cols[1].metric(
        "Avg. live price", f"${kpi['avg_price']:.2f}" if kpi["avg_price"] is not None else "—"
    )
    cols[2].metric(
        "Total forecast demand",
        f"{kpi['total_forecast']:.0f}" if kpi["total_forecast"] is not None else "—",
    )
    cols[3].metric("🔥 Surging SKUs", kpi["surge_count"])
    cols[4].metric("⚠️ Low-stock SKUs", kpi["low_stock_count"])


try:
    _kpi_row()
except Exception:  # pragma: no cover - landing page still renders without a running Redis
    st.info("KPI summary unavailable — waiting for Redis / the speed layer to publish data.")

st.divider()
st.markdown(
    """
    Use the sidebar to navigate:
    - **Forecast vs Actual** — baseline demand line vs live streaming demand.
    - **Price Ticker** — autonomous price changes as they happen.
    - **Automation Log** — auto-triggered reorders / alerts.
    """
)

# Streamlit auto-discovers files in `pages/`. This file is the landing page.
