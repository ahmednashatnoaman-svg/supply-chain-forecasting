"""Streamlit executive command center. Implements Ziad plan Task 5.

Thin entrypoint: navigation + shared config. Each page module owns one view and reads data via
`components.redis_source` (unit-tested). Launch with `make dashboard`.
"""
from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Supply Chain Command Center", page_icon="📈", layout="wide")

st.title("📈 Supply Chain — Demand Forecasting & Dynamic Pricing")
st.caption("Live view of forecasts, autonomous price changes, and supply-chain automation.")

st.markdown(
    """
    Use the sidebar to navigate:
    - **Forecast vs Actual** — baseline demand line vs live streaming demand.
    - **Price Ticker** — autonomous price changes as they happen.
    - **Automation Log** — auto-triggered reorders / alerts.
    """
)

# Streamlit auto-discovers files in `pages/`. This file is the landing page.
