"""Forecast-vs-actual view. Implements Ziad plan Task 5 (page 1).

Baseline forecast (nightly, from Redis ``forecast:*``) vs live demand (windowed sales velocity from
Redis ``velocity:*:60s``). SKUs are discovered from the ``velocity:*`` keyspace so the view tracks
whatever the speed layer has actually processed recently.
"""

from __future__ import annotations

import plotly.graph_objects as go

DEFAULT_WINDOW = "60s"
_FALLBACK_SKUS = ["10", "20", "30"]
# A bar per SKU stops being readable well before real-dataset scale (tens of thousands of active
# SKUs) -- show the most active ones by live velocity, which is what this chart is actually for.
_MAX_SKUS_SHOWN = 30


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
        skus = source.list_skus(window) or _FALLBACK_SKUS

    forecasts = source.get_forecasts(skus)
    velocities = source.get_velocities(skus, window)

    # Most active by live velocity first, capped to stay chart-readable.
    shown = sorted(skus, key=lambda s: velocities.get(s, 0), reverse=True)[:_MAX_SKUS_SHOWN]
    labels = [str(s) for s in shown]
    forecast = [forecasts.get(s, 0) for s in shown]
    actual = [velocities.get(s, 0) for s in shown]

    fig = go.Figure()
    fig.add_bar(x=labels, y=forecast, name="Baseline forecast")
    fig.add_bar(x=labels, y=actual, name=f"Live velocity ({window})")
    fig.update_layout(
        title=f"Forecast vs live demand — top {len(shown)} SKUs by live velocity",
        xaxis_title="SKU",
        yaxis_title="Units",
        barmode="group",
    )
    return fig


if __name__ == "__main__":  # rendered by Streamlit
    # Streamlit execs every script it runs -- both app.py and each pages/*.py -- with
    # __name__ == "__main__" (verified against the installed streamlit; there is no dotted
    # module name at runtime). Unit tests import this file as a real submodule instead
    # (`automation.dashboard.pages.1_forecast_vs_actual`), so this guard skips the live-rendering
    # side effects during import while still running them under a real `streamlit run`.
    try:
        import streamlit as st

        st.header("Forecast vs Actual")

        @st.fragment(run_every="5s")
        def _live_chart() -> None:
            st.plotly_chart(render(), use_container_width=True)

        _live_chart()
    except Exception:  # pragma: no cover - importable without a running Streamlit server
        pass
