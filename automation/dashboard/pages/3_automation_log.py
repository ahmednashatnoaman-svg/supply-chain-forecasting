"""Automation log view. Implements Ziad plan Task 5 (page 3).

Tails ``system_alerts`` (low-stock / surge / price-cap alerts from the speed layer) into a live
audit table. The Streamlit run calls :func:`collect_alerts` then :func:`render`; unit tests call
:func:`render` directly with synthetic events so no broker is required.
"""

from __future__ import annotations

from datetime import datetime, timezone

_IDLE = [
    {"time": "—", "sku": "—", "alert": "—", "icon": "⏳", "message": "waiting for system_alerts…"}
]

_ALERT_ICONS = {"LOW_STOCK": "⚠️", "SURGE": "🔥"}


def _fmt_time(event_time: int | datetime | None) -> str:
    try:
        # The contract's `event_time` is Avro logical type timestamp-millis, which the
        # schema-registry deserializer decodes to a `datetime` in real Kafka use; unit tests pass
        # plain ints. Without this branch, real alerts always fell into the except below and
        # silently rendered "—" instead of the actual time.
        if isinstance(event_time, datetime):
            dt = event_time if event_time.tzinfo else event_time.replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromtimestamp(int(event_time) / 1000, tz=timezone.utc)
        return dt.strftime("%H:%M:%S")
    except (TypeError, ValueError, OSError):
        return "—"


def render(events: list[dict] | None = None) -> list[dict]:
    """Return a list-of-dicts table of automation events.

    Args:
        events: a list of ``SystemAlert`` dicts (from :func:`collect_alerts`). When ``None`` or
            empty an idle placeholder row is returned.
    """
    if not events:
        return list(_IDLE)
    rows: list[dict] = []
    for e in events:
        alert_type = e.get("alert_type")
        rows.append(
            {
                "time": _fmt_time(e.get("event_time")),
                "sku": e.get("item_id"),
                "alert": alert_type,
                "icon": _ALERT_ICONS.get(alert_type, "ℹ️"),
                "message": (e.get("message") or "")[:80],
            }
        )
    return rows


if __name__ == "__main__":  # rendered by Streamlit
    # Streamlit execs every script it runs -- both app.py and each pages/*.py -- with
    # __name__ == "__main__" (verified against the installed streamlit; there is no dotted
    # module name at runtime). Unit tests import this file as a real submodule instead
    # (`automation.dashboard.pages.3_automation_log`), so this guard skips the live-rendering side
    # effects during import while still running them under a real `streamlit run`.
    try:
        import streamlit as st

        from automation.dashboard.components.kafka_source import collect_alerts

        st.header("Supply-Chain Automation Log")

        @st.fragment(run_every="5s")
        def _live_log() -> None:
            alerts = collect_alerts()
            st.table(render(events=alerts))

        _live_log()
    except Exception:  # pragma: no cover - importable without a running Streamlit server
        pass
