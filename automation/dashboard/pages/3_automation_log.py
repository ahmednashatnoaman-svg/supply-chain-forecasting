"""Automation log view. Implements Ziad plan Task 5 (page 3)."""

from __future__ import annotations


def render(events=None):
    """Return a list-of-dicts table of automation events (unit-testable).

    TODO(ziad-plan Task 5): consume system_alerts + n8n webhook receipts for a live audit log.
    """
    return events or [
        {"time": "—", "sku": "—", "action": "waiting for system_alerts…", "status": "idle"},
    ]


if __name__ != "__main__":
    try:
        import streamlit as st

        st.header("Supply-Chain Automation Log")
        st.table(render())
    except Exception:  # pragma: no cover
        pass
