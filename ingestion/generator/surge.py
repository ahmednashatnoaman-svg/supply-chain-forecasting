"""Surge schedule — decides the replay speed multiplier at a given time.

Implements Hatem plan Task 1. Pure logic, unit-tested. The traffic generator divides its inter-event
sleep by `multiplier(now)` to simulate viral surges.
"""
from __future__ import annotations

from datetime import datetime


class SurgeSchedule:
    """Holds surge windows and returns the active speed multiplier.

    Args:
        windows: list of (start_iso, end_iso, multiplier) tuples. Outside any window the
            multiplier is 1.0 (normal speed).
    """

    def __init__(self, windows: list[tuple[str, str, float]] | None = None):
        self._windows = [
            (datetime.fromisoformat(s), datetime.fromisoformat(e), float(m))
            for s, e, m in (windows or [])
        ]

    def multiplier(self, ts: datetime) -> float:
        """Return the multiplier active at `ts` (first matching window, else 1.0)."""
        for start, end, mult in self._windows:
            if start <= ts <= end:
                return mult
        return 1.0
