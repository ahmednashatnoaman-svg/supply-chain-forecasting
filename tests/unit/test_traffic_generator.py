"""Unit tests for the traffic generator's surge-window timing (Hatem plan follow-up, issue #56).

Regression test for a real bug found in code review: `_maybe_sleep` converted the row's epoch-ms
timestamp via `datetime.fromtimestamp()` (host-local timezone) before checking `SurgeSchedule`,
whose window bounds are naive datetimes intended to represent UTC event time. On any host not set
to UTC, every surge window fired hours off from configured.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.generator.surge import SurgeSchedule
from ingestion.generator.traffic_generator import _maybe_sleep

pytestmark = pytest.mark.unit


def test_maybe_sleep_uses_utc_for_surge_window_lookup(monkeypatch):
    """A surge window defined in UTC must apply regardless of the host's local timezone."""
    # 2015-09-15T12:00:00 UTC, expressed as epoch millis (dataset convention).
    event_utc = datetime(2015, 9, 15, 12, 0, 0, tzinfo=timezone.utc)
    cur_ts = int(event_utc.timestamp() * 1000)
    rows = [{"timestamp": cur_ts}, {"timestamp": cur_ts + 1000}]

    surge = SurgeSchedule(windows=[("2015-09-15T11:30:00", "2015-09-15T12:30:00", 10.0)])

    seen_sleep: list[float] = []
    monkeypatch.setattr("time.sleep", lambda s: seen_sleep.append(s))

    _maybe_sleep(rows, 0, speed=1, surge=surge, max_sleep_s=10.0)

    # gap_s=1.0, speed=1, mult=10.0 (inside the window) -> sleep_s == 0.1. If the surge window were
    # checked against local time instead of UTC, `mult` would fall back to 1.0 -> sleep_s == 1.0.
    assert seen_sleep == [pytest.approx(0.1)]


def test_maybe_sleep_falls_back_to_normal_speed_outside_window(monkeypatch):
    event_utc = datetime(2015, 9, 15, 3, 0, 0, tzinfo=timezone.utc)  # well outside the window below
    cur_ts = int(event_utc.timestamp() * 1000)
    rows = [{"timestamp": cur_ts}, {"timestamp": cur_ts + 1000}]

    surge = SurgeSchedule(windows=[("2015-09-15T11:30:00", "2015-09-15T12:30:00", 10.0)])

    seen_sleep: list[float] = []
    monkeypatch.setattr("time.sleep", lambda s: seen_sleep.append(s))

    _maybe_sleep(rows, 0, speed=1, surge=surge, max_sleep_s=10.0)

    assert seen_sleep == [pytest.approx(1.0)]
