"""Unit tests for the surge schedule (Hatem plan Task 1). Pure."""

from datetime import datetime

import pytest

from ingestion.generator.surge import SurgeSchedule

pytestmark = pytest.mark.unit


def test_multiplier_inside_and_outside_window():
    s = SurgeSchedule(windows=[("2026-01-01T10:00", "2026-01-01T11:00", 5.0)])
    assert s.multiplier(datetime(2026, 1, 1, 10, 30)) == 5.0
    assert s.multiplier(datetime(2026, 1, 1, 9, 0)) == 1.0


def test_no_windows_is_normal_speed():
    assert SurgeSchedule().multiplier(datetime(2026, 1, 1)) == 1.0


def test_first_matching_window_wins():
    s = SurgeSchedule(
        windows=[
            ("2026-01-01T10:00", "2026-01-01T12:00", 5.0),
            ("2026-01-01T11:00", "2026-01-01T13:00", 9.0),
        ]
    )
    assert s.multiplier(datetime(2026, 1, 1, 11, 30)) == 5.0
