"""Unit tests for the chronological 80/20 train/test split (Hatem plan Task 1, exec Task 1).

Pure logic — no pandas, no disk. The split must be chronological (earliest 80% -> train,
latest 20% -> test) and stable (equal timestamps keep input order) to avoid time-series
leakage into the downstream forecast/LSTM training set (decision D-A).
"""

from __future__ import annotations

import pytest

from ingestion.generator.split import split_events

pytestmark = pytest.mark.unit


def _row(ts: int, visitor: int = 1, item: int = 1) -> dict:
    return {"timestamp": ts, "visitorid": visitor, "event": "view", "itemid": item, "price": ""}


def test_split_distinct_timestamps_80_20():
    rows = [_row(1000 + i) for i in range(10)]
    train, test = split_events(rows, train_fraction=0.8)
    assert len(train) == 8
    assert len(test) == 2
    # train holds the 8 earliest timestamps; test the 2 latest
    assert max(r["timestamp"] for r in train) < min(r["timestamp"] for r in test)
    assert [r["timestamp"] for r in train] == [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007]
    assert [r["timestamp"] for r in test] == [1008, 1009]


def test_split_equal_timestamps_preserves_order_and_counts():
    rows = [
        {"timestamp": 5000, "visitorid": i, "event": "view", "itemid": i, "price": ""}
        for i in range(10)
    ]
    train, test = split_events(rows, train_fraction=0.8)
    assert len(train) == 8
    assert len(test) == 2
    # stable: equal timestamps keep input order
    assert [r["visitorid"] for r in train] == [0, 1, 2, 3, 4, 5, 6, 7]
    assert [r["visitorid"] for r in test] == [8, 9]


def test_split_fraction_rounds_down():
    # 7 rows * 0.8 = 5.6 -> 5 train, 2 test (floor on train, rest to test)
    rows = [_row(i) for i in range(7)]
    train, test = split_events(rows, train_fraction=0.8)
    assert len(train) == 5
    assert len(test) == 2


def test_split_does_not_mutate_input():
    rows = [_row(3000 - i) for i in range(5)]  # unsorted input
    snapshot = [dict(r) for r in rows]
    split_events(rows, train_fraction=0.8)
    assert rows == snapshot  # input list + dicts unchanged


def test_split_empty_input():
    train, test = split_events([], train_fraction=0.8)
    assert train == [] and test == []
