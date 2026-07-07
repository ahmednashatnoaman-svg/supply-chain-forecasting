"""Chronological 80/20 train/test split of the Retailrocket events CSV.

Decision D-A (frozen): earliest 80% by ``timestamp`` -> train history (HDFS bronze, Emad's batch
brain); latest 20% -> the live "test" tail replayed to Kafka by the traffic generator. No random
shuffle -- avoids time-series leakage into the forecast/LSTM training set.

The core ``split_events`` is pure (plain dicts in, plain dicts out) so unit tests need no pandas.
The ``__main__`` CLI does the pandas CSV I/O.
"""

from __future__ import annotations


def split_events(
    rows: list[dict], train_fraction: float = 0.8
) -> tuple[list[dict], list[dict]]:
    """Split rows chronologically into (train, test).

    Args:
        rows: list of row dicts with an integer-valued ``timestamp`` key (epoch millis).
        train_fraction: fraction of rows going to train; the rest go to test. Floored so the
            test split always receives at least the remainder (no rounding onto train).

    Returns:
        ``(train, test)`` -- a stable ascending sort by ``timestamp``. Equal timestamps keep
        their input order (Python's ``sorted`` is stable). The input list and its dicts are not
        mutated.
    """
    ordered = sorted(rows, key=lambda r: int(r["timestamp"]))
    cut = int(len(ordered) * train_fraction)
    return ordered[:cut], ordered[cut:]


def _cli() -> None:  # pragma: no cover
    import argparse
    from pathlib import Path

    import pandas as pd

    p = argparse.ArgumentParser(description="Chronological 80/20 train/test split of events.csv")
    p.add_argument(
        "--input", default="data/raw/events.csv", help="source events.csv (timestamp-sorted is fine)"
    )
    p.add_argument(
        "--train-fraction", type=float, default=0.8, help="fraction of rows going to train"
    )
    p.add_argument("--train-out", default="data/raw/events_train.csv")
    p.add_argument("--test-out", default="data/raw/events_test.csv")
    args = p.parse_args()

    src = Path(args.input)
    if not src.exists():
        raise SystemExit(f"input not found: {src} (run scripts/download_data.sh first)")

    df = pd.read_csv(src)
    # Guarantee a NUMERIC sort so this path honors the same `int(timestamp)` coercion that the
    # unit-tested split_events uses -- a str-typed column would otherwise sort lexicographically.
    df["timestamp"] = df["timestamp"].astype(int)
    df = df.sort_values("timestamp", kind="stable")
    cut = int(len(df) * args.train_fraction)
    train_df, test_df = df.iloc[:cut], df.iloc[cut:]
    Path(args.train_out).parent.mkdir(parents=True, exist_ok=True)
    train_df.to_csv(args.train_out, index=False)
    test_df.to_csv(args.test_out, index=False)
    print(f"split: {len(train_df)} train -> {args.train_out}")
    print(f"split: {len(test_df)} test  -> {args.test_out}")


if __name__ == "__main__":  # pragma: no cover
    _cli()
