"""Live Kafka tailing for the dashboard. Implements Ziad plan Task 5 (live data path).

Thin polling collectors over ``automated_pricing_updates`` and ``system_alerts``. Each returns a
plain list of dicts so the page ``render()`` functions stay pure and unit-testable: the live
Streamlit run calls ``collect_*()`` then passes the result to ``render(events=...)``; unit tests
call ``render(events=[...synthetic...])`` with no broker.

Consumers use a dedicated dashboard group id and never commit offsets (``auto.offset.reset``
defaults to ``earliest``), so a freshly-launched dashboard replays recent history on first poll —
acceptable for a "command center" view and avoids losing the audit trail between restarts.
"""

from __future__ import annotations

from collections.abc import Callable


def _collect(
    topic: str,
    schema_file: str,
    *,
    group_id: str,
    max_messages: int,
    poll_timeout: float,
    consumer_factory: Callable | None = None,
) -> list[dict]:
    """Poll up to ``max_messages`` records from ``topic``. Returns earliest-available records.

    Polls until ``max_messages`` are gathered or ``deadline`` elapses with no new record — a fresh
    consumer returns ``None`` during partition assignment even when records exist, so we cannot stop
    on the first ``None`` (mirrors the proven pattern in ``test_producer_roundtrip``).

    ``consumer_factory`` is an injection seam for tests (pass a fake consumer). When ``None`` the
    real :class:`AvroKafkaConsumer` is used.
    """
    import time

    if consumer_factory is not None:
        consumer = consumer_factory(topic=topic, schema_file=schema_file, group_id=group_id)
    else:
        from libs.scf_common.io.kafka import AvroKafkaConsumer

        consumer = AvroKafkaConsumer(topic, schema_file, group_id=group_id)
    events: list[dict] = []
    try:
        deadline = time.time() + 30.0
        while len(events) < max_messages and time.time() < deadline:
            rec = consumer.poll(poll_timeout)
            if rec is not None:
                events.append(rec)
            elif events:
                # we already drained the backlog — stop after one empty poll
                break
    finally:
        consumer.close()
    return events


def collect_price_updates(
    *,
    max_messages: int = 50,
    poll_timeout: float = 0.5,
    group_id: str = "dashboard-price-ticker",
    consumer_factory: Callable | None = None,
) -> list[dict]:
    """Tail ``automated_pricing_updates`` for the live price ticker (page 2)."""
    from libs.scf_common.contracts import Topics

    return _collect(
        Topics.AUTOMATED_PRICING_UPDATES,
        "automated_pricing_updates.avsc",
        group_id=group_id,
        max_messages=max_messages,
        poll_timeout=poll_timeout,
        consumer_factory=consumer_factory,
    )


def collect_alerts(
    *,
    max_messages: int = 50,
    poll_timeout: float = 0.5,
    group_id: str = "dashboard-automation-log",
    consumer_factory: Callable | None = None,
) -> list[dict]:
    """Tail ``system_alerts`` for the automation log (page 3)."""
    from libs.scf_common.contracts import Topics

    return _collect(
        Topics.SYSTEM_ALERTS,
        "system_alerts.avsc",
        group_id=group_id,
        max_messages=max_messages,
        poll_timeout=poll_timeout,
        consumer_factory=consumer_factory,
    )
