"""Live Kafka tailing for the dashboard. Implements Ziad plan Task 5 (live data path).

Thin polling collectors over ``automated_pricing_updates`` and ``system_alerts``. Each returns a
plain list of dicts so the page ``render()`` functions stay pure and unit-testable: the live
Streamlit run calls ``collect_*()`` then passes the result to ``render(events=...)``; unit tests
call ``render(events=[...synthetic...])`` with no broker.

Consumers use a dedicated dashboard group id and never commit offsets, matching this project's
``earliest`` default -- but critically, the underlying ``AvroKafkaConsumer`` is created **once**
per (topic, group_id) and cached at module scope (``_consumer_cache``) rather than recreated on
every call. Streamlit reruns this whole script every fragment tick (every 5s); creating a fresh
consumer each time replayed the topic's *entire* history from scratch every single refresh (a
long-running topic would always show the same multi-hour-old messages, never anything recent).
Caching the consumer means only the very first call ever starts at ``earliest`` -- every
subsequent poll naturally continues from wherever the same live connection left off, so the
ticker advances with real time instead of resetting.
"""

from __future__ import annotations

from collections.abc import Callable

_consumer_cache: dict[tuple[str, str], object] = {}


def _get_consumer(topic: str, schema_file: str, group_id: str, consumer_factory: Callable | None):
    key = (topic, group_id)
    if key not in _consumer_cache:
        if consumer_factory is not None:
            _consumer_cache[key] = consumer_factory(
                topic=topic, schema_file=schema_file, group_id=group_id
            )
        else:
            from libs.scf_common.io.kafka import AvroKafkaConsumer

            _consumer_cache[key] = AvroKafkaConsumer(topic, schema_file, group_id=group_id)
    return _consumer_cache[key]


def _collect(
    topic: str,
    schema_file: str,
    *,
    group_id: str,
    max_messages: int,
    poll_timeout: float,
    consumer_factory: Callable | None = None,
) -> list[dict]:
    """Poll up to ``max_messages`` records from ``topic`` using the cached consumer for this
    (topic, group_id) pair (see module docstring).

    Polls until ``max_messages`` are gathered or ``deadline`` elapses with no new record — a fresh
    consumer returns ``None`` during partition assignment even when records exist, so we cannot stop
    on the first ``None`` (mirrors the proven pattern in ``test_producer_roundtrip``).

    ``consumer_factory`` is an injection seam for tests (pass a fake consumer). When ``None`` the
    real :class:`AvroKafkaConsumer` is used. The consumer is intentionally never closed here -- it
    is reused by every subsequent call for this (topic, group_id); it lives for the dashboard
    process's lifetime.
    """
    import time

    consumer = _get_consumer(topic, schema_file, group_id, consumer_factory)
    events: list[dict] = []
    deadline = time.time() + 30.0
    while len(events) < max_messages and time.time() < deadline:
        rec = consumer.poll(poll_timeout)
        if rec is not None:
            events.append(rec)
        elif events:
            # we already drained the backlog — stop after one empty poll
            break
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
