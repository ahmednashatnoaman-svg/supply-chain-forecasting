"""Consumer verifier — detect message loss/lag. Implements Hatem plan Task 4.

Consumes ``live_web_traffic`` with a unique per-run group id and reports how many of the
``expected_count`` records it actually saw, plus the worst-case lag (now_ms - event_time_ms)
across received records. Manual offset commit (kafka-playbook §2): commit only after counting,
so at-least-once delivery stays safe.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from libs.scf_common.contracts import Topics
from libs.scf_common.observability import ERRORS_TOTAL, RECORDS_PROCESSED, get_logger

_COMPONENT = "consumer-verifier"
log = get_logger(__name__)

# Commit offsets in batches rather than per-record -- a synchronous commit is a broker
# round-trip, so per-record commits cap throughput at ~1 RTT/message. The verifier only needs
# at-least-once accounting (it never re-runs with the same group id), so a final commit at loop
# exit is sufficient; this batch interval just bounds re-delivery on a mid-run crash.
_COMMIT_EVERY = 1000


@dataclass
class VerifyReport:
    received: int
    missing: int
    max_lag_ms: int


def verify(expected_count: int, timeout_s: int = 30) -> VerifyReport:
    """Consume up to ``expected_count`` messages and report loss/lag.

    Args:
        expected_count: how many records the producer claimed to send.
        timeout_s: hard wall-clock deadline for the poll loop.

    Returns:
        VerifyReport with ``received`` (count seen), ``missing = expected - received``,
        and ``max_lag_ms`` = max(now_ms - event_time_ms) over received records (>= 0).
    """
    from libs.scf_common.io.kafka import AvroKafkaConsumer

    # Unique per-run group id (kafka-playbook §3) so test runs never share offsets with a
    # production consumer or a previous test run.
    group_id = f"verifier-{uuid.uuid4().hex[:8]}"
    consumer = AvroKafkaConsumer(
        Topics.LIVE_WEB_TRAFFIC, "live_web_traffic.avsc", group_id=group_id
    )
    received = 0
    max_lag_ms = 0
    try:
        deadline = time.time() + timeout_s
        while received < expected_count and time.time() < deadline:
            rec = consumer.poll(1.0)
            if rec is None:
                continue
            try:
                event_time_ms = int(rec["event_time"])
                lag = int(time.time() * 1000) - event_time_ms
                if lag > max_lag_ms:
                    max_lag_ms = lag
                received += 1
                RECORDS_PROCESSED.labels(_COMPONENT).inc()
                # Batch commits to avoid a broker round-trip per record; a final commit after
                # the loop catches any remainder. A crash mid-batch re-delivers (at-least-once).
                if received % _COMMIT_EVERY == 0:
                    consumer.commit()
            except Exception:  # noqa: BLE001 -- one bad record must not abort the verify
                ERRORS_TOTAL.labels(_COMPONENT).inc()
                log.exception("verifier.skip_bad_record")
                # Commit past the poison pill immediately so the next poll advances.
                consumer.commit()
        # Final commit for any uncommitted offsets in the trailing partial batch.
        if received > 0:
            consumer.commit()
    finally:
        consumer.close()
    missing = expected_count - received
    log.info("verifier.done", received=received, missing=missing, max_lag_ms=max_lag_ms)
    return VerifyReport(received=received, missing=missing, max_lag_ms=max_lag_ms)


if __name__ == "__main__":  # pragma: no cover
    import argparse

    p = argparse.ArgumentParser(description="Verify live_web_traffic loss/lag")
    p.add_argument("--expected", type=int, required=True, help="expected record count")
    p.add_argument("--timeout", type=int, default=30, help="poll deadline seconds")
    args = p.parse_args()
    report = verify(expected_count=args.expected, timeout_s=args.timeout)
    print(
        f"received={report.received} missing={report.missing} max_lag_ms={report.max_lag_ms}"
    )
