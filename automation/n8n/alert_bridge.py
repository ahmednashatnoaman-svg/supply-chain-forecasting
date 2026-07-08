"""Bridge: consume `system_alerts` from Kafka and POST reorder requests to the n8n webhook.

Implements Ziad plan Tasks 2 & 3. `build_reorder` is pure (unit-tested); the consumer loop is the
integration piece. All emails are simulated/free — see docs/reference/cost-and-licensing.md.
"""

from __future__ import annotations

import time

DEFAULT_TARGET_STOCK = 200


def build_reorder(alert: dict, target_stock: int = DEFAULT_TARGET_STOCK) -> dict:
    """Build a purchase-order payload from a low-stock alert.

    Args:
        alert: a SystemAlert dict with `item_id`, `stock_level`, `reorder_threshold`.
        target_stock: desired stock level to refill to.

    Returns:
        dict with `sku`, `qty` (units to order to reach target), and email fields.
    """
    from libs.scf_common.config import settings

    sku = alert["item_id"]
    stock = int(alert["stock_level"])
    qty = max(target_stock - stock, 0)
    return {
        "sku": sku,
        "qty": qty,
        "supplier_email": settings.automation.supplier_email,
        "subject": f"[AUTO-REORDER] SKU {sku} low stock ({stock} units)",
        "body": (
            f"Automated reorder triggered for SKU {sku}. Current stock {stock} is below threshold "
            f"{alert.get('reorder_threshold')}. Please ship {qty} units to restore target {target_stock}."
        ),
    }


def _post_with_retry(
    url: str, payload: dict, *, retries: int, timeout: float, secret: str = ""
) -> bool:
    """POST ``payload`` to ``url`` with exponential backoff. Returns True on success.

    Uses ``requests`` (a free, pure-python lib that is already a transitive dependency). On final
    failure returns False so the caller can skip the offset commit (at-least-once delivery — the
    reorder is idempotent because ``build_reorder`` is a pure function of the alert).

    ``secret``: sent as ``X-Webhook-Secret`` when non-empty (see ``settings.automation.
    n8n_webhook_secret``) so a reachable-but-unauthenticated-by-mistake n8n instance can't have
    this endpoint triggered by anyone who guesses the URL.
    """
    import requests  # type: ignore[import-untyped]

    headers = {"X-Webhook-Secret": secret} if secret else None
    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if resp.status_code < 400:
                return True
            # A non-429 4xx (bad payload, wrong URL, auth) can't be fixed by retrying --
            # only retry network errors, 5xx, and 429 (rate limit).
            if resp.status_code < 500 and resp.status_code != 429:
                return False
        except requests.exceptions.RequestException:
            pass  # fall through to backoff + retry
        if attempt < retries - 1:
            time.sleep(0.1 * (2**attempt))  # 0.1s, 0.2s, 0.4s, ...
    return False


def run_bridge(
    *,
    max_messages: int | None = None,
    webhook_url: str | None = None,
    poll_timeout: float = 1.0,
    post_retries: int = 3,
    post_timeout: float = 5.0,
) -> list[dict]:
    """Consume ``system_alerts`` and POST ``build_reorder(...)`` to the n8n webhook.

    Args:
        max_messages: stop after this many successful posts (``None`` = run forever, production).
        webhook_url: override ``settings.automation.n8n_webhook_url`` (used by tests).
        poll_timeout: seconds to block on each Kafka poll.
        post_retries: HTTP POST attempts before giving up on one alert.
        post_timeout: per-request HTTP timeout in seconds.

    Returns:
        The list of reorder payloads that were posted successfully.

    Offsets are committed **only after** a successful POST, giving at-least-once delivery
    (safe because reorders are idempotent). A failed POST after all retries is logged and the
    offset is left uncommitted so the alert is redelivered on the next run.
    """
    from libs.scf_common.config import settings
    from libs.scf_common.contracts import Topics
    from libs.scf_common.io.kafka import AvroKafkaConsumer
    from libs.scf_common.observability import ERRORS_TOTAL, RECORDS_PROCESSED, get_logger

    log = get_logger("alert_bridge")
    url = webhook_url or settings.automation.n8n_webhook_url
    secret = settings.automation.n8n_webhook_secret
    consumer = AvroKafkaConsumer(
        Topics.SYSTEM_ALERTS, "system_alerts.avsc", group_id="alert-bridge"
    )
    log.info("alert_bridge.started", webhook=url, max_messages=max_messages)
    posted: list[dict] = []
    try:
        while max_messages is None or len(posted) < max_messages:
            alert = consumer.poll(poll_timeout)
            if not alert:
                continue
            payload = build_reorder(alert)
            if _post_with_retry(
                url, payload, retries=post_retries, timeout=post_timeout, secret=secret
            ):
                consumer.commit()  # at-least-once: only commit after the downstream write succeeds
                posted.append(payload)
                RECORDS_PROCESSED.labels(component="alert_bridge").inc()
                log.info(
                    "alert_bridge.reorder_sent",
                    sku=payload["sku"],
                    qty=payload["qty"],
                )
            else:
                ERRORS_TOTAL.labels(component="alert_bridge").inc()
                log.error(
                    "alert_bridge.post_failed_after_retries",
                    sku=payload["sku"],
                    retries=post_retries,
                )
                # offset intentionally NOT committed -> redelivery on next poll cycle
    finally:
        consumer.close()
    return posted


if __name__ == "__main__":  # pragma: no cover
    run_bridge()
