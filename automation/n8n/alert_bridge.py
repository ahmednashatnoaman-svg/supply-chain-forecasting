"""Bridge: consume `system_alerts` from Kafka and POST reorder requests to the n8n webhook.

Implements Ziad plan Tasks 2 & 3. `build_reorder` is pure (unit-tested); the consumer loop is the
integration piece. All emails are simulated/free — see docs/reference/cost-and-licensing.md.
"""
from __future__ import annotations

DEFAULT_TARGET_STOCK = 200


def build_reorder(alert: dict, target_stock: int = DEFAULT_TARGET_STOCK) -> dict:
    """Build a purchase-order payload from a low-stock alert.

    Args:
        alert: a SystemAlert dict with `item_id`, `stock_level`, `reorder_threshold`.
        target_stock: desired stock level to refill to.

    Returns:
        dict with `sku`, `qty` (units to order to reach target), and email fields.
    """
    sku = alert["item_id"]
    stock = int(alert["stock_level"])
    qty = max(target_stock - stock, 0)
    return {
        "sku": sku,
        "qty": qty,
        "supplier_email": "supplier@example.com",
        "subject": f"[AUTO-REORDER] SKU {sku} low stock ({stock} units)",
        "body": (
            f"Automated reorder triggered for SKU {sku}. Current stock {stock} is below threshold "
            f"{alert.get('reorder_threshold')}. Please ship {qty} units to restore target {target_stock}."
        ),
    }


def run_bridge() -> None:  # pragma: no cover - needs Kafka + n8n
    """Consume system_alerts and POST build_reorder(...) to the n8n webhook.

    TODO(ziad-plan Task 3): implement with AvroKafkaConsumer + requests.post(retry/backoff).
    """
    import requests

    from libs.scf_common.config import settings
    from libs.scf_common.contracts import Topics
    from libs.scf_common.io.kafka import AvroKafkaConsumer
    from libs.scf_common.observability import ERRORS_TOTAL, get_logger

    log = get_logger("alert_bridge")
    consumer = AvroKafkaConsumer(Topics.SYSTEM_ALERTS, "system_alerts.avsc", group_id="alert-bridge")
    log.info("alert_bridge.started", webhook=settings.automation.n8n_webhook_url)
    try:
        while True:
            alert = consumer.poll(1.0)
            if not alert:
                continue
            payload = build_reorder(alert)
            try:
                requests.post(settings.automation.n8n_webhook_url, json=payload, timeout=5)
                log.info("alert_bridge.reorder_sent", sku=payload["sku"], qty=payload["qty"])
            except Exception as exc:  # noqa: BLE001
                ERRORS_TOTAL.labels(component="alert_bridge").inc()
                log.error("alert_bridge.post_failed", error=str(exc))
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    run_bridge()
