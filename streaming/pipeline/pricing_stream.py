"""Structured Streaming dynamic-pricing engine. Implements Nashat plan Task 5.

Reads live_web_traffic (Avro) -> windowed velocity -> LSTM surge -> dynamic_price (reading Redis
forecast/elasticity) -> emits automated_pricing_updates + system_alerts. Emits metrics + logs.
"""
from __future__ import annotations

from libs.scf_common.config import settings
from libs.scf_common.contracts import Topics
from libs.scf_common.io import get_spark
from libs.scf_common.observability import get_logger, serve_metrics

log = get_logger("streaming.pricing")


def run(trigger_once: bool = False) -> None:  # pragma: no cover - needs Spark + Kafka + Redis
    """Start the streaming pricing job.

    Args:
        trigger_once: if True, use an availableNow trigger (used by the e2e walking-skeleton test).

    TODO(nashat-plan Task 5): implement the full DAG:
      1. readStream Kafka `live_web_traffic`, from_avro deserialize.
      2. event-time sliding window (settings.pricing.window_seconds) -> per-SKU velocity.
      3. apply build_surge_pandas_udf(broadcast_state) for surge_prob.
      4. mapInPandas: read forecast:{sku}/graph:elasticity:{sku} from Redis, call dynamic_price.
      5. writeStream to `automated_pricing_updates` (Avro); branch low-stock -> `system_alerts`.
    """
    serve_metrics(port=8000)
    spark = get_spark("streaming")
    log.info(
        "streaming.pricing.start",
        source=Topics.LIVE_WEB_TRAFFIC,
        sink=Topics.AUTOMATED_PRICING_UPDATES,
        window_s=settings.pricing.window_seconds,
        trigger_once=trigger_once,
    )
    # (stream construction goes here per the TODO above)
    log.info("streaming.pricing.todo", note="wire readStream/writeStream per nashat-plan Task 5")
    spark.stop()


if __name__ == "__main__":
    run()
