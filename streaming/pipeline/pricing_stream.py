"""Structured Streaming dynamic-pricing engine. Implements Nashat plan Task 5.

Reads live_web_traffic (Avro, Confluent wire format) -> windowed per-SKU velocity -> LSTM surge
signal (scored against a genuine per-SKU historical sequence, not a tiled snapshot -- see
`price_row` / issue #36) -> dynamic_price (reading Redis forecast/elasticity) -> honest price
presentation -> emits automated_pricing_updates + system_alerts (low stock during a surge). Metrics
+ structured logs throughout via libs.scf_common.observability.

The LSTM's [SEQ_LEN, N_FEATURES] input is a genuine per-SKU rolling history, persisted in Redis
under `RedisKeys.lstm_sequence(sku)` and read-modify-written once per row inside `price_row` --
the same pattern this codebase already uses for `price:current:*` and `velocity:*`. This is
deliberately NOT a Spark `applyInPandasWithState` operator chained after `compute_velocity`'s
windowed aggregation: Spark 3.5's planner rejects that chain outright in every output mode
(`applyInPandasWithState ... is not supported with aggregation on a streaming DataFrame/Dataset`),
confirmed by actually running it against real Kafka, not guessed. A cold-start SKU's first-ever
window is tiled (there is no history yet), and every subsequent window pushes one more real step
into the buffer, organically evicting the synthetic padding -- see `update_sequence_buffer`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import redis as redis_lib
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from libs.scf_common.config import settings
from libs.scf_common.contracts import RedisKeys, Topics
from libs.scf_common.io import get_redis, get_spark
from libs.scf_common.io.kafka import load_schema
from libs.scf_common.observability import (
    ERRORS_TOTAL,
    LOW_STOCK_ALERTS_TOTAL,
    RECORDS_PROCESSED,
    SURGE_EVENTS_TOTAL,
    get_logger,
    serve_metrics,
)
from streaming.lstm.infer import predict_batch
from streaming.lstm.model import N_FEATURES, SEQ_LEN, SurgeLSTM
from streaming.pricing.formula import PricingConfig, dynamic_price
from streaming.pricing.psychology import PsychologyConfig, present_price
from streaming.sinks.kafka_sink import alert_producer, pricing_producer

log = get_logger("streaming.pricing")

CONFLUENT_WIRE_PREFIX_BYTES = 5  # 1 magic byte + 4-byte Schema Registry ID

# TTL on the per-SKU LSTM history buffer: a SKU quiet for 2 hours has its history dropped rather
# than growing a Redis key forever for delisted/abandoned products.
_LSTM_SEQUENCE_TTL_SECONDS = 2 * 60 * 60

_PRICING_CFG = PricingConfig(
    elasticity_coeff=settings.pricing.elasticity_coeff,
    max_uplift_pct=settings.pricing.max_uplift_pct,
    min_margin_pct=settings.pricing.min_margin_pct,
    surge_threshold=settings.pricing.surge_threshold,
)
_PSYCH_CFG = PsychologyConfig()


def parse_events(raw: DataFrame, schema_str: str) -> DataFrame:
    """Strip the Confluent wire-format prefix and decode the Avro payload.

    Args:
        raw: the raw Kafka readStream DataFrame (must have a `value` binary column).
        schema_str: the Avro schema JSON string (contracts/avro/live_web_traffic.avsc).

    Returns:
        DataFrame with columns [event_time, visitor_id, event, item_id, price].
    """
    from pyspark.sql.avro.functions import from_avro

    payload = F.expr(f"substring(value, {CONFLUENT_WIRE_PREFIX_BYTES + 1}, length(value))")
    return raw.select(from_avro(payload, schema_str).alias("e")).select("e.*")


def compute_velocity(events: DataFrame, window_seconds: int) -> DataFrame:
    """Aggregate per-SKU transaction velocity over a tumbling window.

    Args:
        events: parsed events with [event_time, item_id, event, price]. `event_time` may already be
            a proper timestamp (real Avro logical-type path) or a raw millis long (defensive path).
        window_seconds: tumbling window size in seconds (settings.pricing.window_seconds).

    Returns:
        DataFrame: [item_id, window_start, window_end, velocity, view_count, addtocart_count,
        last_price].
    """
    is_already_ts = dict(events.dtypes).get("event_time") == "timestamp"
    ts_col = (
        F.col("event_time") if is_already_ts else (F.col("event_time") / 1000).cast("timestamp")
    )
    with_ts = events.withColumn("event_time", ts_col)
    if with_ts.isStreaming:
        with_ts = with_ts.withWatermark("event_time", "2 minutes")

    agg = with_ts.groupBy(F.window("event_time", f"{window_seconds} seconds"), "item_id").agg(
        F.sum(F.when(F.col("event") == "transaction", 1).otherwise(0)).alias("velocity"),
        F.sum(F.when(F.col("event") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event") == "addtocart", 1).otherwise(0)).alias("addtocart_count"),
        F.last("price", ignorenulls=True).alias("last_price"),
    )
    return agg.select(
        "item_id",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "velocity",
        "view_count",
        "addtocart_count",
        "last_price",
    )


def update_sequence_buffer(
    buffer: list[float],
    velocity: float,
    view_count: float,
    addtocart_count: float,
    price_delta: float,
) -> list[float]:
    """Append one window's real feature vector to a per-SKU rolling history buffer.

    Real fix for issue #36: replaces the old "tile the current window SEQ_LEN times" simplification.
    `buffer` is a flat list of floats (N_FEATURES per step, oldest step first). This appends the new
    step, keeps only the most recent SEQ_LEN steps, and front-pads with the earliest known step so
    the result always has the model's contractual `SEQ_LEN * N_FEATURES` length.

    A cold-start SKU (buffer empty, first window ever seen) is tiled just like the old behavior --
    there is no history to show yet -- but every subsequent call pushes in one more genuine
    historical step, which the trim below organically evicts the synthetic padding to make room for.
    """
    new_step = [float(velocity), float(view_count), float(addtocart_count), float(price_delta)]
    updated = list(buffer) + new_step
    max_len = SEQ_LEN * N_FEATURES

    if len(updated) > max_len:
        updated = updated[-max_len:]
    elif len(updated) < max_len:
        first_step = updated[:N_FEATURES]
        pad_steps = (max_len - len(updated)) // N_FEATURES
        updated = first_step * pad_steps + updated

    return updated


def reshape_lstm_sequence(sequence_flat: list[float]) -> np.ndarray:
    """Reshape a flat `SEQ_LEN * N_FEATURES` history buffer into the model's [SEQ_LEN, N_FEATURES]
    input tensor."""
    return np.asarray(sequence_flat, dtype=np.float32).reshape(SEQ_LEN, N_FEATURES)


def price_row(
    item_id: int,
    velocity: float,
    view_count: float,
    addtocart_count: float,
    last_price: float | None,
    redis: redis_lib.Redis,
    model: SurgeLSTM | None,
) -> dict:
    """Compute the priced decision for one SKU's aggregated window.

    Reads this SKU's real historical feature sequence from Redis (`RedisKeys.lstm_sequence`),
    appends the current window's features via `update_sequence_buffer` (issue #36's actual fix --
    not a tiled single point), writes the updated buffer back, and scores the LSTM against it.
    Deterministic given the Redis client's current state and the model, which is what makes this
    unit-testable with fakeredis.
    """
    # Retailrocket's live clickstream events carry no price field (see batch/etl/pricing.py) -- the
    # live event's own price is used when present (e.g. synthetic test data), but real traffic
    # always falls back to whatever price is already in Redis (seeded nightly by
    # batch.etl.pricing.build_pricing, then kept current by this same key on every prior window).
    # Only a genuinely brand-new, never-priced SKU falls all the way through to 0.0.
    if last_price is not None:
        base_price = float(last_price)
    else:
        existing_price = redis.get(RedisKeys.price(item_id))
        base_price = float(existing_price) if existing_price is not None else 0.0
    forecast_raw = redis.get(RedisKeys.forecast(item_id))
    baseline = float(forecast_raw) if forecast_raw is not None else 0.0

    # Scale daily baseline to the streaming window size for accurate ratio comparison
    if baseline > 0:
        window_baseline = max(baseline * (settings.pricing.window_seconds / 86400.0), 0.01)
    else:
        window_baseline = 0.0

    elasticity_raw = redis.get(RedisKeys.elasticity(item_id))
    elasticity = 0.0
    if elasticity_raw:
        edges = json.loads(elasticity_raw)
        if edges:
            elasticity = max(e["weight"] for e in edges)

    sequence_key = RedisKeys.lstm_sequence(item_id)
    existing_raw = redis.get(sequence_key)
    existing_buffer: list[float] = json.loads(existing_raw) if existing_raw else []
    sequence_flat = update_sequence_buffer(
        existing_buffer,
        velocity=velocity,
        view_count=view_count,
        addtocart_count=addtocart_count,
        price_delta=0.0,
    )
    redis.set(sequence_key, json.dumps(sequence_flat), ex=_LSTM_SEQUENCE_TTL_SECONDS)

    seq = reshape_lstm_sequence(sequence_flat)
    surge_prob = float(predict_batch(model, seq[np.newaxis, :, :])[0])
    is_surge = (
        surge_prob >= 0.5
        and window_baseline > 0
        and (velocity / window_baseline) > _PRICING_CFG.surge_threshold
    )
    if is_surge:
        SURGE_EVENTS_TOTAL.labels(component="streaming.pricing").inc()

    new_price = dynamic_price(
        base_price, velocity, window_baseline, elasticity, is_surge, _PRICING_CFG
    )
    presentation = present_price(new_price, base_price, _PSYCH_CFG)

    redis.set(RedisKeys.price(item_id), presentation.display_price)
    redis.set(RedisKeys.velocity(item_id, f"{settings.pricing.window_seconds}s"), velocity)

    return {
        "item_id": int(item_id),
        "base_price": base_price,
        "new_price": presentation.display_price,
        "velocity": float(velocity),
        "baseline": baseline,
        "surge_prob": surge_prob,
        "reason": f"surge_prob={surge_prob:.2f} note={presentation.note} anchor={presentation.anchor_price}",
    }


def check_low_stock_alert(
    item_id: int, redis: redis_lib.Redis, reorder_threshold: int
) -> dict | None:
    """Return a LOW_STOCK alert payload if inventory is below threshold, else None."""
    inv_raw = redis.get(RedisKeys.inventory(item_id))
    if inv_raw is None:
        return None
    stock = int(inv_raw)
    if stock >= reorder_threshold:
        return None
    LOW_STOCK_ALERTS_TOTAL.labels(component="streaming.pricing").inc()
    return {
        "item_id": int(item_id),
        "alert_type": "LOW_STOCK",
        "stock_level": stock,
        "reorder_threshold": reorder_threshold,
        "message": f"SKU {item_id} stock {stock} below threshold {reorder_threshold}",
    }


def process_batch(
    batch_df: DataFrame, batch_id: int, model: SurgeLSTM | None
) -> None:  # noqa: ARG001
    """`foreachBatch` handler: enrich each aggregated row with Redis + LSTM + pricing, emit to Kafka.

    One row per SKU per window — small enough to `.collect()` and loop (see spark-playbook.md §3 for
    when this would need to change to a broadcast/join pattern instead).
    """
    redis = get_redis()
    price_out = pricing_producer()
    alert_out = alert_producer()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    for row in batch_df.collect():
        try:
            priced = price_row(
                row["item_id"],
                float(row["velocity"]),
                float(row["view_count"]),
                float(row["addtocart_count"]),
                row["last_price"],
                redis,
                model,
            )
            priced["event_time"] = now_ms
            price_out.produce(priced)

            alert = check_low_stock_alert(
                row["item_id"], redis, settings.automation.reorder_threshold
            )
            if alert:
                alert["event_time"] = now_ms
                alert_out.produce(alert)

            RECORDS_PROCESSED.labels(component="streaming.pricing").inc()
        except Exception as exc:  # noqa: BLE001
            ERRORS_TOTAL.labels(component="streaming.pricing").inc()
            log.error("streaming.pricing.row_failed", item_id=row["item_id"], error=str(exc))

    price_out.flush()
    alert_out.flush()


def _load_production_model() -> SurgeLSTM | None:
    """Load the Production-staged LSTM from MLflow; None triggers the zero fallback (never blocks)."""
    try:
        import mlflow

        return mlflow.pytorch.load_model("models:/surge_classifier/Production")
    except Exception as exc:  # noqa: BLE001
        log.warning("streaming.pricing.model_unavailable", error=str(exc))
        return None


def run(trigger_once: bool = False, checkpoint_dir: str | None = None) -> None:
    """Start the streaming pricing job.

    Args:
        trigger_once: if True, use an `availableNow`-style single-batch trigger (used by the
            walking-skeleton e2e test) instead of the continuous processingTime trigger.
        checkpoint_dir: overrides `settings.pricing.checkpoint_dir` -- tracks Kafka source offsets
            across restarts (the per-SKU LSTM history buffer itself lives in Redis, not in this
            checkpoint; see `price_row`).
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

    schema_str = load_schema("live_web_traffic.avsc")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", Topics.LIVE_WEB_TRAFFIC)
        .option("startingOffsets", "earliest" if trigger_once else "latest")
        .option(
            "maxOffsetsPerTrigger", 10000
        )  # bound per-trigger read so a surge can't overwhelm one batch
        .load()
    )
    events = parse_events(raw, schema_str)
    velocity = compute_velocity(events, settings.pricing.window_seconds)
    model = _load_production_model()

    trigger = (
        {"once": True}
        if trigger_once
        else {"processingTime": f"{settings.pricing.window_seconds} seconds"}
    )
    query = (
        velocity.writeStream.foreachBatch(lambda df, bid: process_batch(df, bid, model))
        .option("checkpointLocation", checkpoint_dir or settings.pricing.checkpoint_dir)
        .trigger(**trigger)
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    run()
