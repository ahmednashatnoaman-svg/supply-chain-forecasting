"""Unit tests for LSTM inference + fallback (Nashat plan Task 4). Skips if torch is absent."""

import pytest

pytest.importorskip("torch")
pytest.importorskip("numpy")

import numpy as np  # noqa: E402

from streaming.lstm.infer import predict_batch  # noqa: E402
from streaming.lstm.model import N_FEATURES, SEQ_LEN, SurgeLSTM  # noqa: E402

pytestmark = pytest.mark.unit


def test_predict_batch_returns_probabilities():
    model = SurgeLSTM()
    seqs = np.random.randn(8, SEQ_LEN, N_FEATURES).astype("float32")
    out = predict_batch(model, seqs)
    assert out.shape == (8,)
    assert ((out >= 0.0) & (out <= 1.0)).all()


def test_predict_batch_fallback_when_model_missing():
    seqs = np.zeros((4, SEQ_LEN, N_FEATURES), dtype="float32")
    out = predict_batch(None, seqs)
    assert out.shape == (4,)
    assert (out == 0.0).all()  # graceful fallback -> velocity-only pricing


def test_surge_pandas_udf_returns_probabilities_per_row(spark):
    """The Spark-wired UDF (Task 4's real deliverable): broadcast weights -> per-row surge_prob."""
    pytest.importorskip("pyspark")
    from streaming.lstm.infer import build_surge_pandas_udf

    model = SurgeLSTM()
    broadcast = spark.sparkContext.broadcast(model.state_dict())

    flat_len = SEQ_LEN * N_FEATURES
    # .tolist() -> native Python floats; a list of numpy scalars breaks Spark's type inference.
    rows = [(np.random.randn(flat_len).tolist(),) for _ in range(5)]
    df = spark.createDataFrame(rows, ["sequence"])

    udf = build_surge_pandas_udf(broadcast)
    out = df.withColumn("surge_prob", udf(df["sequence"])).select("surge_prob").collect()

    assert len(out) == 5
    assert all(0.0 <= r["surge_prob"] <= 1.0 for r in out)


def test_surge_pandas_udf_falls_back_to_zero_when_broadcast_has_no_model(spark):
    """Broadcasting None (no trained model available) must degrade to velocity-only (prob=0.0)."""
    pytest.importorskip("pyspark")
    from streaming.lstm.infer import build_surge_pandas_udf

    broadcast = spark.sparkContext.broadcast(None)

    flat_len = SEQ_LEN * N_FEATURES
    rows = [(np.zeros(flat_len).tolist(),) for _ in range(3)]
    df = spark.createDataFrame(rows, ["sequence"])

    udf = build_surge_pandas_udf(broadcast)
    out = df.withColumn("surge_prob", udf(df["sequence"])).select("surge_prob").collect()

    assert len(out) == 3
    assert all(r["surge_prob"] == 0.0 for r in out)
