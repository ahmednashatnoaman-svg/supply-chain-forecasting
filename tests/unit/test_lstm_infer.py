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
