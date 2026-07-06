"""LSTM inference wrapped as a Spark pandas_udf, with graceful fallback.

Implements Nashat plan Task 4 (inference side). If the model artifact is unavailable, `surge_prob`
returns 0.0 so the streaming pricing loop degrades to velocity-only pricing (never blocks).
"""
from __future__ import annotations

import numpy as np
import torch

from streaming.lstm.model import N_FEATURES, SEQ_LEN, SurgeLSTM


def predict_batch(model: SurgeLSTM | None, sequences: np.ndarray) -> np.ndarray:
    """Return surge probabilities for a batch of sequences.

    Args:
        model: a loaded SurgeLSTM, or None to trigger the zero-fallback.
        sequences: array shaped (batch, SEQ_LEN, N_FEATURES).

    Returns:
        1-D array of probabilities in [0, 1], one per row.
    """
    if model is None:
        return np.zeros(len(sequences), dtype=np.float32)
    model.eval()
    with torch.no_grad():
        x = torch.as_tensor(sequences, dtype=torch.float32)
        return model(x).cpu().numpy().astype(np.float32)


def build_surge_pandas_udf(broadcast_state_dict):  # pragma: no cover - needs Spark
    """Build a Spark pandas_udf that scores each row's sequence column.

    TODO(nashat-plan Task 5): wire into `pricing_stream`. `broadcast_state_dict` is a
    `spark.sparkContext.broadcast(model.state_dict())` so workers rebuild the model locally.
    """
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import FloatType

    @pandas_udf(FloatType())
    def _udf(seq_col):
        model = SurgeLSTM()
        try:
            model.load_state_dict(broadcast_state_dict.value)
        except Exception:
            model = None  # fallback -> zeros
        arr = np.stack(seq_col.apply(lambda s: np.asarray(s).reshape(SEQ_LEN, N_FEATURES)).to_list())
        return __import__("pandas").Series(predict_batch(model, arr))

    return _udf
