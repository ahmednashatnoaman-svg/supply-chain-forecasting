"""LSTM surge classifier — model definition + local training entrypoint.

Implements Nashat plan Task 4. Trained locally in PyTorch (zero-cost, no inference API) and logged to
MLflow. Signature mirrors contracts/models/lstm_signature.json (input [50, 4] -> surge_prob in [0,1]).
"""
from __future__ import annotations

import torch
from torch import nn

SEQ_LEN = 50
N_FEATURES = 4


class SurgeLSTM(nn.Module):
    """1-layer LSTM + sigmoid head producing a surge probability."""

    def __init__(self, n_features: int = N_FEATURES, hidden: int = 32):
        super().__init__()
        self.lstm = nn.LSTM(input_size=n_features, hidden_size=hidden, batch_first=True)
        self.head = nn.Sequential(nn.Linear(hidden, 1), nn.Sigmoid())

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        _, (h_n, _) = self.lstm(x)
        return self.head(h_n[-1]).squeeze(-1)  # (batch,)


def train(epochs: int = 5) -> SurgeLSTM:
    """Train on labeled sequences and log to MLflow.

    TODO(nashat-plan Task 4): load real labeled sequences from HDFS gold features. This scaffold
    trains on synthetic data so the artifact + MLflow flow works end-to-end at zero cost.
    """
    model = SurgeLSTM()
    x = torch.randn(256, SEQ_LEN, N_FEATURES)
    y = (x.mean(dim=(1, 2)) > 0).float()  # synthetic label
    opt = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_fn = nn.BCELoss()
    for _ in range(epochs):
        opt.zero_grad()
        loss = loss_fn(model(x), y)
        loss.backward()
        opt.step()
    return model


if __name__ == "__main__":  # pragma: no cover
    import mlflow

    m = train()
    with mlflow.start_run(run_name="surge_classifier"):
        mlflow.pytorch.log_model(m, artifact_path="model", registered_model_name="surge_classifier")
    print("Trained and logged surge_classifier to MLflow.")
