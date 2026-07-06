# MLOps Playbook (project-specific)

Model lifecycle for the two trained models in this system, both registered in the self-hosted, free
MLflow instance (`infra/docker/docker-compose.yml`'s `mlflow` service, profile `full`). Owner: **Emad**
(forecast model), **Nashat** (LSTM surge classifier).

## 1. The two models

| Model | Trained by | Registry name | Signature |
|---|---|---|---|
| Demand forecast (GBT) | `batch/mllib/forecast.py` (nightly, Airflow) | `demand_forecast` | `contracts/models/forecast_output.json` |
| Surge classifier (LSTM) | `streaming/lstm/model.py` (ad hoc, `python -m streaming.lstm.model`) | `surge_classifier` | `contracts/models/lstm_signature.json` |

## 2. Lifecycle: train → log → register → promote

```
train (batch/mllib/forecast.py or streaming/lstm/model.py)
        │
        ▼
mlflow.spark.log_model(...) / mlflow.pytorch.log_model(...)   # artifact + params + metrics
        │
        ▼
Model Registry: registered_model_name="demand_forecast"        # version N created, stage=None
        │
        ▼  (manual or scripted validation against a holdout set)
        ▼
transition to stage="Staging"                                  # candidate for the pricing stream
        │
        ▼  (canary: run alongside current Production model, compare)
        ▼
transition to stage="Production"                                # streaming job loads this stage
```

- **Never skip Staging.** The pricing engine reads whatever is tagged `Production` in MLflow — promote
  directly only for the very first model version (nothing to compare against yet).
- **Archive, don't delete**, the previous Production version when promoting a new one — MLflow's
  `transition_model_version_stage` archives automatically, giving you an instant rollback path.

## 3. Concrete commands (against the local MLflow at `http://localhost:5000`)

```python
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient(tracking_uri="http://localhost:5000")

# Promote the latest Staging version to Production (only after validation passes)
latest_staging = client.get_latest_versions("demand_forecast", stages=["Staging"])[0]
client.transition_model_version_stage(
    name="demand_forecast", version=latest_staging.version,
    stage="Production", archive_existing_versions=True,
)
```

## 4. Loading the Production model at inference time

- **Batch (`batch/mllib/forecast.py`):** trains fresh every night — no need to "load" a prior model,
  but log every run's metrics so forecast drift is visible in the MLflow UI over time.
- **Streaming (`streaming/lstm/infer.py`):** currently broadcasts weights passed in directly
  (`build_surge_pandas_udf(broadcast_state_dict)`). To load the actual Production-staged model instead
  of an ad hoc one: `mlflow.pytorch.load_model("models:/surge_classifier/Production")` once at stream
  startup, then `spark.sparkContext.broadcast(model.state_dict())` — this is the remaining wiring for
  Task 5 (`nashat-plan.md`).

## 5. Drift & retraining triggers

- **Data drift:** the batch DAG's Great Expectations gate (`batch/etl/expectations.py`, Task 3) already
  fails loudly on schema/range violations in silver data — treat a gate failure as a signal to inspect
  the forecast model's inputs before the next training run, not just a data bug.
- **Model drift (forecast accuracy):** log a backtest metric (e.g., MAPE against last week's actuals)
  with every `mlflow.spark.log_model` call; a sustained upward trend in the MLflow UI is the retraining
  trigger — no automated retrain-on-drift job exists yet (would be a good M5+ addition).
- **Surge classifier drift:** harder to detect without labeled surge/no-surge ground truth; until real
  labels exist, treat `predict_batch`'s fallback-to-zero path (see `streaming/lstm/infer.py`) as the
  safety net — a broken/stale model degrades to velocity-only pricing, never garbage output.

## 6. Zero-cost note

Everything above runs against the **self-hosted MLflow container** — no Databricks, no SageMaker, no
paid model registry. See [cost-and-licensing.md](cost-and-licensing.md).
