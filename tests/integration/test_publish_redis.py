"""Integration test: batch publish -> Redis (Emad plan Task 6). Needs a Redis container."""

import pytest

pytest.importorskip("testcontainers")
pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="Scaffold — implement with testcontainers Redis per emad-plan Task 6")
def test_publish_forecasts_writes_contract_keys():
    """
    Plan:
      1. Start Redis via testcontainers; point settings.redis at it.
      2. Build tiny forecast_df + graph_df.
      3. publish_forecasts(forecast_df, graph_df).
      4. assert redis.get(RedisKeys.forecast(10)) == "100.0" and elasticity is a JSON list.
    """
