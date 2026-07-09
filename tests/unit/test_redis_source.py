"""Unit test for the dashboard Redis source (Ziad plan Task 1). Uses fakeredis (free)."""

import pytest

fakeredis = pytest.importorskip("fakeredis")

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_redis(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr("libs.scf_common.io.redis_client.get_redis", lambda: client, raising=True)
    monkeypatch.setattr(
        "automation.dashboard.components.redis_source.get_redis", lambda: client, raising=True
    )
    return client


def test_get_price_and_missing(fake_redis):
    from automation.dashboard.components import redis_source

    fake_redis.set("price:current:10", 120.99)
    assert redis_source.get_price("10") == 120.99
    assert redis_source.get_price("999") is None


def test_get_velocity_and_missing(fake_redis):
    from automation.dashboard.components import redis_source

    fake_redis.set("velocity:10:60s", 42.5)
    assert redis_source.get_velocity("10") == 42.5
    assert redis_source.get_velocity("10", "300s") is None
    assert redis_source.get_velocity("999") is None


def test_list_skus_discovers_active_velocities(fake_redis):
    from automation.dashboard.components import redis_source

    assert redis_source.list_skus() == []
    # price:current:* is seeded catalog-wide by batch.etl.pricing (every SKU, live or not) --
    # list_skus() must key off velocity:*, the speed layer's own "I actually processed this SKU
    # this window" signal, or every dashboard view would try to render the entire catalog.
    for sku, price in [("10", 9.99), ("30", 4.5), ("20", 19.0)]:
        fake_redis.set(f"price:current:{sku}", price)
    assert redis_source.list_skus() == []
    for sku, velocity in [("10", 1.0), ("30", 2.0), ("20", 3.0)]:
        fake_redis.set(f"velocity:{sku}:60s", velocity)
    assert redis_source.list_skus() == ["10", "20", "30"]


def test_kpi_summary_empty_when_no_skus_priced(fake_redis):
    from automation.dashboard.components import redis_source

    kpi = redis_source.get_kpi_summary()
    assert kpi == {
        "active_skus": 0,
        "avg_price": None,
        "total_forecast": None,
        "surge_count": 0,
        "low_stock_count": 0,
    }


def test_kpi_summary_aggregates_across_skus(fake_redis):
    from automation.dashboard.components import redis_source

    fake_redis.set("price:current:10", 20.0)
    fake_redis.set("price:current:20", 10.0)
    fake_redis.set("forecast:10", 100.0)
    fake_redis.set("forecast:20", 50.0)
    # Scaled baseline for 100.0 is ~0.069. Velocity 0 -> 0 ratio (not surging).
    fake_redis.set("velocity:10:60s", 0.0)
    # Scaled baseline for 50.0 is ~0.035. Velocity 1.0 -> ratio ~28 (surging > 2.0).
    fake_redis.set("velocity:20:60s", 1.0)
    fake_redis.set("inventory:10", 500)  # above default reorder threshold (50)
    fake_redis.set("inventory:20", 10)  # below default reorder threshold -> low stock

    kpi = redis_source.get_kpi_summary()
    assert kpi["active_skus"] == 2
    assert kpi["avg_price"] == 15.0
    assert kpi["total_forecast"] == 150.0
    assert kpi["surge_count"] == 1
    assert kpi["low_stock_count"] == 1
