"""Unit tests for the shared contract accessors (Nashat plan Task 1). Pure — no services."""

import pytest

from libs.scf_common.contracts import HdfsPaths, RedisKeys, Topics

pytestmark = pytest.mark.unit


def test_topic_names():
    assert Topics.LIVE_WEB_TRAFFIC == "live_web_traffic"
    assert Topics.INVENTORY_UPDATES == "inventory_updates"
    assert Topics.SYSTEM_ALERTS == "system_alerts"
    assert Topics.AUTOMATED_PRICING_UPDATES == "automated_pricing_updates"
    assert len(Topics.ALL) == 4


def test_redis_keys():
    assert RedisKeys.forecast("10") == "forecast:10"
    assert RedisKeys.elasticity("10") == "graph:elasticity:10"
    assert RedisKeys.community("10") == "graph:community:10"
    assert RedisKeys.price("10") == "price:current:10"
    assert RedisKeys.inventory("10") == "inventory:10"
    assert RedisKeys.velocity("10", "60s") == "velocity:10:60s"


def test_hdfs_paths():
    assert HdfsPaths.bronze("events").endswith("/data/bronze/events")
    assert HdfsPaths.silver("events_cleaned").endswith("/data/silver/events_cleaned")
    assert HdfsPaths.gold("forecast").endswith("/data/gold/forecast")
