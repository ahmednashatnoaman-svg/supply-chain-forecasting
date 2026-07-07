"""Regression test for issue #41: libs.scf_common.io's submodules must load independently.

Before the fix, `libs/scf_common/io/__init__.py` eagerly imported both `get_redis` and `get_spark`
at module level. Importing ANY submodule -- e.g. `libs.scf_common.io.kafka`, needed only by
ingestion/streaming consumers -- always runs the parent package's `__init__.py` first, so a
pure-Kafka or pure-Redis consumer was forced to have pyspark importable even though nothing it
uses needs Spark. This asserts the import is now genuinely deferred (PEP 562 module __getattr__),
by checking `sys.modules` state rather than requiring pyspark to actually be absent (it's a real
project dependency and will be installed in CI).
"""

from __future__ import annotations

import sys

import pytest

pytestmark = pytest.mark.unit


def _unload_scf_io_modules() -> None:
    for name in list(sys.modules):
        if name == "libs.scf_common.io" or name.startswith("libs.scf_common.io."):
            del sys.modules[name]


def test_importing_kafka_submodule_does_not_eagerly_load_spark_module():
    _unload_scf_io_modules()

    import libs.scf_common.io.kafka  # noqa: F401

    assert "libs.scf_common.io.spark" not in sys.modules, (
        "importing io.kafka alone should not trigger io/__init__.py to eagerly import "
        "libs.scf_common.io.spark (and therefore pyspark) as a side effect"
    )


def test_get_redis_and_get_spark_still_resolve_via_the_package(monkeypatch):
    pytest.importorskip("pyspark")
    _unload_scf_io_modules()

    from libs.scf_common.io import get_redis, get_spark
    from libs.scf_common.io.redis_client import get_redis as direct_get_redis
    from libs.scf_common.io.spark import get_spark as direct_get_spark

    assert get_redis is direct_get_redis
    assert get_spark is direct_get_spark


def test_unknown_attribute_raises_attribute_error():
    import libs.scf_common.io as io_pkg

    with pytest.raises(AttributeError):
        _ = io_pkg.not_a_real_export
