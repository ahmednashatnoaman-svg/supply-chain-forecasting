"""IO clients shared by all layers: Redis, Kafka (Avro), and Spark session builders.
Layers must use these instead of instantiating raw clients.

Lazily imported (PEP 562 module __getattr__): importing a single submodule -- e.g.
`libs.scf_common.io.kafka` for a pure-ingestion consumer -- always runs this package's __init__
first, so an eager `import get_spark` here would force pyspark to be installed even when nothing
in that submodule needs it. Deferring the import until the name is actually accessed keeps each
layer's real dependency footprint honest.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from libs.scf_common.io.redis_client import get_redis as get_redis
    from libs.scf_common.io.spark import get_spark as get_spark

__all__ = ["get_redis", "get_spark"]


def __getattr__(name: str):
    if name == "get_redis":
        from libs.scf_common.io.redis_client import get_redis

        return get_redis
    if name == "get_spark":
        from libs.scf_common.io.spark import get_spark

        return get_spark
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
