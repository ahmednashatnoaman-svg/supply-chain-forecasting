"""IO clients shared by all layers: Redis, Kafka (Avro), and Spark session builders.
Layers must use these instead of instantiating raw clients."""

from libs.scf_common.io.redis_client import get_redis
from libs.scf_common.io.spark import get_spark

__all__ = ["get_redis", "get_spark"]
