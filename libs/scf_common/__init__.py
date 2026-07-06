"""Shared library imported by every layer. Home of config, IO clients, contract accessors,
and observability. Layers must not instantiate raw Kafka/Redis/Spark clients directly."""

__all__ = ["config", "contracts", "io", "observability"]
