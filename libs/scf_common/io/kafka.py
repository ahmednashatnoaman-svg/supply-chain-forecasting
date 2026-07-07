from __future__ import annotations

from io import BytesIO

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from libs.scf_common.config import get_settings


def load_schema(schema_file: str) -> dict:
    """Load Avro schema from contracts/avro directory."""
    from pathlib import Path

    schema_path = Path(__file__).resolve().parents[3] / "contracts" / "avro" / schema_file
    return schema_path.read_text()


def _sr_client() -> SchemaRegistryClient:
    settings = get_settings()
    return SchemaRegistryClient({"url": settings.kafka.schema_registry_url})


class AvroKafkaProducer:
    """Produce Avro records to a topic. `key_field` is used as the partition key.

    Reliability config (kafka-playbook §1) is baked in: ``acks=all`` +
    ``enable.idempotence=true`` + a high retry count + lz4 compression. On a single-broker
    dev cluster ``acks=all`` simply means "ack from the one in-sync broker" (R-3, safe).
    """

    # kafka-playbook §1 -- shared by every producer (Hatem/Nashat). Idempotence requires
    # acks=all; confluent-kafka enforces that automatically when enable.idempotence is set.
    _RELIABILITY = {
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2147483647,
        "compression.type": "lz4",
    }

    def __init__(self, topic: str, schema_file: str, key_field: str = "item_id"):
        settings = get_settings()
        self.topic = topic
        self.key_field = key_field
        self._producer = Producer(
            {"bootstrap.servers": settings.kafka.bootstrap_servers, **self._RELIABILITY}
        )
        self._serializer = AvroSerializer(_sr_client(), load_schema(schema_file))

    def produce(self, record: dict) -> None:
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        self._producer.produce(
            topic=self.topic,
            key=str(record[self.key_field]),
            value=self._serializer(record, ctx),
        )
        # Serve delivery-report callbacks so the internal message buffer is reaped. Without
        # poll(0), queue.buffering.max.messages fills and the next produce() blocks forever
        # under sustained load (the trailing flush() only polls after the loop already hung).
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)


class AvroKafkaConsumer:
    """Consume Avro records from a topic as plain dicts.

    Manual offset commit (kafka-playbook §2): ``enable.auto.commit=false`` by default, and callers
    commit explicitly via :meth:`commit` only after downstream writes succeed (at-least-once safe
    because the processing functions are idempotent).
    """

    def __init__(self, topic: str, schema_file: str, group_id: str):
        settings = get_settings()
        self.topic = topic
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka.bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([topic])
        self._deserializer = AvroDeserializer(_sr_client())

    def poll(self, timeout: float = 1.0) -> dict | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        return self._deserializer(msg.value(), ctx)

    def commit(self) -> None:
        """Commit the current offsets synchronously (call after successful processing)."""
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
