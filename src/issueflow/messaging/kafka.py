from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from issueflow.messaging.messages import Message, decode, encode

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC = "issueflow.issues"
DEFAULT_GROUP = "issueflow-csv"
DEFAULT_IDLE_TIMEOUT = 120.0  # longer than the rate limiter's default 60 s wait


class KafkaUnavailableError(Exception):
    """No Kafka broker answered."""

    def __init__(self, bootstrap_servers: str) -> None:
        super().__init__(
            f"cannot reach Kafka at {bootstrap_servers}; start it with `docker compose up -d` "
            "or pass --bootstrap-servers"
        )


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = DEFAULT_BOOTSTRAP
    topic: str = DEFAULT_TOPIC
    group_id: str = DEFAULT_GROUP
    idle_timeout: float = DEFAULT_IDLE_TIMEOUT  # seconds of silence after which the reader stops

    def __post_init__(self) -> None:
        if self.idle_timeout <= 0:
            raise ValueError("idle_timeout must be > 0")


class KafkaPublisher:
    """Sends messages to the topic. Pass `producer` to use a fake (tests)."""

    def __init__(self, settings: KafkaSettings, *, producer: Any = None) -> None:
        self._settings = settings
        self._errors: list[BaseException] = []
        if producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=settings.bootstrap_servers,
                    acks="all",
                    retries=5,
                    max_in_flight_requests_per_connection=1,  # keep messages in order across retries
                    linger_ms=20,
                )
            except NoBrokersAvailable:
                raise KafkaUnavailableError(settings.bootstrap_servers) from None
        self._producer = producer

    def publish(self, message: Message) -> None:
        future = self._producer.send(self._settings.topic, key=message.run_id.encode(), value=encode(message))
        future.add_errback(self._errors.append)

    def flush(self) -> None:
        """Wait until every message is acknowledged; raise if any send failed."""
        self._producer.flush()
        if self._errors:
            raise self._errors[0]

    def close(self) -> None:
        self._producer.close()


class KafkaReader:
    """Reads messages from the last committed position. Pass `consumer` to use a fake (tests).

    Iteration ends after `idle_timeout` seconds without a message. Auto-commit is off: the
    position moves only when `commit()` is called.
    """

    def __init__(self, settings: KafkaSettings, *, consumer: Any = None) -> None:
        if consumer is None:
            try:
                consumer = KafkaConsumer(
                    settings.topic,
                    bootstrap_servers=settings.bootstrap_servers,
                    group_id=settings.group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    consumer_timeout_ms=int(settings.idle_timeout * 1000),
                )
            except NoBrokersAvailable:
                raise KafkaUnavailableError(settings.bootstrap_servers) from None
        self._consumer = consumer

    def __iter__(self) -> Iterator[Message]:
        for record in self._consumer:
            yield decode(record.value)

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()


__all__ = [
    "KafkaError",
    "KafkaPublisher",
    "KafkaReader",
    "KafkaSettings",
    "KafkaUnavailableError",
]
