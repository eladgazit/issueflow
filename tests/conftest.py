from dataclasses import dataclass, field
from typing import Any

import pytest

from issueflow.messaging.kafka import KafkaPublisher, KafkaReader, KafkaSettings

SETTINGS = KafkaSettings()


class FakeFuture:
    def __init__(self, error: BaseException | None) -> None:
        self._error = error

    def add_errback(self, callback: Any) -> None:
        if self._error is not None:
            callback(self._error)


@dataclass
class FakeRecord:
    value: bytes


@dataclass
class FakeBroker:
    """An in-memory single-partition topic that stores the real encoded bytes."""

    records: list[bytes] = field(default_factory=list)
    committed: int = 0  # position saved by commit()
    position: int = 0  # next record the consumer will read
    send_error: BaseException | None = None
    keys: list[bytes] = field(default_factory=list)
    flushes: int = 0
    producer_closed: bool = False
    consumer_closed: bool = False

    def producer(self) -> "FakeProducer":
        return FakeProducer(self)

    def consumer(self) -> "FakeConsumer":
        return FakeConsumer(self)

    def publisher(self) -> KafkaPublisher:
        return KafkaPublisher(SETTINGS, producer=self.producer())

    def reader(self) -> KafkaReader:
        return KafkaReader(SETTINGS, consumer=self.consumer())


class FakeProducer:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker

    def send(self, topic: str, key: bytes, value: bytes) -> FakeFuture:
        assert topic == SETTINGS.topic
        self.broker.keys.append(key)
        if self.broker.send_error is None:
            self.broker.records.append(value)
        return FakeFuture(self.broker.send_error)

    def flush(self) -> None:
        self.broker.flushes += 1

    def close(self) -> None:
        self.broker.producer_closed = True


class FakeConsumer:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.broker.position = broker.committed  # a new consumer resumes from the committed position

    def __iter__(self) -> "FakeConsumer":
        return self

    def __next__(self) -> FakeRecord:
        if self.broker.position >= len(self.broker.records):
            raise StopIteration  # what kafka-python does after consumer_timeout_ms
        record = FakeRecord(self.broker.records[self.broker.position])
        self.broker.position += 1
        return record

    def commit(self) -> None:
        self.broker.committed = self.broker.position

    def close(self) -> None:
        self.broker.consumer_closed = True


@pytest.fixture
def broker() -> FakeBroker:
    return FakeBroker()
