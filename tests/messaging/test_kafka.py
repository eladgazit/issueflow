from datetime import UTC, datetime

import pytest

from conftest import FakeBroker
from issueflow.messaging.kafka import KafkaPublisher, KafkaReader, KafkaSettings, KafkaUnavailableError
from issueflow.messaging.messages import MessageError, RunEnd, RunStart, decode

START = RunStart("r1", datetime(2025, 3, 1, tzinfo=UTC), "o/r")


def test_publish_sends_encoded_message_keyed_by_run(broker: FakeBroker) -> None:
    broker.publisher().publish(START)
    assert decode(broker.records[0]) == START
    assert broker.keys == [b"r1"]


def test_flush_waits_on_the_producer(broker: FakeBroker) -> None:
    publisher = broker.publisher()
    publisher.publish(START)
    publisher.flush()
    assert broker.flushes == 1


def test_flush_raises_when_a_send_failed(broker: FakeBroker) -> None:
    broker.send_error = RuntimeError("broker rejected")
    publisher = broker.publisher()
    publisher.publish(START)
    with pytest.raises(RuntimeError, match="broker rejected"):
        publisher.flush()


def test_close_closes_the_producer(broker: FakeBroker) -> None:
    broker.publisher().close()
    assert broker.producer_closed


def test_reader_yields_decoded_messages_in_order(broker: FakeBroker) -> None:
    publisher = broker.publisher()
    publisher.publish(START)
    publisher.publish(RunEnd("r1", 0))
    assert list(broker.reader()) == [START, RunEnd("r1", 0)]


def test_reader_resumes_from_the_committed_position(broker: FakeBroker) -> None:
    publisher = broker.publisher()
    publisher.publish(START)
    publisher.publish(RunEnd("r1", 0))
    first = broker.reader()
    assert next(iter(first)) == START
    first.commit()
    assert list(broker.reader()) == [RunEnd("r1", 0)]


def test_reader_does_not_move_the_position_without_commit(broker: FakeBroker) -> None:
    broker.publisher().publish(START)
    list(broker.reader())
    assert list(broker.reader()) == [START]


def test_reader_raises_on_an_undecodable_record(broker: FakeBroker) -> None:
    broker.records.append(b"garbage")
    with pytest.raises(MessageError):
        list(broker.reader())


def test_close_closes_the_consumer(broker: FakeBroker) -> None:
    broker.reader().close()
    assert broker.consumer_closed


def test_idle_timeout_must_be_positive() -> None:
    with pytest.raises(ValueError, match="idle_timeout"):
        KafkaSettings(idle_timeout=0)


def test_unreachable_broker_gives_a_helpful_error() -> None:
    settings = KafkaSettings(bootstrap_servers="127.0.0.1:1")  # nothing listens here
    with pytest.raises(KafkaUnavailableError, match="docker compose up -d"):
        KafkaPublisher(settings)
    with pytest.raises(KafkaUnavailableError, match="127.0.0.1:1"):
        KafkaReader(settings)
