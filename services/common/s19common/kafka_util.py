"""Thin Kafka helpers on top of confluent-kafka.

If KAFKA_BOOTSTRAP is empty the publisher becomes a no-op so services and
unit tests can run without a broker.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Iterable, Optional

log = logging.getLogger("kafka")

try:  # confluent-kafka is installed in the container image
    from confluent_kafka import Consumer, KafkaError, Producer
except ImportError:  # pragma: no cover - local unit tests without the wheel
    Consumer = Producer = None  # type: ignore
    KafkaError = None  # type: ignore

# Topic names are shared by every service and by the Flink job.
TOPIC_INCIDENT_EVENTS = "incident-events"
TOPIC_DISPATCH_EVENTS = "dispatch-events"
TOPIC_CONSISTENCY_EVENTS = "consistency-events"
TOPIC_AGGREGATES = "incident-aggregates"


class KafkaPublisher:
    def __init__(self, bootstrap: str, client_id: str):
        self.enabled = bool(bootstrap) and Producer is not None
        self._p = None
        if self.enabled:
            self._p = Producer(
                {
                    "bootstrap.servers": bootstrap,
                    "client.id": client_id,
                    "acks": "1",
                    "linger.ms": 20,
                    "message.timeout.ms": 10000,
                    "socket.keepalive.enable": True,
                }
            )
            log.info("kafka producer ready", extra={"bootstrap": bootstrap})
        else:
            log.warning("KAFKA_BOOTSTRAP not set - Kafka publishing disabled")

    def publish(self, topic: str, key: str, event: dict) -> bool:
        if not self.enabled:
            return False
        try:
            self._p.produce(topic, key=key.encode(), value=json.dumps(event).encode(), on_delivery=_on_delivery)
            self._p.poll(0)
            return True
        except BufferError:
            self._p.poll(0.5)
            log.warning("kafka local queue full, event dropped", extra={"topic": topic})
            return False
        except Exception:  # noqa: BLE001
            log.exception("kafka publish failed", extra={"topic": topic})
            return False

    def flush(self, timeout: float = 5.0) -> None:
        if self._p is not None:
            self._p.flush(timeout)


def _on_delivery(err, msg) -> None:
    if err is not None:
        log.warning("kafka delivery failed", extra={"error": str(err), "topic": msg.topic()})


class KafkaConsumerThread(threading.Thread):
    """Background poll loop calling ``handler(event_dict)`` for every JSON message."""

    def __init__(self, bootstrap: str, group_id: str, topics: Iterable[str],
                 handler: Callable[[dict], None], offset_reset: str = "latest"):
        super().__init__(daemon=True, name=f"kafka-{group_id}")
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.topics = list(topics)
        self.handler = handler
        self.offset_reset = offset_reset
        self._stop_event = threading.Event()
        self.processed = 0
        self.last_error: Optional[str] = None

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:  # pragma: no cover - needs a broker
        if not self.bootstrap or Consumer is None:
            log.warning("Kafka consumer disabled", extra={"group": self.group_id})
            return
        while not self._stop_event.is_set():
            try:
                c = Consumer(
                    {
                        "bootstrap.servers": self.bootstrap,
                        "group.id": self.group_id,
                        "auto.offset.reset": self.offset_reset,
                        "enable.auto.commit": True,
                    }
                )
                c.subscribe(self.topics)
                log.info("kafka consumer subscribed", extra={"group": self.group_id, "topics": self.topics})
                while not self._stop_event.is_set():
                    msg = c.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            self.last_error = str(msg.error())
                            log.warning("kafka consumer error", extra={"error": self.last_error})
                        continue
                    try:
                        self.handler(json.loads(msg.value()))
                        self.processed += 1
                    except Exception:  # noqa: BLE001
                        log.exception("kafka handler failed", extra={"topic": msg.topic()})
                c.close()
            except Exception as exc:  # noqa: BLE001
                self.last_error = str(exc)
                log.exception("kafka consumer crashed, retrying in 5s")
                time.sleep(5)


def now_ms() -> int:
    return int(time.time() * 1000)
