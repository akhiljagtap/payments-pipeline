import random
import signal
import time

from confluent_kafka import Producer
from src.common.config import settings
from src.common.logger import get_logger
from src.producer.event_factory import make_valid_event

log = get_logger(__name__)


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        log.info(
            f"Delivered to {msg.topic()} [partition {msg.partition()}] "
            f"at offset {msg.offset()}"
        )


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "socket.timeout.ms": 5000,
        "message.timeout.ms": 10000,
    })


running = True


def handle_shutdown(signum, frame):
    global running
    log.info(f"Received signal {signum}, shutting down gracefully...")
    running = False


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    producer = build_producer()
    count = 0

    log.info("Producer starting. Press Ctrl+C to stop.")

    while running:
        event = make_valid_event()
        payload = event.model_dump_json()

        producer.produce(
            topic=settings.kafka_topic_transactions,
            key=event.card_id,
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)

        count += 1
        if settings.producer_max_events and count >= settings.producer_max_events:
            log.info(f"Reached max events ({count}), stopping.")
            break

        delay_ms = random.randint(
            settings.producer_min_delay_ms, settings.producer_max_delay_ms
        )
        time.sleep(delay_ms / 1000)

    log.info(f"Flushing remaining messages. Total produced: {count}")
    producer.flush(10)
    log.info("Producer stopped cleanly.")