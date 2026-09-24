import random
import signal
import time

from confluent_kafka import Producer
from src.common.config import settings
from src.common.logger import get_logger
from src.producer.event_factory import make_valid_event
from src.producer.event_factory import make_valid_event, make_late_event, make_corrupt_event, recent_events, _remember
from src.producer.ground_truth import log_event
import json as json_lib

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
        roll = random.random()
        key_for_send = None

        if roll < settings.fault_duplicate_rate and recent_events:
            event = random.choice(recent_events)
            payload = event.model_dump_json()
            key_for_send = event.card_id
            log_event("duplicate", event.transaction_id, "resent from recent buffer")

        elif roll < settings.fault_duplicate_rate + settings.fault_late_rate:
            event = make_late_event()
            payload = event.model_dump_json()
            key_for_send = event.card_id
            _remember(event)
            log_event("late", event.transaction_id, f"backdated to {event.event_time}")

        elif roll < settings.fault_duplicate_rate + settings.fault_late_rate + settings.fault_corrupt_rate:
            corrupt_payload = make_corrupt_event()
            payload = json_lib.dumps(corrupt_payload)
            txn_id = corrupt_payload.get("transaction_id", "unknown")
            key_for_send = txn_id
            log_event("corrupt", txn_id, corrupt_payload.get("__corruption_type__", "broken_json"))

        else:
            event = make_valid_event()
            payload = event.model_dump_json()
            key_for_send = event.card_id
            _remember(event)

        producer.produce(
            topic=settings.kafka_topic_transactions,
            key=key_for_send,
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