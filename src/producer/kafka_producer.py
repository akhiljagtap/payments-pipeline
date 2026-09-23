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

if __name__ == "__main__":
    producer = build_producer()
    event = make_valid_event()
    payload = event.model_dump_json()

    producer.produce(
        topic=settings.kafka_topic_transactions,
        key=event.card_id,
        value=payload,
        callback=delivery_report,
    )
    producer.flush()