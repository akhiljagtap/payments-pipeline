#create the fake events for KAFKA

import random
from faker import Faker
from src.common.schemas import PaymentEvent
from datetime import datetime, timedelta, timezone

fake = Faker()

CARD_POOL = [f"card_{i:05d}" for i in range(1, 501)]
MERCHANT_POOL = [f"m_{i:04d}" for i in range(1, 101)]
CHANNELS = ["ECOM", "POS", "ATM"]
COUNTRIES = ["IN", "US", "GB", "AE"]
STATUSES = ["APPROVED", "DECLINED"]

def make_valid_event() -> PaymentEvent:
    return PaymentEvent(
        card_id=random.choice(CARD_POOL),
        merchant_id=random.choice(MERCHANT_POOL),
        amount=round(random.uniform(10, 25000), 2),
        currency="INR",
        channel=random.choice(CHANNELS),
        country=random.choice(COUNTRIES),
        status=random.choices(STATUSES, weights=[92, 8])[0],
    )

#Fult injection logic

recent_events: list[PaymentEvent] = []
MAX_RECENT = 50

def _remember(event: PaymentEvent):
    recent_events.append(event)
    if len(recent_events) > MAX_RECENT:
        recent_events.pop(0)

def make_late_event() -> PaymentEvent:
    event = make_valid_event()
    minutes_late = random.randint(2, 15)
    late_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_late)
    event.event_time = late_time.isoformat()
    return event

def make_corrupt_event() -> dict:
    event = make_valid_event()
    payload = event.model_dump()

    corruption = random.choice(["negative_amount", "bad_currency", "missing_field", "broken_json"])

    if corruption == "negative_amount":
        payload["amount"] = -abs(payload["amount"])
    elif corruption == "bad_currency":
        payload["currency"] = "XXX"
    elif corruption == "missing_field":
        del payload["card_id"]
    elif corruption == "broken_json":
        return {"__broken_json__": True, "transaction_id": payload["transaction_id"]}

    payload["__corruption_type__"] = corruption
    return payload