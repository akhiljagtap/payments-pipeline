#create the fake events for KAFKA

import random
from faker import Faker
from src.common.schemas import PaymentEvent

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