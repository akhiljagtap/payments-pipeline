import uuid
from datetime import datetime, timezone
from pydantic import BaseModel, Field

class PaymentEvent(BaseModel):
    schema_version: int = 1
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4())) #need to genrate
    card_id: str
    merchant_id: str
    amount: float
    currency: str
    channel: str
    country: str
    status: str
    event_time: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat() #need to genrate 
    )