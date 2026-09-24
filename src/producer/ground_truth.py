""""
    This is a small, separate file whose only job is to record what we deliberately broke,
    so we can verify the pipeline handled it correctly later.
"""

import json
import os
from datetime import datetime, timezone
from src.common.config import settings

GROUND_TRUTH_PATH = "./data/ground_truth.jsonl"

def log_event(event_type: str, transaction_id: str, detail: str = ""):
    os.makedirs(os.path.dirname(GROUND_TRUTH_PATH), exist_ok=True)
    record = {
        "logged_at": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "transaction_id": transaction_id,
        "detail": detail,
    }
    with open(GROUND_TRUTH_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")

