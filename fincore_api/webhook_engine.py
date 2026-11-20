import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Callable


@dataclass
class WebhookEvent:
    event_id: str
    payload: dict
    attempts: int = 0
    max_retries: int = 3
    next_retry_at: float = 0.0
    status: str = "PENDING"  # PENDING, DELIVERED, DLQ
    last_error: str = ""


class FlagshipWebhookEngine:
    def __init__(self, secret_key: str, base_backoff_sec: float = 2.0):
        self.secret_key = secret_key.encode()
        self.base_backoff_sec = base_backoff_sec
        self.queue: list[WebhookEvent] = []
        self.dlq: list[WebhookEvent] = []
        self.delivered_ids: set[str] = set()

    def sign_payload(self, payload: dict, timestamp: int) -> str:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        message = f"{timestamp}.{body}".encode()
        return hmac.new(self.secret_key, message, hashlib.sha256).hexdigest()

    def verify_signature(
        self,
        payload: dict,
        timestamp: int,
        signature: str,
        now: int,
        tolerance_sec: int = 300,
    ) -> bool:
        if abs(now - timestamp) > tolerance_sec:
            return False
        expected = self.sign_payload(payload, timestamp)
        return hmac.compare_digest(expected, signature)

    def dispatch(
        self,
        event: WebhookEvent,
        sender: Callable[[dict, str], bool],
        now: float,
    ) -> str:
        if event.event_id in self.delivered_ids:
            return "DUPLICATE_SKIPPED"

        sig = self.sign_payload(event.payload, int(now))

        try:
            ok = sender(event.payload, sig)
        except Exception as exc:
            ok = False
            event.last_error = str(exc)

        if ok:
            event.status = "DELIVERED"
            self.delivered_ids.add(event.event_id)
            return "DELIVERED"

        event.attempts += 1
        if not event.last_error:
            event.last_error = "DELIVERY_FAILED"

        if event.attempts >= event.max_retries:
            event.status = "DLQ"
            self.dlq.append(event)
            return "DLQ"

        delay = self.base_backoff_sec * (2 ** (event.attempts - 1))
        event.next_retry_at = now + delay
        self.queue.append(event)
        return "RETRY_SCHEDULED"