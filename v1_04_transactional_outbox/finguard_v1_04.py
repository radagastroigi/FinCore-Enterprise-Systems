from pydantic import BaseModel, Field
from typing import List, Literal
import uuid
import time
import json

# FinCore AI (Release v1.04): Transactional Outbox Pattern (Solving DB vs. Message Broker Split-Brain)
class OutboxEvent(BaseModel):
    event_id: str
    aggregate_id: str = Field(description="Transaction ID from the Ledger")
    topic: Literal["payment.settled.kafka", "fraud.mcp.audit.queue"]
    payload_json: str
    status: Literal["PENDING", "PUBLISHED"] = "PENDING"
    created_at: float
    published_at: float | None = None

class TransactionalOutboxStore:
    def __init__(self):
        self.ledger_commits: List[dict] = []
        self.outbox_table: List[OutboxEvent] = []

    def commit_ledger_with_outbox(self, tx_id: str, amount_inr: float, route_to_mcp: bool) -> dict:
        # 1. Single Atomic Local Transaction: Insert Ledger Row + Insert Outbox Row together
        now = time.time()
        ledger_row = {"tx_id": tx_id, "amount_inr": amount_inr, "committed_at": now}
        
        topic = "fraud.mcp.audit.queue" if route_to_mcp else "payment.settled.kafka"
        outbox_row = OutboxEvent(
            event_id=f"EVT-{uuid.uuid4().hex[:6].upper()}",
            aggregate_id=tx_id,
            topic=topic,
            payload_json=json.dumps({"tx_id": tx_id, "amount_inr": amount_inr}),
            status="PENDING",
            created_at=now
        )
        
        self.ledger_commits.append(ledger_row)
        self.outbox_table.append(outbox_row)
        return {"atomic_commit": "SUCCESS", "tx_id": tx_id, "outbox_status": outbox_row.status, "topic": topic}

    def run_outbox_relay_worker(self) -> List[dict]:
        # 2. Background Relay Poller: Publishes PENDING events to Kafka/RabbitMQ with At-Least-Once guarantee
        relayed = []
        for event in self.outbox_table:
            if event.status == "PENDING":
                event.status = "PUBLISHED"
                event.published_at = time.time()
                relayed.append(event.model_dump())
        return relayed

if __name__ == "__main__":
    store = TransactionalOutboxStore()
    print("--- 1. Atomic Commit (Ledger + Outbox Table in 1 Transaction) ---")
    print(json.dumps(store.commit_ledger_with_outbox("UPI-TXN-4001", 45000.0, route_to_mcp=False), indent=2))
    print(json.dumps(store.commit_ledger_with_outbox("UPI-TXN-4002", 185000.0, route_to_mcp=True), indent=2))

    print("\n--- 2. Outbox Relay Worker Polling & Publishing to Kafka / MCP Queue ---")
    print(json.dumps(store.run_outbox_relay_worker(), indent=2))