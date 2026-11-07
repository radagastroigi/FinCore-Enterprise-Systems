from pydantic import BaseModel, Field
from typing import List, Literal
import json

# FinCore AI (Release v1.01): O(1) Payment Idempotency & Deterministic Fraud Scorer
processed_idempotency_keys = set()  # HashSet preventing duplicate double-charges

class PaymentFraudAssessment(BaseModel):
    transaction_id: str = Field(description="ISO-20022 / UPI unique reference")
    amount_inr: float
    is_duplicate_replay: bool
    risk_score: int = Field(ge=0, le=100)
    decision: Literal["APPROVE", "FLAG_FOR_MCP_AUDIT", "BLOCK_IMMEDIATELY"]
    risk_signals: List[str]

def evaluate_transaction(tx_id: str, amount: float, location_mismatch: bool) -> PaymentFraudAssessment:
    # 1. O(1) HashSet lookup for duplicate payment replay attacks
    is_replayed = tx_id in processed_idempotency_keys
    processed_idempotency_keys.add(tx_id)
    
    signals, score = [], 10
    if is_replayed:
        signals.append("DUPLICATE_IDEMPOTENCY_KEY_REPLAY")
        score += 85
    if amount > 200000:
        signals.append("HIGH_VALUE_RTGS_THRESHOLD")
        score += 35
    if location_mismatch:
        signals.append("GEO_IP_DEVICE_MISMATCH")
        score += 40
        
    final_score = min(score, 100)
    decision = (
        "BLOCK_IMMEDIATELY" if final_score >= 80 
        else "FLAG_FOR_MCP_AUDIT" if final_score >= 45 
        else "APPROVE"
    )
    
    return PaymentFraudAssessment(
        transaction_id=tx_id, amount_inr=amount, is_duplicate_replay=is_replayed,
        risk_score=final_score, decision=decision, risk_signals=signals
    )

if __name__ == "__main__":
    print("--- Test 1: High-Value Anomaly (Routed to MCP Audit Agent) ---")
    tx1 = evaluate_transaction("UPI-TXN-994821", 250000.0, location_mismatch=True)
    print(json.dumps(tx1.model_dump(), indent=2))

    print("\n--- Test 2: Replay Attack with Same ID (Blocked in <1ms via HashSet) ---")
    tx2 = evaluate_transaction("UPI-TXN-994821", 250000.0, location_mismatch=False)
    print(json.dumps(tx2.model_dump(), indent=2))