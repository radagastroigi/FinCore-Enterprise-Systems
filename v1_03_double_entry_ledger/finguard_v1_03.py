from pydantic import BaseModel, Field
from typing import Dict, List, Literal
import uuid
import time
import json

# FinCore AI (Release v1.03): Razorpay/Zerodha-Style Immutable Double-Entry Ledger & Optimistic Concurrency
class LedgerEntry(BaseModel):
    entry_id: str
    transfer_id: str
    account_id: str
    entry_type: Literal["DEBIT", "CREDIT"]
    amount_inr: float  # Negative for DEBIT, Positive for CREDIT
    timestamp: float

class AccountWallet(BaseModel):
    account_id: str
    balance_inr: float
    version: int  # Optimistic Concurrency token (like EF Core [Timestamp] / RowVersion)

class DoubleEntryLedgerEngine:
    def __init__(self):
        self.wallets: Dict[str, AccountWallet] = {
            "ACC-SENDER-01": AccountWallet(account_id="ACC-SENDER-01", balance_inr=50000.0, version=1),
            "ACC-MERCHANT-99": AccountWallet(account_id="ACC-MERCHANT-99", balance_inr=10000.0, version=1),
        }
        self.immutable_ledger: List[LedgerEntry] = []

    def execute_atomic_transfer(self, sender_id: str, receiver_id: str, amount: float, expected_sender_version: int) -> dict:
        sender = self.wallets[sender_id]
        receiver = self.wallets[receiver_id]

        # 1. Optimistic Concurrency Check (Prevents Race Conditions / Double-Spend)
        if sender.version != expected_sender_version:
            return {
                "status": "CONCURRENCY_CONFLICT_ABORTED",
                "reason": f"Stale version! Expected v{expected_sender_version}, but DB is at v{sender.version}."
            }

        if sender.balance_inr < amount:
            return {"status": "INSUFFICIENT_FUNDS", "balance": sender.balance_inr}

        # 2. Append Immutable Double-Entry Rows (Sum of entries MUST equal 0.0)
        tx_id = f"TX-{uuid.uuid4().hex[:8].upper()}"
        now = time.time()
        debit = LedgerEntry(entry_id=f"E-{uuid.uuid4().hex[:6]}", transfer_id=tx_id, account_id=sender_id, entry_type="DEBIT", amount_inr=-amount, timestamp=now)
        credit = LedgerEntry(entry_id=f"E-{uuid.uuid4().hex[:6]}", transfer_id=tx_id, account_id=receiver_id, entry_type="CREDIT", amount_inr=+amount, timestamp=now)
        self.immutable_ledger.extend([debit, credit])

        # 3. Update Materialized Balances & Increment Optimistic Version
        sender.balance_inr -= amount
        sender.version += 1
        receiver.balance_inr += amount
        receiver.version += 1

        return {
            "status": "COMMITTED_ACID_TRANSFER",
            "transfer_id": tx_id,
            "zero_sum_check": (debit.amount_inr + credit.amount_inr == 0.0),
            "sender_new_state": sender.model_dump(),
            "receiver_new_state": receiver.model_dump(),
        }

if __name__ == "__main__":
    engine = DoubleEntryLedgerEngine()
    print("--- 1. Valid Atomic Transfer (Sender at v1) ---")
    print(json.dumps(engine.execute_atomic_transfer("ACC-SENDER-01", "ACC-MERCHANT-99", 15000.0, expected_sender_version=1), indent=2))

    print("\n--- 2. Concurrent Race-Condition Attack (Stale Thread still using v1) ---")
    print(json.dumps(engine.execute_atomic_transfer("ACC-SENDER-01", "ACC-MERCHANT-99", 15000.0, expected_sender_version=1), indent=2))