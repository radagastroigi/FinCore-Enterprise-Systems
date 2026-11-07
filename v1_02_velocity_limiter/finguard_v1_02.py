import time
from collections import defaultdict, deque
from pydantic import BaseModel
from typing import Dict, Deque, List, Literal
import json

# Experiment parameters: 6-second rolling window for fast real-time testing
WINDOW_SECONDS = 6
MAX_TX_PER_WINDOW = 3
MAX_AMOUNT_PER_WINDOW_INR = 100000.0

user_tx_window: Dict[str, Deque[tuple[float, float]]] = defaultdict(deque)

class VelocityCheckResult(BaseModel):
    account_id: str
    tx_id: str
    timestamp_sec: float
    amount_inr: float
    tx_count_in_window: int
    rolling_volume_inr: float
    status: Literal["APPROVED", "VELOCITY_THROTTLED_TO_MCP_AGENT", "BLOCKED_BURST_ATTACK"]
    triggered_rules: List[str]

def check_payment_velocity_live(account_id: str, tx_id: str, amount_inr: float) -> VelocityCheckResult:
    current_time = time.time()  # Actual epoch timestamp in seconds (float)
    window = user_tx_window[account_id]
    
    # 1. Print state BEFORE eviction
    initial_len = len(window)
    
    # 2. Sliding Window Eviction: Drop anything older than WINDOW_SECONDS
    evicted_count = 0
    while window and (current_time - window[0][0]) > WINDOW_SECONDS:
        expired_tx = window.popleft()
        evicted_count += 1
        print(f"   [EVICTED] Old Tx recorded at {expired_tx[0]:.2f} (Age: {current_time - expired_tx[0]:.1f}s ago > {WINDOW_SECONDS}s window)")
        
    # 3. Append current transaction
    window.append((current_time, amount_inr))
    
    tx_count = len(window)
    rolling_volume = sum(amt for _, amt in window)
    
    rules = []
    if tx_count > MAX_TX_PER_WINDOW:
        rules.append(f"BURST_FREQUENCY_EXCEEDED ({tx_count} tx in window)")
    if rolling_volume > MAX_AMOUNT_PER_WINDOW_INR:
        rules.append(f"ROLLING_VOLUME_CAP_EXCEEDED (INR {rolling_volume:,.0f} in window)")
        
    if tx_count > MAX_TX_PER_WINDOW and rolling_volume > MAX_AMOUNT_PER_WINDOW_INR:
        status = "BLOCKED_BURST_ATTACK"
    elif rules:
        status = "VELOCITY_THROTTLED_TO_MCP_AGENT"
    else:
        status = "APPROVED"
        
    return VelocityCheckResult(
        account_id=account_id,
        tx_id=tx_id,
        timestamp_sec=round(current_time, 2),
        amount_inr=amount_inr,
        tx_count_in_window=tx_count,
        rolling_volume_inr=rolling_volume,
        status=status,
        triggered_rules=rules
    )

if __name__ == "__main__":
    print(f"=== Starting Live Sliding Window Simulation ({WINDOW_SECONDS}s Window) ===\n")
    
    # Step A: Fire 3 legitimate transactions quickly
    for i in range(1, 4):
        print(f"--> Firing Tx #{i}...")
        res = check_payment_velocity_live("ACC-771", f"UPI-00{i}", 30000.0)
        print(f"    Result: {res.status} | Active Window Count: {res.tx_count_in_window} | Volume: INR {res.rolling_volume_inr:,.0f}\n")
        time.sleep(1)  # 1-second pause between transactions
        
    # Step B: Fire 4th transaction at ~3.5s -> Triggers BLOCKED_BURST_ATTACK
    print("--> Firing Tx #4 (Burst Attack Attempt)...")
    res4 = check_payment_velocity_live("ACC-771", "UPI-004", 30000.0)
    print(f"    Result: {res4.status} | Rules: {res4.triggered_rules}\n")
    
    # Step C: Sleep past the window duration (sleep 7 seconds)
    print(f"--> [SLEEPING FOR 7 SECONDS] Waiting for window to slide and evict older transactions...\n")
    time.sleep(7)
    
    # Step D: Fire Tx #5 after sleep -> Watch all 4 old transactions get evicted!
    print("--> Firing Tx #5 after waiting 7 seconds...")
    res5 = check_payment_velocity_live("ACC-771", "UPI-005", 25000.0)
    print(f"    Result: {res5.status} | Active Window Count: {res5.tx_count_in_window} | Volume: INR {res5.rolling_volume_inr:,.0f}")