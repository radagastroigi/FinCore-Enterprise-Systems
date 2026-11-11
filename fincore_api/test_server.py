from fastapi.testclient import TestClient
from server import app, ip_request_windows
import uuid

client = TestClient(app)

def test_atomic_payment_and_idempotency():
    ip_request_windows.clear()
    tx_id = f"UPI-TEST-{uuid.uuid4().hex[:6]}"
    payload ={
        "tx_id": tx_id,
        "sender_account": "ACC-SENDER-01",
        "receiver_account": "ACC-MERCHANT-99",
        "amount_inr": 25000.0,
        "geo_mismatch": False
    }

    # 1.first request commits to ledger
    response = client.post("/api/v1/payments/transfer", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "COMMITTED_ACID"

    # 2.second request fails with idempotency violation
    response = client.post("/api/v1/payments/transfer", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "IDEMPOTENT_REPLAY_BLOCKED"

    # 3. verify double entry ledger sums to zero
    response = client.get("/api/v1/ledger/audit")
    assert response.status_code == 200
    assert response.json()["global_zero_sum_balance"] == 0.0

def test_sliding_window_rate_limiting():
    ip_request_windows.clear()
    # fire 5 requests from the same IP, 6th request should be rate limited
    for _ in range(5):
        response = client.get("/api/v1/ledger/audit")
        assert response.status_code == 200

    res_blocked = client.get("/api/v1/ledger/audit")
    assert res_blocked.status_code == 429
    assert res_blocked.json()["error"] == "Rate limit exceeded"