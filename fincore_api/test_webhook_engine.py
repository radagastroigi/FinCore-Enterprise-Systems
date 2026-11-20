from fincore_api.webhook_engine import FlagshipWebhookEngine, WebhookEvent


def test_signature_valid_tampered_and_stale():
    engine = FlagshipWebhookEngine("whsec_test_secret")
    payload = {"order_id": "ORD-1", "amount": 500}
    ts = 1_700_000_000
    sig = engine.sign_payload(payload, ts)

    assert engine.verify_signature(payload, ts, sig, now=ts + 10) is True
    assert engine.verify_signature({"order_id": "ORD-1", "amount": 999}, ts, sig, now=ts + 10) is False
    assert engine.verify_signature(payload, ts, sig, now=ts + 301) is False


def test_transient_failure_backoff_then_delivered():
    engine = FlagshipWebhookEngine("whsec_test_secret", base_backoff_sec=2.0)
    event = WebhookEvent(event_id="EVT-1", payload={"status": "PAID"}, max_retries=3)

    calls = {"count": 0}

    def flaky_sender(_payload: dict, _sig: str) -> bool:
        calls["count"] += 1
        return calls["count"] == 3

    assert engine.dispatch(event, flaky_sender, now=100.0) == "RETRY_SCHEDULED"
    assert event.attempts == 1
    assert event.next_retry_at == 102.0  # +2s

    assert engine.dispatch(event, flaky_sender, now=102.0) == "RETRY_SCHEDULED"
    assert event.attempts == 2
    assert event.next_retry_at == 106.0  # +4s

    assert engine.dispatch(event, flaky_sender, now=106.0) == "DELIVERED"
    assert event.status == "DELIVERED"
    assert "EVT-1" in engine.delivered_ids


def test_permanent_failure_moves_to_dlq():
    engine = FlagshipWebhookEngine("whsec_test_secret", base_backoff_sec=2.0)
    event = WebhookEvent(event_id="EVT-FAIL", payload={"status": "FAILED"}, max_retries=3)

    always_fail = lambda _payload, _sig: False

    assert engine.dispatch(event, always_fail, now=10.0) == "RETRY_SCHEDULED"
    assert engine.dispatch(event, always_fail, now=12.0) == "RETRY_SCHEDULED"
    assert engine.dispatch(event, always_fail, now=16.0) == "DLQ"

    assert event.status == "DLQ"
    assert len(engine.dlq) == 1
    assert engine.dlq[0].event_id == "EVT-FAIL"