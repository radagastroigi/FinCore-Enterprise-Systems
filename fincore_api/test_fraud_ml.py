import numpy as np
from fincore_api.fraud_ml import PayShieldFraudModel


def _make_sample_transactions():
    rng = np.random.default_rng(42)
    # 490 normal swipes: low velocity, normal spend ratio, low travel speed, known device
    normal = np.column_stack([
        rng.uniform(4.0, 7.0, 490),   # amount_log
        rng.integers(1, 3, 490),      # tx_count_1h
        rng.uniform(0.5, 1.5, 490),   # amount_to_user_avg_ratio
        rng.uniform(0, 40, 490),      # geo_speed_kmh
        np.zeros(490),                # is_new_device = 0
    ])
    # 10 fraud swipes (~2%): rapid swipes, 8x-15x spend spike, impossible travel, new device
    fraud = np.column_stack([
        rng.uniform(9.5, 11.5, 10),
        rng.integers(8, 20, 10),
        rng.uniform(8.0, 15.0, 10),
        rng.uniform(600, 1200, 10),
        np.ones(10),
    ])
    X = np.vstack([normal, fraud])
    y = np.array([0] * 490 + [1] * 10)
    return X, y


def test_high_risk_transaction_scores_above_0_7():
    X, y = _make_sample_transactions()
    scorer = PayShieldFraudModel(threshold=0.5)
    metrics = scorer.fit_and_evaluate(X, y, X, y)

    assert metrics["recall"] >= 0.8
    # High-risk swipe: big amount, 15 swipes/hr, 12x normal spend, 900 km/h jump, new device
    risk_score = scorer.predict_risk_score([10.8, 15, 12.0, 900.0, 1])
    assert risk_score > 0.7


def test_shifted_scores_trigger_drift_critical():
    # Yesterday: almost all scores were safe (~0.05)
    yesterday_scores = [0.05] * 100
    # Today: sudden spike into high-risk scores (~0.85)
    today_scores = [0.85] * 100

    result = PayShieldFraudModel.compute_psi(yesterday_scores, today_scores)
    assert result["psi"] >= 0.25
    assert result["status"] == "DRIFT_CRITICAL"