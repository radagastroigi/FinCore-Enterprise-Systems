import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import confusion_matrix, precision_score, recall_score


class PayShieldFraudModel:
    def __init__(self, threshold: float = 0.5):
        # If fraud probability >= threshold (e.g., 50%), block/flag the transaction
        self.threshold = threshold
        # A tree-based model that learns "if-else" risk rules from past transactions
        self.model = GradientBoostingClassifier(
            n_estimators=60, max_depth=3, random_state=42
        )

    def fit_and_evaluate(self, X_train, y_train, X_val, y_val) -> dict:
        # Step 1: Teach the model using past labeled transactions
        self.model.fit(X_train, y_train)

        # Step 2: Ask the model for fraud probabilities (0.0 to 1.0) on new data
        # [:, 1] grabs the probability of class "1" (Fraud)
        probs = self.model.predict_proba(X_val)[:, 1]

        # Step 3: Turn probabilities into True/False (1 or 0) using our threshold
        preds = (probs >= self.threshold).astype(int)

        # Step 4: Count True Negatives (tn) and False Positives (fp)
        tn, fp, fn, tp = confusion_matrix(y_val, preds, labels=[0, 1]).ravel()
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

        return {
            "precision": round(float(precision_score(y_val, preds, zero_division=0)), 4),
            "recall": round(float(recall_score(y_val, preds, zero_division=0)), 4),
            "fpr": round(float(fpr), 4),
        }

    def predict_risk_score(self, features: list[float]) -> float:
        # Score a single live swipe: [amount_log, tx_count_1h, ratio, speed, is_new_device]
        prob = self.model.predict_proba([features])[0][1]
        return round(float(prob), 4)

    @staticmethod
    def compute_psi(
        expected_scores: list[float], actual_scores: list[float], bins: int = 10
    ) -> dict:
        # Step 1: Split scores from 0.0 to 1.0 into 10 equal buckets (0-0.1, 0.1-0.2, ...)
        exp_counts, _ = np.histogram(expected_scores, bins=bins, range=(0.0, 1.0))
        act_counts, _ = np.histogram(actual_scores, bins=bins, range=(0.0, 1.0))

        # Step 2: Turn raw counts into percentages (clamp at 1e-6 so we never divide by zero)
        exp_pct = np.clip(exp_counts / len(expected_scores), 1e-6, None)
        act_pct = np.clip(act_counts / len(actual_scores), 1e-6, None)

        # Step 3: Compare each bucket's percentage change
        psi = float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)))

        # Step 4: Industry-standard alarm thresholds
        if psi >= 0.25:
            status = "DRIFT_CRITICAL"
        elif psi >= 0.1:
            status = "DRIFT_WARNING"
        else:
            status = "OK"

        return {"psi": round(psi, 4), "status": status}