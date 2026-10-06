"""Prediction helpers for the clearly labeled synthetic demo model."""

import json
from functools import lru_cache
from pathlib import Path

import joblib
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_DIR = PROJECT_ROOT / "data" / "output" / "v4.3_synthetic_baseline"
MODEL_PATH = ARTIFACT_DIR / "synthetic_demo_model.joblib"
EXAMPLES_PATH = ARTIFACT_DIR / "prediction_examples.json"
MODEL_NAME = "healthpaw-v4.3-synthetic-random-forest"
MODEL_FEATURES = [
    "consumed_g",
    "session_duration_s",
    "eating_duration_s",
    "avg_velocity_gps",
    "max_velocity_gps",
    "consumption_event_count",
    "time_to_first_consumption_s",
    "pause_count",
    "avg_pause_duration_s",
    "max_pause_duration_s",
    "active_eating_ratio",
    "feeding_interval_s",
    "daily_intake_g",
]


@lru_cache(maxsize=1)
def load_demo_model():
    """Load the local demo artifact once; never download or train at runtime."""
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Synthetic demo model was not found at {MODEL_PATH}. "
            "Run data/scripts/run_v4_3_feature_selection_and_training.py first."
        )
    return joblib.load(MODEL_PATH)


def predict_demo(features: dict[str, float | int | None]) -> dict:
    artifact = load_demo_model()
    model = artifact["estimator"]
    selected_features = artifact["features"]
    row = pd.DataFrame(
        [{name: features.get(name) for name in selected_features}],
        columns=selected_features,
    )
    prediction = str(model.predict(row)[0])
    probabilities = model.predict_proba(row)[0]
    return {
        "predicted_class": prediction,
        "class_scores": {
            str(label): float(probability)
            for label, probability in zip(model.classes_, probabilities)
        },
        "model": artifact.get("model_name", MODEL_NAME),
        "scope": "Synthetic v4.3 demonstration only; not validated on real dogs.",
        "score_note": "Random Forest vote fractions; not calibrated probabilities.",
    }


def load_demo_examples() -> dict:
    if not EXAMPLES_PATH.exists():
        raise FileNotFoundError(
            f"Demo inputs were not found at {EXAMPLES_PATH}. "
            "Run data/scripts/run_v4_3_feature_selection_and_training.py first."
        )
    with EXAMPLES_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)
