"""Accelerated, clearly synthetic MQTT telemetry publishers for Phase 5."""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path



PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_PATH = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "v4.3_synthetic_baseline"
    / "prediction_examples.json"
)
DEVICE_ID = "phase5_demo_v2"
MQTT_TOPIC = f"device/{DEVICE_ID}/telemetry"
MQTT_HOST = "broker.hivemq.com"
MQTT_PORT = 1883
SAMPLE_PERIOD_S = 0.5
INACTIVITY_TIMEOUT_S = 150

sys.path.insert(0, str(PROJECT_ROOT))


def read_example(class_name: str) -> dict:
    payload = json.loads(EXAMPLES_PATH.read_text(encoding="utf-8"))
    for item in payload["examples"]:
        if item["scenario_label"] == class_name:
            return item
    raise KeyError(f"No saved example for {class_name!r}")


def build_profile(item: dict) -> list[tuple[float, float]]:
    """Build a weight-vs-time trace whose sampled fields follow the API example.

    A 0.60 g alternating transient is added to the slow eating slopes so that
    the existing 0.75 g per-sample event trigger sees measurable activity.
    The No Consumption fixture uses a single 0.75 g threshold-sized change;
    it is therefore a near-zero, threshold-edge demo, not literal zero intake.
    """
    x = item["request_body"]
    consumed = float(x["consumed_g"])
    target_duration = float(x["session_duration_s"])
    event_count = int(x["consumption_event_count"])
    eating_duration = float(x["eating_duration_s"])
    first_s = float(x["time_to_first_consumption_s"])
    if event_count == 0:
        event_count = 1
        consumed = 0.75
        eating_duration = 0.5

    profile: list[tuple[float, float]] = [(0.0, 300.0)]
    current_weight = 300.0

    # Use pauses between observed events. The synthetic source sometimes has
    # internally inconsistent event/pause counts, so create event_count-1
    # observable gaps and preserve the supplied maximum where possible.
    gap_count = max(0, event_count - 1)
    gaps: list[float] = []
    if gap_count:
        avg_pause = float(x.get("avg_pause_duration_s") or 0.0)
        max_pause = float(x.get("max_pause_duration_s") or avg_pause)
        if gap_count == 1:
            gaps = [max(avg_pause, 2.0)]
        else:
            ordinary = max(2.0, (gap_count * avg_pause - max_pause) / (gap_count - 1))
            gaps = [ordinary] * (gap_count - 1) + [max(ordinary, max_pause)]

    active_durations = [eating_duration / event_count] * event_count
    active_consumptions = [consumed / event_count] * event_count
    elapsed = 0.0
    while elapsed + SAMPLE_PERIOD_S < first_s:
        elapsed += SAMPLE_PERIOD_S
        profile.append((elapsed, current_weight))
    elapsed = first_s
    profile.append((elapsed, current_weight))
    for event_index, (duration, event_consumed) in enumerate(
        zip(active_durations, active_consumptions)
    ):
        steps = max(1, int(round(duration / SAMPLE_PERIOD_S)))
        per_step = event_consumed / steps
        for step in range(steps):
            if steps == 1:
                delta = event_consumed
            else:
                # Paired transients cancel over each one-second interval.
                if steps % 2 == 1 and step == steps - 1:
                    transient = 0.0
                else:
                    transient = 0.60 if step % 2 == 0 else -0.60
                delta = per_step + transient
            current_weight -= delta
            elapsed += SAMPLE_PERIOD_S
            profile.append((elapsed, current_weight))

        # Four quiet samples allow the current 1.5 s stability rule to close
        # the event before the next simulated pause.
        for _ in range(4):
            elapsed += SAMPLE_PERIOD_S
            profile.append((elapsed, current_weight))

        if event_index < len(gaps):
            gap = gaps[event_index]
            gap_end = elapsed - 2.0 + gap
            while elapsed + SAMPLE_PERIOD_S < gap_end:
                elapsed += SAMPLE_PERIOD_S
                profile.append((elapsed, current_weight))
            elapsed = max(elapsed, gap_end)
            profile.append((elapsed, current_weight))

    # Add the timeout confirmation as a timestamp jump. Messages remain
    # accelerated in wall-clock time; the 60 s demo gap is simulated time.
    last_consumption_time = max(
        (t for t, weight in profile if weight < 300.0),
        default=elapsed,
    )
    close_time = last_consumption_time + INACTIVITY_TIMEOUT_S
    if close_time <= elapsed:
        close_time = elapsed + INACTIVITY_TIMEOUT_S
    profile.append((close_time, current_weight))

    # Keep a small amount of trailing time from the example where possible.
    if target_duration > elapsed and target_duration < close_time:
        profile.insert(-1, (target_duration, current_weight))

    # Remove duplicate timestamps while preserving the last weight at each.
    deduplicated: dict[float, float] = {}
    for timestamp, weight in profile:
        deduplicated[timestamp] = weight
    return sorted(deduplicated.items())


def derive_and_predict(profile: list[tuple[float, float]], start: datetime) -> tuple[dict, str]:
    from backend.app.feature_extractor import extract_features
    from backend.app.prediction import predict_demo
    from backend.app.sessionizer import TelemetryPoint, sessionize

    points = [
        TelemetryPoint(start + timedelta(seconds=t), w, "CLOSED")
        for t, w in profile
    ]
    session = sessionize(points)
    if session is None or not session.consumption_events:
        raise RuntimeError("The generated trace did not produce a detectable event")
    # A simulated timeout message is the final observation. Match the feature
    # row that the backend creates from that completed session.
    features = extract_features(session)
    vector = vars(features)
    prediction = predict_demo(vector)["predicted_class"]
    return vector, prediction


def ensure_demo_device() -> None:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / "backend" / ".env")
    from backend.app.database import engine
    from backend.app.models import Device
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    with Session(engine) as db:
        device = db.scalar(select(Device).where(Device.device_id == DEVICE_ID))
        if device is None:
            db.add(Device(device_id=DEVICE_ID, status="synthetic-demo"))
            db.commit()
            print(f"Registered isolated demo device {DEVICE_ID} in Neon")


def next_demo_start() -> datetime:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / "backend" / ".env")
    from backend.app.database import engine
    from backend.app.models import Telemetry
    from sqlalchemy import func, select
    from sqlalchemy.orm import Session

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with Session(engine) as db:
        latest = db.scalar(
            select(func.max(Telemetry.timestamp)).where(
                Telemetry.device_id == DEVICE_ID
            )
        )
    if latest is None:
        return now
    return max(
        now,
        latest + timedelta(seconds=INACTIVITY_TIMEOUT_S + 2.0),
    )


def wait_for_persisted_prediction(start: datetime, expected: str) -> str:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / "backend" / ".env")
    from backend.app.database import engine
    from backend.app.models import FeedingFeature, FeedingSession
    from backend.app.prediction import predict_demo
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        with Session(engine) as db:
            session_row = db.scalar(
                select(FeedingSession)
                .where(
                    FeedingSession.device_id == DEVICE_ID,
                    FeedingSession.session_start >= start - timedelta(seconds=1),
                )
                .order_by(FeedingSession.session_start.desc())
                .limit(1)
            )
            if session_row is not None:
                feature = db.scalar(
                    select(FeedingFeature).where(
                        FeedingFeature.session_id == session_row.id
                    )
                )
                if feature is not None:
                    names = [
                        "consumed_g", "session_duration_s", "eating_duration_s",
                        "avg_velocity_gps", "max_velocity_gps",
                        "consumption_event_count", "time_to_first_consumption_s",
                        "pause_count", "avg_pause_duration_s",
                        "max_pause_duration_s", "active_eating_ratio",
                        "feeding_interval_s", "daily_intake_g",
                    ]
                    result = predict_demo({name: getattr(feature, name) for name in names})
                    actual = result["predicted_class"]
                    print(
                        f"Neon verified: session={session_row.id}, "
                        f"events={session_row.consumption_event_count}, "
                        f"feature_row={feature.id}, prediction={actual}"
                    )
                    if actual != expected:
                        raise RuntimeError(
                            f"Neon feature prediction was {actual!r}; expected {expected!r}"
                        )
                    return actual
        time.sleep(1)
    raise TimeoutError("Timed out waiting for a completed feature row in Neon")


def publish(class_name: str, delay: float = 0.01, offline: bool = False) -> None:
    import paho.mqtt.client as mqtt

    item = read_example(class_name)
    profile = build_profile(item)
    start = datetime.now(timezone.utc).replace(tzinfo=None)
    print(f"Scenario: {class_name}")
    print("Synthetic accelerated telemetry; 2 Hz simulated timestamps")
    print(f"Samples: {len(profile)} | Expected class: {class_name}")
    if offline:
        feature_vector, local_prediction = derive_and_predict(profile, start)
        print(f"Local trace check: {local_prediction}")
        if local_prediction != class_name:
            raise RuntimeError(
                f"Local trace predicted {local_prediction!r}; expected {class_name!r}"
            )
        print("Offline check only; nothing was published or written to Neon.")
        print("Derived features:", json.dumps(feature_vector, default=str))
        return

    ensure_demo_device()
    start = next_demo_start()
    print(f"Run start (UTC simulated clock): {start.isoformat()}", flush=True)
    feature_vector, local_prediction = derive_and_predict(profile, start)
    print(f"Local trace check: {local_prediction}")
    if local_prediction != class_name:
        raise RuntimeError(
            f"Local trace predicted {local_prediction!r}; expected {class_name!r}"
        )
    connected = False

    def on_connect(client, userdata, flags, reason_code, properties):
        nonlocal connected
        connected = reason_code == 0
        print(f"Publisher MQTT connect result: {reason_code}", flush=True)

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"hp-phase5-{uuid.uuid4().hex[:10]}",
    )
    client.on_connect = on_connect
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    try:
        for _ in range(300):
            if connected:
                break
            time.sleep(0.05)
        if not connected:
            raise RuntimeError("Could not connect to the public MQTT broker")

        last_report = -25
        for index, (offset, weight) in enumerate(profile):
            timestamp_ms = int(
                (start + timedelta(seconds=offset))
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
            message = {
                "device_id": DEVICE_ID,
                "timestamp": timestamp_ms,
                "current_weight_g": round(weight, 4),
                "gate_status": "CLOSED",
            }
            result = client.publish(MQTT_TOPIC, json.dumps(message), qos=0)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed with code {result.rc}")
            if index - last_report >= 25 or index == len(profile) - 1:
                consumed_so_far = 300.0 - weight
                print(
                    f"  [{index + 1:4}/{len(profile)}] "
                    f"t={offset:7.1f}s weight={weight:8.2f}g "
                    f"change={consumed_so_far:7.2f}g"
                )
                last_report = index
            if delay > 0:
                time.sleep(delay)
        time.sleep(1.0)
        print(f"Published {len(profile)} samples to {MQTT_TOPIC}")
        print("Waiting for Neon persistence and the backend's automatic prediction…")
        wait_for_persisted_prediction(start, class_name)
    finally:
        client.loop_stop()
        client.disconnect()


def run_cli(class_name: str) -> None:
    parser = argparse.ArgumentParser(description=f"Publish {class_name} synthetic telemetry")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.01,
        help="Wall-clock seconds between samples; timestamps remain simulated at 2 Hz",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Validate the generated trace and model locally without MQTT or Neon writes",
    )
    args = parser.parse_args()
    publish(class_name, delay=args.delay, offline=args.offline)
