"""Persist sessionizer output and derive features when a session closes."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from .feature_pipeline import generate_features_for_completed_session
from .models import ConsumptionEvent, FeedingFeature, FeedingSession, Telemetry
from .sessionizer import FeedingSession as SessionizedSession
from .sessionizer import (
    INACTIVITY_TIMEOUT_SECONDS,
    TelemetryPoint,
    sessionize,
)


def persist_session_result(
    db: Session,
    device_id: str,
    result: SessionizedSession,
) -> FeedingSession | None:
    """Insert or update one session and atomically add completed features."""
    if not result.consumption_events:
        return None

    row = db.scalar(
        select(FeedingSession).where(
            FeedingSession.device_id == device_id,
            FeedingSession.session_start == result.start_time,
        )
    )
    if row is None:
        row = FeedingSession(
            device_id=device_id,
            session_start=result.start_time,
            session_end=result.end_time,
            starting_weight_g=result.starting_weight_g,
            ending_weight_g=result.ending_weight_g,
            consumed_g=result.consumed_g,
        )
        db.add(row)
        db.flush()
    elif row.features is None:
        row.session_end = result.end_time
        row.starting_weight_g = result.starting_weight_g
        row.ending_weight_g = result.ending_weight_g
        row.consumed_g = result.consumed_g
        db.flush()
        db.execute(
            ConsumptionEvent.__table__.delete().where(
                ConsumptionEvent.session_id == row.id
            )
        )

    if row.features is None:
        db.add_all(
            ConsumptionEvent(
                session_id=row.id,
                start_time=event.start_time,
                end_time=event.end_time,
                start_weight_g=event.start_weight_g,
                end_weight_g=event.end_weight_g,
                consumed_g=event.consumed_g,
                duration_s=event.duration_s,
                velocity_gps=event.velocity_gps,
            )
            for event in result.consumption_events
        )
        db.flush()
        row.session_duration_s = (
            (row.session_end - row.session_start).total_seconds()
            if row.session_end is not None
            else None
        )
        row.eating_duration_s = sum(
            event.duration_s for event in result.consumption_events
        )
        row.consumption_event_count = len(result.consumption_events)
        generate_features_for_completed_session(db, row)

    return row


def process_device_telemetry(db: Session, device_id: str) -> list[FeedingSession]:
    """Sessionize stored telemetry after each committed MQTT batch.

    Closed intervals are persisted with features. A trailing open interval is
    persisted without features and refreshed as further telemetry arrives.
    """
    rows = db.scalars(
        select(Telemetry)
        .where(Telemetry.device_id == device_id)
        .order_by(Telemetry.timestamp)
    ).all()
    points = [
        TelemetryPoint(
            timestamp=row.timestamp,
            weight_g=row.current_weight_g,
            gate_status=row.gate_status,
        )
        for row in rows
    ]
    persisted: list[FeedingSession] = []
    newly_finalized: list[tuple[FeedingSession, FeedingFeature]] = []

    # Split the device history at long gaps between observations. The first
    # point after a gap is also passed to the prior segment as a boundary
    # sentinel; sessionize uses it to confirm inactivity, then truncates it.
    boundaries = [
        index
        for index in range(1, len(points))
        if (points[index].timestamp - points[index - 1].timestamp).total_seconds()
        > INACTIVITY_TIMEOUT_SECONDS
    ]
    segment_start = 0
    segments: list[list[TelemetryPoint]] = []
    for boundary in boundaries:
        segments.append(points[segment_start : boundary + 1])
        segment_start = boundary
    if points:
        segments.append(points[segment_start:])

    for segment in segments:
        result = sessionize(segment)
        if result is None:
            continue
        previous_row = db.scalar(
            select(FeedingSession).where(
                FeedingSession.device_id == device_id,
                FeedingSession.session_start == result.start_time,
            )
        )
        previous_feature_id = (
            db.scalar(
                select(FeedingFeature.id).where(
                    FeedingFeature.session_id == previous_row.id
                )
            )
            if previous_row is not None
            else None
        )

        row = persist_session_result(db, device_id, result)
        if row is not None:
            persisted.append(row)
            if previous_row is None and row.session_end is None:
                print(
                    f"\n🟡 OPEN FEEDING SESSION stored: "
                    f"session={row.id} | device={device_id}"
                )
            feature = db.scalar(
                select(FeedingFeature).where(
                    FeedingFeature.session_id == row.id
                )
            )
            if feature is not None and previous_feature_id is None:
                newly_finalized.append((row, feature))

    db.commit()

    # Classify only at the first successful finalization. Later telemetry
    # batches reprocess stored rows, so this guard avoids duplicate log lines.
    from .prediction import predict_demo

    feature_names = [
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
    for row, feature in newly_finalized:
        vector = {name: getattr(feature, name) for name in feature_names}
        try:
            prediction = predict_demo(vector)
            print(
                "\n==============================================\n"
                " SYNTHETIC DEMO PREDICTION — NOT CLINICALLY VALIDATED\n"
                f" Session: {row.id} | Device: {device_id}\n"
                f" Class: {prediction['predicted_class']}\n"
                "==============================================\n"
            )
        except Exception as error:
            print(
                f"⚠️ Session {row.id} features were saved, but demo "
                f"classification failed: {error}"
            )

    return persisted
