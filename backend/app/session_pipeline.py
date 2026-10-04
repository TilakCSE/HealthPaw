"""Persist sessionizer output and derive features when a session closes."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from .feature_pipeline import generate_features_for_completed_session
from .models import ConsumptionEvent, FeedingSession, Telemetry
from .sessionizer import FeedingSession as SessionizedSession
from .sessionizer import TelemetryPoint, sessionize


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

    while points:
        result = sessionize(points)
        if result is None:
            break

        row = persist_session_result(db, device_id, result)
        if row is not None:
            persisted.append(row)

        if result.end_time is None:
            break

        remaining = [point for point in points if point.timestamp > result.end_time]
        if len(remaining) == len(points):
            break
        points = remaining

    db.commit()
    return persisted
