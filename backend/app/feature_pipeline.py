"""Persist derived features when a feeding session is finalized."""

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .feature_extractor import extract_features
from .models import ConsumptionEvent, FeedingFeature, FeedingSession
from .sessionizer import (
    ConsumptionEvent as SessionizedConsumptionEvent,
    FeedingSession as SessionizedFeedingSession,
)


def generate_features_for_completed_session(
    db: Session,
    session_row: FeedingSession,
) -> FeedingFeature | None:
    """Create one feature row for a completed session, idempotently.

    Returns None for an open session. The caller owns the transaction, so the
    session, events, and features can be committed atomically.
    """
    if session_row.session_end is None:
        return None

    existing = db.scalar(
        select(FeedingFeature).where(
            FeedingFeature.session_id == session_row.id
        )
    )
    if existing is not None:
        return existing

    event_rows = db.scalars(
        select(ConsumptionEvent)
        .where(ConsumptionEvent.session_id == session_row.id)
        .order_by(ConsumptionEvent.start_time)
    ).all()
    events = [
        SessionizedConsumptionEvent(
            start_time=row.start_time,
            end_time=row.end_time,
            start_weight_g=row.start_weight_g,
            end_weight_g=row.end_weight_g,
            consumed_g=row.consumed_g,
            duration_s=row.duration_s,
            velocity_gps=row.velocity_gps,
        )
        for row in event_rows
    ]
    session = SessionizedFeedingSession(
        start_time=session_row.session_start,
        end_time=session_row.session_end,
        starting_weight_g=session_row.starting_weight_g,
        ending_weight_g=session_row.ending_weight_g,
        consumed_g=session_row.consumed_g,
        consumption_events=events,
    )

    previous = db.scalar(
        select(FeedingSession)
        .where(
            FeedingSession.device_id == session_row.device_id,
            FeedingSession.session_end.is_not(None),
            FeedingSession.session_end < session_row.session_start,
            FeedingSession.id != session_row.id,
        )
        .order_by(FeedingSession.session_end.desc())
        .limit(1)
    )
    features = extract_features(
        session=session,
        previous_session_end=(previous.session_end if previous else None),
    )
    daily_start = session_row.session_start.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    daily_intake = db.scalar(
        select(func.coalesce(func.sum(FeedingSession.consumed_g), 0.0)).where(
            FeedingSession.device_id == session_row.device_id,
            FeedingSession.session_end.is_not(None),
            FeedingSession.session_start >= daily_start,
            FeedingSession.session_start < session_row.session_start,
        )
    )

    row = FeedingFeature(
        session_id=session_row.id,
        consumed_g=features.consumed_g,
        session_duration_s=features.session_duration_s,
        eating_duration_s=features.eating_duration_s,
        avg_velocity_gps=features.avg_velocity_gps,
        max_velocity_gps=features.max_velocity_gps,
        consumption_event_count=features.consumption_event_count,
        time_to_first_consumption_s=features.time_to_first_consumption_s,
        pause_count=features.pause_count,
        avg_pause_duration_s=features.avg_pause_duration_s,
        max_pause_duration_s=features.max_pause_duration_s,
        active_eating_ratio=features.active_eating_ratio,
        feeding_interval_s=features.feeding_interval_s,
        daily_intake_g=daily_intake + session_row.consumed_g,
    )
    db.add(row)
    db.flush()
    return row
