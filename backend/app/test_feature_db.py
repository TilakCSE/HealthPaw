from sqlalchemy import select
from sqlalchemy.orm import Session

from database import engine
from models import FeedingSession, ConsumptionEvent, FeedingFeature

from sessionizer import (
    FeedingSession as SessionizedFeedingSession,
    ConsumptionEvent as SessionizedConsumptionEvent,
)

from feature_extractor import extract_features


SESSION_ID = 1


def main():
    print("\n==========================================")
    print("   FEATURE EXTRACTION → DATABASE")
    print("==========================================")

    with Session(engine) as db:

        # --------------------------------------------------
        # 1. Load completed feeding session
        # --------------------------------------------------

        session_row = db.execute(
            select(FeedingSession)
            .where(FeedingSession.id == SESSION_ID)
        ).scalar_one_or_none()

        if session_row is None:
            print(f"\n❌ Session {SESSION_ID} not found.")
            return

        print(f"\nFeeding Session ID: {session_row.id}")
        print(f"Start:              {session_row.session_start}")
        print(f"End:                {session_row.session_end}")

        if session_row.session_end is None:
            print("❌ Session is not completed.")
            return

        # --------------------------------------------------
        # 2. Load consumption events
        # --------------------------------------------------

        event_rows = db.execute(
            select(ConsumptionEvent)
            .where(
                ConsumptionEvent.session_id == session_row.id
            )
            .order_by(ConsumptionEvent.start_time)
        ).scalars().all()

        print(f"Consumption events: {len(event_rows)}")

        if not event_rows:
            print("❌ No consumption events found.")
            return

        # --------------------------------------------------
        # 3. Convert DB records → feature extractor objects
        # --------------------------------------------------

        events = [
            SessionizedConsumptionEvent(
                start_time=event.start_time,
                end_time=event.end_time,
                start_weight_g=event.start_weight_g,
                end_weight_g=event.end_weight_g,
                consumed_g=event.consumed_g,
                duration_s=event.duration_s,
                velocity_gps=event.velocity_gps,
            )
            for event in event_rows
        ]

        session = SessionizedFeedingSession(
            start_time=session_row.session_start,
            end_time=session_row.session_end,
            starting_weight_g=session_row.starting_weight_g,
            ending_weight_g=session_row.ending_weight_g,
            consumed_g=session_row.consumed_g,
            consumption_events=events,
        )

        # --------------------------------------------------
        # 4. Extract features
        # --------------------------------------------------

        features = extract_features(
            session=session,
            previous_session_end=None,
            daily_intake_g=None,
        )

        print("\nFeature vector generated.")

        # --------------------------------------------------
        # 5. Prevent duplicate feature row
        # --------------------------------------------------

        existing = db.execute(
            select(FeedingFeature)
            .where(
                FeedingFeature.session_id == SESSION_ID
            )
        ).scalar_one_or_none()

        if existing is not None:
            print(
                f"\n⚠️ Feature record already exists: "
                f"{existing.id}"
            )
            print("No new record inserted.")
            return

        # --------------------------------------------------
        # 6. Create database feature record
        # --------------------------------------------------

        feature_row = FeedingFeature(
            session_id=SESSION_ID,

            consumed_g=features.consumed_g,
            session_duration_s=features.session_duration_s,
            eating_duration_s=features.eating_duration_s,

            avg_velocity_gps=features.avg_velocity_gps,
            max_velocity_gps=features.max_velocity_gps,

            consumption_event_count=(
                features.consumption_event_count
            ),

            time_to_first_consumption_s=(
                features.time_to_first_consumption_s
            ),

            pause_count=features.pause_count,

            avg_pause_duration_s=(
                features.avg_pause_duration_s
            ),

            max_pause_duration_s=(
                features.max_pause_duration_s
            ),

            active_eating_ratio=(
                features.active_eating_ratio
            ),

            feeding_interval_s=(
                features.feeding_interval_s
            ),

            daily_intake_g=(
                features.daily_intake_g
            ),
        )

        db.add(feature_row)
        db.commit()

        # --------------------------------------------------
        # 7. Verify
        # --------------------------------------------------

        db.refresh(feature_row)

        print("\n==========================================")
        print("       FEATURE DATABASE SUCCESS")
        print("==========================================")

        print(f"Feature ID:             {feature_row.id}")
        print(f"Session ID:             {feature_row.session_id}")
        print(f"Consumed:               {feature_row.consumed_g:.2f} g")
        print(
            f"Session duration:       "
            f"{feature_row.session_duration_s:.2f} s"
        )
        print(
            f"Eating duration:        "
            f"{feature_row.eating_duration_s:.2f} s"
        )
        print(
            f"Average velocity:       "
            f"{feature_row.avg_velocity_gps:.4f} g/s"
        )
        print(
            f"Maximum velocity:       "
            f"{feature_row.max_velocity_gps:.4f} g/s"
        )
        print(
            f"Consumption events:     "
            f"{feature_row.consumption_event_count}"
        )
        print(
            f"Time to first eating:   "
            f"{feature_row.time_to_first_consumption_s:.3f} s"
        )
        print(
            f"Pause count:            "
            f"{feature_row.pause_count}"
        )
        print(
            f"Average pause:          "
            f"{feature_row.avg_pause_duration_s:.3f} s"
        )
        print(
            f"Maximum pause:          "
            f"{feature_row.max_pause_duration_s:.3f} s"
        )
        print(
            f"Active eating ratio:    "
            f"{feature_row.active_eating_ratio:.4f}"
        )

        print("\n✅ Feature vector persisted.")
        print("==========================================")


if __name__ == "__main__":
    main()