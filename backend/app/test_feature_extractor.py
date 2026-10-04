from sqlalchemy import select
from sqlalchemy.orm import Session

from database import engine
from models import FeedingSession, ConsumptionEvent
from sessionizer import (
    FeedingSession as SessionizedFeedingSession,
    ConsumptionEvent as SessionizedConsumptionEvent,
)
from feature_extractor import extract_features, print_features


SESSION_ID = 2


def main():
    print("\n==========================================")
    print("   DATABASE SESSION → FEATURE EXTRACTION")
    print("==========================================")

    with Session(engine) as db:

        # --------------------------------------------------
        # 1. Load feeding session
        # --------------------------------------------------

        session_row = db.execute(
            select(FeedingSession)
            .where(FeedingSession.id == SESSION_ID)
        ).scalar_one_or_none()

        if session_row is None:
            print(f"\n❌ Feeding session {SESSION_ID} not found.")
            return

        print(f"\nFeeding Session ID: {session_row.id}")
        print(f"Start:              {session_row.session_start}")
        print(f"End:                {session_row.session_end}")
        print(f"Starting weight:    {session_row.starting_weight_g:.2f} g")
        print(f"Ending weight:      {session_row.ending_weight_g:.2f} g")
        print(f"Consumed:           {session_row.consumed_g:.2f} g")

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
            print("\n❌ No consumption events found.")
            return

        # --------------------------------------------------
        # 3. Convert DB objects → sessionizer dataclasses
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

        # --------------------------------------------------
        # 5. Display feature vector
        # --------------------------------------------------

        print_features(features)

        # --------------------------------------------------
        # 6. Validation checks
        # --------------------------------------------------

        print("\n==========================================")
        print("          FEATURE VALIDATION")
        print("==========================================")

        checks_passed = 0
        checks_total = 0

        def check(name, condition):
            nonlocal checks_passed, checks_total

            checks_total += 1

            if condition:
                print(f"✅ {name}")
                checks_passed += 1
            else:
                print(f"❌ {name}")

        check(
            "Consumed matches session",
            abs(
                features.consumed_g
                - session_row.consumed_g
            ) < 0.001,
        )

        check(
            "Eating duration is positive",
            features.eating_duration_s > 0,
        )

        check(
            "Average velocity is positive",
            features.avg_velocity_gps is not None
            and features.avg_velocity_gps > 0,
        )

        check(
            "Maximum velocity is positive",
            features.max_velocity_gps is not None
            and features.max_velocity_gps > 0,
        )

        check(
            "Event count matches database",
            features.consumption_event_count
            == len(event_rows),
        )

        check(
            "First consumption time exists",
            features.time_to_first_consumption_s is not None,
        )

        check(
            "Pause count is correct",
            features.pause_count
            == max(0, len(event_rows) - 1),
        )

        # Session #2 is still open, so these should
        # intentionally be None.
        check(
            "Open session has no session duration",
            features.session_duration_s is None,
        )

        check(
            "Open session has no active eating ratio",
            features.active_eating_ratio is None,
        )

        print("\n==========================================")
        print(
            f"Validation: "
            f"{checks_passed}/{checks_total} checks passed"
        )
        print("==========================================")

        if checks_passed == checks_total:
            print("✅ FEATURE EXTRACTION TEST PASSED")
        else:
            print("❌ FEATURE EXTRACTION TEST FAILED")

        print("\n❗ No database records were modified.")


if __name__ == "__main__":
    main()