from sqlalchemy import select
from sqlalchemy.orm import Session

from database import engine
from models import Device, FeedingSession, ConsumptionEvent, Telemetry
from sessionizer import TelemetryPoint, sessionize


DEVICE_ID = "cts_01"


def main():
    print("\n==========================================")
    print("   REAL TELEMETRY → SESSION DATABASE")
    print("==========================================")

    with Session(engine) as db:

        # --------------------------------------------------
        # 1. Load raw telemetry
        # --------------------------------------------------

        stmt = (
            select(Telemetry)
            .where(Telemetry.device_id == DEVICE_ID)
            .order_by(Telemetry.timestamp)
        )

        rows = db.execute(stmt).scalars().all()

        print(f"\nTelemetry rows loaded: {len(rows)}")

        if not rows:
            print("❌ No telemetry found.")
            return

        # --------------------------------------------------
        # 2. Convert DB rows → TelemetryPoint
        # --------------------------------------------------

        points = [
            TelemetryPoint(
                timestamp=row.timestamp,
                weight_g=row.current_weight_g,
                gate_status=row.gate_status,
            )
            for row in rows
        ]

        print(f"TelemetryPoint objects created: {len(points)}")

        # --------------------------------------------------
        # 3. Run sessionizer
        # --------------------------------------------------

        session = sessionize(points)

        if session is None:
            print("\n❌ No feeding session detected.")
            return

        print("\nSession detected.")
        print(f"  Start:   {session.start_time}")
        print(f"  End:     {session.end_time}")
        print(f"  Consumed: {session.consumed_g:.2f} g")
        print(f"  Events:   {len(session.consumption_events)}")

        # --------------------------------------------------
        # 4. Find device
        # --------------------------------------------------

        device = db.execute(
            select(Device).where(Device.device_id == DEVICE_ID)
        ).scalar_one_or_none()

        if device is None:
            print(f"\n❌ Device not found: {DEVICE_ID}")
            return

        print(f"\n✅ Device found: {DEVICE_ID}")

        # --------------------------------------------------
        # 5. Calculate session durations
        # --------------------------------------------------

        session_duration_s = None

        if session.end_time is not None:
            session_duration_s = (
                session.end_time - session.start_time
            ).total_seconds()

        eating_duration_s = sum(
            event.duration_s
            for event in session.consumption_events
        )

        # --------------------------------------------------
        # 6. Create FeedingSession
        # --------------------------------------------------

        db_session = FeedingSession(
            device_id=DEVICE_ID,
            session_start=session.start_time,
            session_end=session.end_time,
            starting_weight_g=session.starting_weight_g,
            ending_weight_g=session.ending_weight_g,
            consumed_g=session.consumed_g,
            session_duration_s=session_duration_s,
            eating_duration_s=eating_duration_s,
            consumption_event_count=len(
                session.consumption_events
            ),
        )

        db.add(db_session)
        db.flush()

        # --------------------------------------------------
        # 7. Create ConsumptionEvents
        # --------------------------------------------------

        for event in session.consumption_events:

            db_event = ConsumptionEvent(
                session_id=db_session.id,
                start_time=event.start_time,
                end_time=event.end_time,
                start_weight_g=event.start_weight_g,
                end_weight_g=event.end_weight_g,
                consumed_g=event.consumed_g,
                duration_s=event.duration_s,
                velocity_gps=event.velocity_gps,
            )

            db.add(db_event)

        # --------------------------------------------------
        # 8. Commit
        # --------------------------------------------------

        db.commit()

        print("\n==========================================")
        print("       DATABASE INSERT SUCCESS")
        print("==========================================")

        print(f"Feeding Session ID: {db_session.id}")
        print(f"Events inserted:    {len(session.consumption_events)}")
        print(f"Total consumed:     {session.consumed_g:.2f} g")
        print(f"Eating duration:    {eating_duration_s:.2f} s")
        print(f"Session end:        {session.end_time}")

        print("==========================================")
        print("✅ Real telemetry persisted successfully.")
        print("==========================================")


if __name__ == "__main__":
    main()