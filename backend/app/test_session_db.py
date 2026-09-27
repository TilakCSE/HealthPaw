from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from database import engine
from models import Device, FeedingSession, ConsumptionEvent

from sessionizer import (
    TelemetryPoint,
    sessionize,
)


# ============================================================
# TEST CONFIGURATION
# ============================================================

DEVICE_ID = "cts_01"

START = datetime(2026, 9, 27, 15, 0, 0)


# ============================================================
# CONTROLLED TELEMETRY
# ============================================================

def make_point(seconds, weight):

    return TelemetryPoint(
        timestamp=START + timedelta(seconds=seconds),
        weight_g=weight,
        gate_status="CLOSED",
    )


weights = [

    # --------------------------------------------------------
    # EVENT 1
    # --------------------------------------------------------

    (0.0, 200.00),
    (0.5, 199.70),
    (1.0, 198.80),
    (1.5, 197.90),
    (2.0, 197.00),
    (2.5, 196.50),

    # Pause
    (3.0, 196.55),
    (3.5, 196.45),
    (4.0, 196.50),
    (4.5, 196.48),

    # --------------------------------------------------------
    # PAUSE
    # --------------------------------------------------------

    (5.0, 196.50),
    (6.0, 196.49),
    (7.0, 196.51),
    (8.0, 196.50),
    (9.0, 196.49),

    # --------------------------------------------------------
    # EVENT 2
    # --------------------------------------------------------

    (9.5, 195.80),
    (10.0, 194.70),
    (10.5, 193.60),
    (11.0, 192.50),
    (11.5, 191.20),
    (12.0, 190.00),

    # Pause
    (12.5, 190.05),
    (13.0, 189.95),
    (13.5, 190.02),
    (14.0, 190.00),

    # --------------------------------------------------------
    # LONGER PAUSE
    # --------------------------------------------------------

    (20.0, 190.01),
    (30.0, 190.00),
    (40.0, 190.02),

    # --------------------------------------------------------
    # EVENT 3
    # --------------------------------------------------------

    (40.5, 189.20),
    (41.0, 188.20),
    (41.5, 187.30),
    (42.0, 186.20),
    (42.5, 185.00),

    # Pause
    (43.0, 185.05),
    (43.5, 184.95),
    (44.0, 185.02),
    (45.0, 185.00),

    # --------------------------------------------------------
    # 10+ MINUTES INACTIVE
    # --------------------------------------------------------

    (100.0, 185.01),
    (200.0, 185.00),
    (300.0, 184.99),
    (400.0, 185.01),
    (500.0, 185.00),
    (600.0, 185.01),
    (645.0, 185.00),
]


points = [
    make_point(seconds, weight)
    for seconds, weight in weights
]


# ============================================================
# SESSIONIZE
# ============================================================

print("Running sessionizer...")

session_result = sessionize(points)

if session_result is None:
    raise RuntimeError("❌ Sessionizer returned no session.")

print("✅ Session detected")

print(
    f"   Start: {session_result.start_time}"
)

print(
    f"   End:   {session_result.end_time}"
)

print(
    f"   Consumed: "
    f"{session_result.consumed_g:.2f} g"
)

print(
    f"   Events: "
    f"{len(session_result.consumption_events)}"
)


# ============================================================
# FIND DEVICE
# ============================================================

with Session(engine) as db:

    device = db.query(Device).filter(
        Device.device_id == DEVICE_ID
    ).first()

    if device is None:
        raise RuntimeError(
            f"❌ Device '{DEVICE_ID}' does not exist."
        )

    print(
        f"✅ Found device: {device.device_id}"
    )


    # ========================================================
    # CALCULATE SESSION VALUES
    # ========================================================

    session_duration_s = None

    if session_result.end_time is not None:

        session_duration_s = (
            session_result.end_time
            - session_result.start_time
        ).total_seconds()


    eating_duration_s = sum(
        event.duration_s
        for event in session_result.consumption_events
    )


    # ========================================================
    # CREATE FEEDING SESSION
    # ========================================================

    db_session = FeedingSession(
        device_id=DEVICE_ID,

        session_start=session_result.start_time,

        session_end=session_result.end_time,

        starting_weight_g=(
            session_result.starting_weight_g
        ),

        ending_weight_g=(
            session_result.ending_weight_g
        ),

        consumed_g=(
            session_result.consumed_g
        ),

        session_duration_s=session_duration_s,

        eating_duration_s=eating_duration_s,

        consumption_event_count=len(
            session_result.consumption_events
        ),
    )


    db.add(db_session)

    # Flush so PostgreSQL generates the session ID
    db.flush()


    # ========================================================
    # CREATE CONSUMPTION EVENTS
    # ========================================================

    for event in session_result.consumption_events:

        db_event = ConsumptionEvent(

            session_id=db_session.id,

            start_time=event.start_time,

            end_time=event.end_time,

            start_weight_g=(
                event.start_weight_g
            ),

            end_weight_g=(
                event.end_weight_g
            ),

            consumed_g=(
                event.consumed_g
            ),

            duration_s=(
                event.duration_s
            ),

            velocity_gps=(
                event.velocity_gps
            ),
        )

        db.add(db_event)


    # ========================================================
    # COMMIT
    # ========================================================

    db.commit()

    print()
    print("==========================================")
    print("       DATABASE INSERT SUCCESS")
    print("==========================================")

    print(
        f"Feeding Session ID: {db_session.id}"
    )

    print(
        f"Events inserted: "
        f"{len(session_result.consumption_events)}"
    )

    print(
        f"Total consumed: "
        f"{session_result.consumed_g:.2f} g"
    )

    print(
        f"Eating duration: "
        f"{eating_duration_s:.2f} s"
    )

    print("==========================================")