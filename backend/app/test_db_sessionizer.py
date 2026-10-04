from sqlalchemy import select
from sqlalchemy.orm import Session

from database import engine
from models import Telemetry
from sessionizer import TelemetryPoint, sessionize, print_session


DEVICE_ID = "cts_01"


def main():
    print("\n==========================================")
    print("      DB TELEMETRY → SESSIONIZER")
    print("==========================================")

    with Session(engine) as db:

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

        # Convert database telemetry into sessionizer input
        points = [
            TelemetryPoint(
                timestamp=row.timestamp,
                weight_g=row.current_weight_g,
                gate_status=row.gate_status,
            )
            for row in rows
        ]

        print(f"TelemetryPoint objects created: {len(points)}")

        # Run sessionizer
        session = sessionize(points)

        if session is None:
            print("\n❌ No session detected.")
            return

        print("\n==========================================")
        print("       SESSIONIZATION RESULT")
        print("==========================================")

        print_session(session)

        print("\n==========================================")
        print("           DRY RUN COMPLETE")
        print("==========================================")
        print("✅ Database telemetry successfully passed")
        print("   through the sessionizer.")
        print("❗ Nothing was inserted or modified in DB.")


if __name__ == "__main__":
    main()