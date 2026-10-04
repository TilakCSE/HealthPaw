from datetime import datetime
from sqlalchemy import select
from sqlalchemy.orm import Session

from database import engine
from models import Telemetry


def load_telemetry(
    device_id: str,
    limit: int | None = None,
) -> list[Telemetry]:

    with Session(engine) as db:

        statement = (
            select(Telemetry)
            .where(Telemetry.device_id == device_id)
            .order_by(Telemetry.timestamp.asc())
        )

        if limit is not None:
            statement = statement.limit(limit)

        return list(db.scalars(statement).all())


def print_telemetry(rows: list[Telemetry]):

    print()
    print("==========================================")
    print("          DATABASE TELEMETRY")
    print("==========================================")

    print(f"Rows loaded: {len(rows)}")
    print()

    for row in rows:

        print(
            f"{row.timestamp.isoformat(timespec='milliseconds')} | "
            f"{row.current_weight_g:.2f} g | "
            f"{row.gate_status}"
        )

    print("==========================================")