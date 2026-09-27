import csv
from datetime import datetime

from sessionizer import TelemetryPoint, sessionize, print_session


CSV_FILE = r"D:\Development\Projects\HealthPaw\data\telemetry_neon\telemetry.csv"


def load_telemetry():

    points = []

    with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:

        reader = csv.DictReader(file)

        for row in reader:

            points.append(
                TelemetryPoint(
                    timestamp=datetime.fromisoformat(
                        row["timestamp"]
                    ),
                    weight_g=float(
                        row["current_weight_g"]
                    ),
                    gate_status=row["gate_status"],
                )
            )

    return points


if __name__ == "__main__":

    points = load_telemetry()

    print(f"Loaded telemetry points: {len(points)}")

    session = sessionize(points)

    if session is None:
        print("No session detected.")
    else:
        print_session(session)