from telemetry_reader import (
    load_telemetry,
    print_telemetry,
)


DEVICE_ID = "cts_01"


if __name__ == "__main__":

    rows = load_telemetry(
        device_id=DEVICE_ID
    )

    print_telemetry(rows)