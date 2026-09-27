from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List


# ============================================================
# CONFIGURATION
# ============================================================

MEANINGFUL_CONSUMPTION_G = 0.75

STABILITY_TOLERANCE_G = 0.50

STABLE_DURATION_SECONDS = 1.5

INACTIVITY_TIMEOUT_SECONDS = 600


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class TelemetryPoint:
    timestamp: datetime
    weight_g: float
    gate_status: str


@dataclass
class ConsumptionEvent:
    start_time: datetime
    end_time: datetime
    start_weight_g: float
    end_weight_g: float
    consumed_g: float
    duration_s: float
    velocity_gps: float


@dataclass
class FeedingSession:
    start_time: datetime
    end_time: datetime | None
    starting_weight_g: float
    ending_weight_g: float
    consumed_g: float
    consumption_events: List[ConsumptionEvent]


# ============================================================
# HELPER
# ============================================================

def is_stable(
    points: List[TelemetryPoint],
    tolerance_g: float
) -> bool:

    if not points:
        return False

    weights = [
        point.weight_g
        for point in points
    ]

    return (
        max(weights) - min(weights)
        <= tolerance_g
    )


# ============================================================
# SESSIONIZER
# ============================================================

def sessionize(
    points: List[TelemetryPoint]
) -> FeedingSession | None:

    if not points:
        return None

    points = sorted(
        points,
        key=lambda point: point.timestamp
    )

    session_start = points[0].timestamp
    starting_weight = points[0].weight_g

    events = []

    event_start_index = None

    last_meaningful_time = None

    stability_start_index = None

    for i in range(1, len(points)):

        current = points[i]
        previous = points[i - 1]

        drop = previous.weight_g - current.weight_g

        # ----------------------------------------------------
        # No active event
        # ----------------------------------------------------

        if event_start_index is None:

            # Look for meaningful downward movement.
            if drop >= MEANINGFUL_CONSUMPTION_G:

                # Look backwards to include small initial
                # decreases that were part of the same trend.
                start_index = i - 1

                while (
                    start_index > 0
                    and points[start_index].weight_g
                    >= points[start_index + 1].weight_g
                ):
                    start_index -= 1

                event_start_index = start_index
                stability_start_index = None
                last_meaningful_time = current.timestamp

        # ----------------------------------------------------
        # Active consumption event
        # ----------------------------------------------------

        else:

            if drop >= MEANINGFUL_CONSUMPTION_G:

                last_meaningful_time = current.timestamp
                stability_start_index = None

            else:

                # --------------------------------------------
                # Check whether the weight has become stable
                # --------------------------------------------

                if stability_start_index is None:
                    stability_start_index = i

                stable_points = points[
                    stability_start_index:i + 1
                ]

                stable_duration = (
                    current.timestamp
                    - stable_points[0].timestamp
                ).total_seconds()

                if (
                    stable_duration
                    >= STABLE_DURATION_SECONDS
                    and is_stable(
                        stable_points,
                        STABILITY_TOLERANCE_G
                    )
                ):

                    event_end_index = (
                        stability_start_index - 1
                    )

                    if (
                        event_end_index
                        > event_start_index
                    ):

                        start_point = points[
                            event_start_index
                        ]

                        end_point = points[
                            event_end_index
                        ]

                        consumed = (
                            start_point.weight_g
                            - end_point.weight_g
                        )

                        duration = (
                            end_point.timestamp
                            - start_point.timestamp
                        ).total_seconds()

                        if consumed >= MEANINGFUL_CONSUMPTION_G:

                            velocity = (
                                consumed / duration
                                if duration > 0
                                else 0
                            )

                            events.append(
                                ConsumptionEvent(
                                    start_time=start_point.timestamp,
                                    end_time=end_point.timestamp,
                                    start_weight_g=start_point.weight_g,
                                    end_weight_g=end_point.weight_g,
                                    consumed_g=consumed,
                                    duration_s=duration,
                                    velocity_gps=velocity,
                                )
                            )

                    event_start_index = None
                    stability_start_index = None

    # --------------------------------------------------------
    # Close event at end of available telemetry
    # --------------------------------------------------------

    if event_start_index is not None:

        start_point = points[event_start_index]
        end_point = points[-1]

        consumed = (
            start_point.weight_g
            - end_point.weight_g
        )

        duration = (
            end_point.timestamp
            - start_point.timestamp
        ).total_seconds()

        if consumed >= MEANINGFUL_CONSUMPTION_G:

            velocity = (
                consumed / duration
                if duration > 0
                else 0
            )

            events.append(
                ConsumptionEvent(
                    start_time=start_point.timestamp,
                    end_time=end_point.timestamp,
                    start_weight_g=start_point.weight_g,
                    end_weight_g=end_point.weight_g,
                    consumed_g=consumed,
                    duration_s=duration,
                    velocity_gps=velocity,
                )
            )

    # --------------------------------------------------------
    # Session end
    # --------------------------------------------------------

    if last_meaningful_time is not None:

        final_timestamp = points[-1].timestamp

        inactivity = (
            final_timestamp - last_meaningful_time
        ).total_seconds()

        if inactivity >= INACTIVITY_TIMEOUT_SECONDS:

            session_end = (
                last_meaningful_time
                + timedelta(
                    seconds=INACTIVITY_TIMEOUT_SECONDS
                )
            )

        else:
            session_end = None

    else:
        session_end = None

    # --------------------------------------------------------
    # Session totals
    # --------------------------------------------------------

    ending_weight = points[-1].weight_g

    consumed_g = max(
        0,
        starting_weight - ending_weight
    )

    return FeedingSession(
        start_time=session_start,
        end_time=session_end,
        starting_weight_g=starting_weight,
        ending_weight_g=ending_weight,
        consumed_g=consumed_g,
        consumption_events=events,
    )


# ============================================================
# DEBUG OUTPUT
# ============================================================

def print_session(
    session: FeedingSession
):

    print()
    print("==========================================")
    print("          HEALTHPAW SESSION")
    print("==========================================")

    print(
        f"Start:              {session.start_time}"
    )

    print(
        f"End:                {session.end_time}"
    )

    print(
        f"Starting weight:    "
        f"{session.starting_weight_g:.2f} g"
    )

    print(
        f"Ending weight:      "
        f"{session.ending_weight_g:.2f} g"
    )

    print(
        f"Total consumed:     "
        f"{session.consumed_g:.2f} g"
    )

    print()

    print(
        f"Consumption events: "
        f"{len(session.consumption_events)}"
    )

    print("------------------------------------------")

    for index, event in enumerate(
        session.consumption_events,
        start=1
    ):

        print()
        print(f"Event {index}")

        print(
            f"  Start weight: "
            f"{event.start_weight_g:.2f} g"
        )

        print(
            f"  End weight:   "
            f"{event.end_weight_g:.2f} g"
        )

        print(
            f"  Consumed:     "
            f"{event.consumed_g:.2f} g"
        )

        print(
            f"  Duration:     "
            f"{event.duration_s:.2f} s"
        )

        print(
            f"  Velocity:     "
            f"{event.velocity_gps:.4f} g/s"
        )

    print("==========================================")