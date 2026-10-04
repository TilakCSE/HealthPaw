from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    from .sessionizer import FeedingSession
except ImportError:  # Supports the existing direct-script workflow.
    from sessionizer import FeedingSession


@dataclass
class FeedingFeatures:
    consumed_g: float
    session_duration_s: Optional[float]
    eating_duration_s: float

    avg_velocity_gps: Optional[float]
    max_velocity_gps: Optional[float]

    consumption_event_count: int

    time_to_first_consumption_s: Optional[float]

    pause_count: int
    avg_pause_duration_s: Optional[float]
    max_pause_duration_s: Optional[float]

    active_eating_ratio: Optional[float]

    feeding_interval_s: Optional[float]

    daily_intake_g: Optional[float]


def extract_features(
    session: FeedingSession,
    previous_session_end: Optional[datetime] = None,
    daily_intake_g: Optional[float] = None,
) -> FeedingFeatures:

    events = session.consumption_events

    # --------------------------------------------------
    # 1. Session duration
    # --------------------------------------------------

    session_duration_s = None

    if session.end_time is not None:
        session_duration_s = (
            session.end_time - session.start_time
        ).total_seconds()

    # --------------------------------------------------
    # 2. Eating duration
    # --------------------------------------------------

    eating_duration_s = sum(
        event.duration_s
        for event in events
    )

    # --------------------------------------------------
    # 3. Average velocity
    # --------------------------------------------------

    avg_velocity_gps = None

    if eating_duration_s > 0:
        avg_velocity_gps = (
            session.consumed_g / eating_duration_s
        )

    # --------------------------------------------------
    # 4. Maximum event velocity
    # --------------------------------------------------

    max_velocity_gps = None

    if events:
        max_velocity_gps = max(
            event.velocity_gps
            for event in events
        )

    # --------------------------------------------------
    # 5. Number of consumption events
    # --------------------------------------------------

    consumption_event_count = len(events)

    # --------------------------------------------------
    # 6. Time to first consumption
    # --------------------------------------------------

    time_to_first_consumption_s = None

    if events:
        time_to_first_consumption_s = (
            events[0].start_time
            - session.start_time
        ).total_seconds()

    # --------------------------------------------------
    # 7. Pause durations
    # --------------------------------------------------

    pause_durations = []

    for previous_event, current_event in zip(
        events,
        events[1:]
    ):
        pause = (
            current_event.start_time
            - previous_event.end_time
        ).total_seconds()

        if pause >= 0:
            pause_durations.append(pause)

    pause_count = len(pause_durations)

    avg_pause_duration_s = None
    max_pause_duration_s = None

    if pause_durations:
        avg_pause_duration_s = (
            sum(pause_durations)
            / len(pause_durations)
        )

        max_pause_duration_s = max(
            pause_durations
        )

    # --------------------------------------------------
    # 8. Active eating ratio
    # --------------------------------------------------

    active_eating_ratio = None

    if (
        session_duration_s is not None
        and session_duration_s > 0
    ):
        active_eating_ratio = (
            eating_duration_s
            / session_duration_s
        )

    # --------------------------------------------------
    # 9. Feeding interval
    # --------------------------------------------------

    feeding_interval_s = None

    if previous_session_end is not None:
        feeding_interval_s = (
            session.start_time
            - previous_session_end
        ).total_seconds()

    # --------------------------------------------------
    # 10. Return feature vector
    # --------------------------------------------------

    return FeedingFeatures(
        consumed_g=session.consumed_g,
        session_duration_s=session_duration_s,
        eating_duration_s=eating_duration_s,

        avg_velocity_gps=avg_velocity_gps,
        max_velocity_gps=max_velocity_gps,

        consumption_event_count=consumption_event_count,

        time_to_first_consumption_s=time_to_first_consumption_s,

        pause_count=pause_count,
        avg_pause_duration_s=avg_pause_duration_s,
        max_pause_duration_s=max_pause_duration_s,

        active_eating_ratio=active_eating_ratio,

        feeding_interval_s=feeding_interval_s,

        daily_intake_g=daily_intake_g,
    )


def print_features(features: FeedingFeatures):
    print("\n==========================================")
    print("        HEALTHPAW FEATURE VECTOR")
    print("==========================================")

    print(f"Consumed:                  {features.consumed_g:.2f} g")

    print(
        f"Session duration:          "
        f"{features.session_duration_s}"
    )

    print(
        f"Eating duration:           "
        f"{features.eating_duration_s:.2f} s"
    )

    print(
        f"Average velocity:          "
        f"{features.avg_velocity_gps}"
    )

    print(
        f"Maximum velocity:          "
        f"{features.max_velocity_gps}"
    )

    print(
        f"Consumption events:        "
        f"{features.consumption_event_count}"
    )

    print(
        f"Time to first consumption: "
        f"{features.time_to_first_consumption_s}"
    )

    print(
        f"Pause count:               "
        f"{features.pause_count}"
    )

    print(
        f"Average pause:             "
        f"{features.avg_pause_duration_s}"
    )

    print(
        f"Maximum pause:             "
        f"{features.max_pause_duration_s}"
    )

    print(
        f"Active eating ratio:       "
        f"{features.active_eating_ratio}"
    )

    print(
        f"Feeding interval:          "
        f"{features.feeding_interval_s}"
    )

    print(
        f"Daily intake:              "
        f"{features.daily_intake_g}"
    )

    print("==========================================")
