"""Regression checks for feature generation at session finalization."""

import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from io import StringIO
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.models import Base, Device, FeedingFeature, Telemetry
from app.session_pipeline import persist_session_result, process_device_telemetry
from app.sessionizer import ConsumptionEvent, FeedingSession


class AutomaticFeatureGenerationTest(unittest.TestCase):
    def test_open_session_waits_until_finalization_and_is_idempotent(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        start = datetime(2026, 9, 27, 15, 0, 0)
        events = [
            ConsumptionEvent(
                start_time=start,
                end_time=start + timedelta(seconds=10),
                start_weight_g=200.0,
                end_weight_g=195.0,
                consumed_g=5.0,
                duration_s=10.0,
                velocity_gps=0.5,
            ),
            ConsumptionEvent(
                start_time=start + timedelta(seconds=20),
                end_time=start + timedelta(seconds=30),
                start_weight_g=190.0,
                end_weight_g=180.0,
                consumed_g=10.0,
                duration_s=10.0,
                velocity_gps=1.0,
            ),
        ]

        with Session(engine) as db:
            db.add(Device(device_id="test-device"))
            open_result = FeedingSession(
                start_time=start,
                end_time=None,
                starting_weight_g=200.0,
                ending_weight_g=185.0,
                consumed_g=15.0,
                consumption_events=events,
            )
            session_row = persist_session_result(
                db, "test-device", open_result
            )
            db.flush()

            self.assertIsNone(session_row.session_end)
            self.assertIsNone(db.scalar(select(FeedingFeature.id)))

            completed_result = FeedingSession(
                start_time=start,
                end_time=start + timedelta(seconds=100),
                starting_weight_g=200.0,
                ending_weight_g=185.0,
                consumed_g=15.0,
                consumption_events=events,
            )
            finalized = persist_session_result(
                db, "test-device", completed_result
            )
            db.commit()

            feature = finalized.features
            self.assertIsNotNone(feature)
            self.assertEqual(feature.consumed_g, 15.0)
            self.assertEqual(feature.session_duration_s, 100.0)
            self.assertEqual(feature.eating_duration_s, 20.0)
            self.assertEqual(feature.avg_velocity_gps, 0.75)
            self.assertEqual(feature.max_velocity_gps, 1.0)
            self.assertEqual(feature.pause_count, 1)

            duplicate = persist_session_result(
                db, "test-device", completed_result
            )
            self.assertEqual(duplicate.features.id, feature.id)
            self.assertEqual(len(db.scalars(select(FeedingFeature)).all()), 1)

    def test_mqtt_telemetry_finalization_logs_one_demo_prediction(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        start = datetime(2026, 10, 4, 12, 0, 0)
        telemetry = [
            (0.0, 300.0),
            (0.5, 299.0),
            (1.0, 299.01),
            (1.5, 299.0),
            (2.0, 299.0),
            (2.5, 299.0),
            (3.0, 299.0),
            (600.5, 299.0),
        ]

        with Session(engine) as db:
            db.add(Device(device_id="demo-device"))
            db.add_all(
                Telemetry(
                    device_id="demo-device",
                    timestamp=start + timedelta(seconds=seconds),
                    current_weight_g=weight,
                    gate_status="CLOSED",
                )
                for seconds, weight in telemetry
            )
            db.commit()

            output = StringIO()
            with redirect_stdout(output):
                rows = process_device_telemetry(db, "demo-device")

            self.assertEqual(len(rows), 1)
            self.assertIsNotNone(rows[0].features)
            self.assertIn("SYNTHETIC DEMO PREDICTION", output.getvalue())

            repeated_output = StringIO()
            with redirect_stdout(repeated_output):
                process_device_telemetry(db, "demo-device")
            self.assertNotIn(
                "SYNTHETIC DEMO PREDICTION",
                repeated_output.getvalue(),
            )

    def test_two_meals_with_an_inactivity_gap_are_separate_sessions(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        start = datetime(2026, 10, 4, 13, 0, 0)
        telemetry = [
            (0.0, 300.0), (0.5, 299.0), (1.0, 299.0),
            (1.5, 299.0), (2.0, 299.0), (3.0, 299.0),
            (64.0, 299.0), (64.5, 298.0), (65.0, 298.0),
            (65.5, 298.0), (66.0, 298.0), (124.5, 298.0),
        ]

        with Session(engine) as db:
            db.add(Device(device_id="two-meals"))
            db.add_all(
                Telemetry(
                    device_id="two-meals",
                    timestamp=start + timedelta(seconds=seconds),
                    current_weight_g=weight,
                    gate_status="CLOSED",
                )
                for seconds, weight in telemetry
            )
            db.commit()

            output = StringIO()
            with (
                patch("app.sessionizer.INACTIVITY_TIMEOUT_SECONDS", 60),
                patch("app.session_pipeline.INACTIVITY_TIMEOUT_SECONDS", 60),
            ):
                with redirect_stdout(output):
                    rows = process_device_telemetry(db, "two-meals")

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].consumed_g, 1.0)
            self.assertEqual(rows[1].consumed_g, 1.0)
            self.assertTrue(all(row.features is not None for row in rows))
            self.assertEqual(
                len(db.scalars(select(FeedingFeature)).all()),
                2,
            )
            self.assertEqual(
                output.getvalue().count("SYNTHETIC DEMO PREDICTION"),
                2,
            )


if __name__ == "__main__":
    unittest.main()
