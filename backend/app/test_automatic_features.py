"""Regression checks for feature generation at session finalization."""

import unittest
from datetime import datetime, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.models import Base, Device, FeedingFeature
from app.session_pipeline import persist_session_result
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


if __name__ == "__main__":
    unittest.main()
