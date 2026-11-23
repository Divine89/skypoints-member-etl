import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import psycopg2

from pipeline.etl import (
    DEFAULT_MEMBER_FILE,
    DEFAULT_REDEMPTION_FILE,
    calculate_age,
    connect_from_environment,
    iter_member_records,
    iter_redemption_records,
    stale_member_flag,
)


class MemberFeedTests(unittest.TestCase):
    def test_synthetic_member_feed_is_parsed(self):
        records = list(iter_member_records(DEFAULT_MEMBER_FILE))
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].values["Member_Id"], "DEMO001")
        self.assertEqual(records[0].values["Country"], "IND")
        self.assertIsNone(records[0].values["Post_Code"])

    def test_postcode_is_optional_in_header(self):
        content = (
            "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|"
            "Agent_Name|State|Country|Post_Code|DOB|Is_Active\n"
            "|D|Asha|1|20240101|||||IND|00001|01012000|A\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "members.txt"
            path.write_text(content, encoding="utf-8")
            record = next(iter_member_records(path))
        self.assertEqual(record.values["Post_Code"], "00001")

    def test_invalid_date_is_reported_with_source_line(self):
        content = (
            "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|"
            "Agent_Name|State|Country|DOB|Is_Active\n"
            "|D|Asha|1|20240230||||IND||01012000|A\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.txt"
            path.write_text(content, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Record 2: Enrollment_Date"):
                list(iter_member_records(path))


class DerivedColumnTests(unittest.TestCase):
    def test_age_uses_completed_birthdays(self):
        self.assertEqual(calculate_age(date(2000, 12, 31), date(2024, 12, 30)), 23)
        self.assertEqual(calculate_age(date(2000, 12, 31), date(2024, 12, 31)), 24)
        self.assertIsNone(calculate_age(None, date(2024, 1, 1)))

    def test_stale_threshold_is_strictly_more_than_90_days(self):
        as_of = date(2024, 12, 30)
        self.assertEqual(stale_member_flag(date(2024, 10, 1), as_of), "N")
        self.assertEqual(stale_member_flag(date(2024, 9, 30), as_of), "Y")
        self.assertIsNone(stale_member_flag(None, as_of))


class DatabaseConnectionTests(unittest.TestCase):
    def test_password_authentication_error_gives_safe_guidance(self):
        environment = {
            "PG_HOST": "localhost",
            "PG_DATABASE": "skypoints",
            "PG_USER": "skypoints",
            "PG_PASSWORD": "do-not-print-this",
        }
        error = psycopg2.OperationalError(
            'connection failed: FATAL: password authentication failed for user "skypoints"'
        )
        with patch.dict(os.environ, environment, clear=True):
            with patch("pipeline.etl.psycopg2.connect", side_effect=error):
                with self.assertRaisesRegex(RuntimeError, "existing database volume") as caught:
                    connect_from_environment()
        self.assertNotIn(environment["PG_PASSWORD"], str(caught.exception))


class RedemptionFeedTests(unittest.TestCase):
    def test_synthetic_json_feed_is_flattened(self):
        records = list(iter_redemption_records(DEFAULT_REDEMPTION_FILE))
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].member_id, "DEMO001")
        self.assertEqual(records[0].transaction_id, "DEMO-TXN-1")
        self.assertEqual(records[0].miles_redeemed, 1200)
        self.assertEqual(records[1].status, "PENDING")

    def test_negative_miles_are_rejected(self):
        payload = {
            "member_id": "1",
            "feed_date": "20240115",
            "redemptions": [
                {
                    "txn_id": "RX1",
                    "txn_date": "20240110",
                    "partner": "Airline",
                    "miles_redeemed": -1,
                    "status": "COMPLETED",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "invalid transaction values"):
                list(iter_redemption_records(path))

    def test_fractional_miles_are_rejected(self):
        payload = {
            "member_id": "1",
            "feed_date": "20240115",
            "redemptions": [
                {
                    "txn_id": "RX1",
                    "txn_date": "20240110",
                    "partner": "Airline",
                    "miles_redeemed": 1.5,
                    "status": "COMPLETED",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fractional.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "miles_redeemed must be an integer"):
                list(iter_redemption_records(path))


if __name__ == "__main__":
    unittest.main()
