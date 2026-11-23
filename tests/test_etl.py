import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import psycopg2
from openpyxl import Workbook

from pipeline.etl import (
    DEFAULT_MEMBER_FILE,
    DEFAULT_REDEMPTION_FILE,
    calculate_age,
    connect_from_environment,
    iter_member_records,
    iter_redemption_records,
    stale_member_flag,
)
from pipeline.seed_country_data import parse_seed_file


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


class CountrySeedTests(unittest.TestCase):
    def test_csv_accepts_slash_and_unpadded_date_formats(self):
        india = (
            "ID,Name,DOB,TierCode,EnrollmentDate,Individual or Corporate,Flight Date\n"
            "1,Vikas,12/1/1998,SLV,1/1/2022,I,6/15/2022\n"
        )
        usa = (
            "ID,Name,TierCode,EnrollmentDate,FlightDate\n"
            "2,John,SLV,1052022,1152022\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            india_path = Path(directory) / "india.csv"
            usa_path = Path(directory) / "usa.csv"
            india_path.write_text(india, encoding="utf-8")
            usa_path.write_text(usa, encoding="utf-8")

            india_rows, india_rejects = parse_seed_file(india_path, "IND")
            usa_rows, usa_rejects = parse_seed_file(usa_path, "USA")

        self.assertEqual(india_rows[0].date_of_flight, date(2022, 6, 15))
        self.assertEqual(usa_rows[0].date_of_enrollment, date(2022, 1, 5))
        self.assertEqual(usa_rows[0].date_of_flight, date(2022, 1, 15))
        self.assertFalse(india_rejects)
        self.assertFalse(usa_rejects)

    def test_xlsx_quarantines_invalid_enrollment_date(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "australia.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(
                [
                    "Unique ID",
                    "Member Name",
                    "Tier Type",
                    "DOB",
                    "Date of Enrollment",
                    "Date of Flight",
                ]
            )
            sheet.append([1, "Mike", "PLT", "NULL", date(2022, 5, 11), date(2022, 8, 1)])
            sheet.append([2, "Jonnathan", "GLD", date(1997, 12, 13), "2021-13-13", date(2022, 1, 5)])
            workbook.save(path)

            accepted, rejected = parse_seed_file(path, "AUS")

        self.assertEqual(len(accepted), 1)
        self.assertIsNone(accepted[0].date_of_birth)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0].member_name, "Jonnathan")
        self.assertIn("invalid date", rejected[0].reason)


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
