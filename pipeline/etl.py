"""Streaming ETL for SkyPoints member and redemption feeds."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import ijson
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values


ROOT = Path(__file__).resolve().parents[1]
DDL_PATH = ROOT / "resource" / "ddl_scripts.sql"
DEFAULT_MEMBER_FILE = ROOT / "examples" / "member-profile.txt"
DEFAULT_REDEMPTION_FILE = ROOT / "examples" / "redemption-feed.json"
BATCH_SIZE = 5_000

MEMBER_HEADERS = (
    "Member_Name",
    "Member_Id",
    "Enrollment_Date",
    "Last_Flight_Date",
    "Tier_Code",
    "Agent_Name",
    "State",
    "Country",
    "DOB",
    "Is_Active",
)
MEMBER_HEADERS_WITH_POSTCODE = MEMBER_HEADERS[:8] + ("Post_Code",) + MEMBER_HEADERS[8:]
MAX_LENGTHS = {
    "Member_Name": 255,
    "Member_Id": 18,
    "Tier_Code": 5,
    "Agent_Name": 255,
    "State": 5,
    "Country": 5,
    "Post_Code": 5,
    "Is_Active": 1,
}

COUNTRY_TABLES = {
    "IND": ("table_india", "IND"),
    "IN": ("table_india", "IND"),
    "AU": ("table_australia", "AUS"),
    "AUS": ("table_australia", "AUS"),
    "USA": ("table_usa", "USA"),
    "US": ("table_usa", "USA"),
    "PHIL": ("table_philippines", "PHIL"),
    "CAN": ("table_canada", "CAN"),
}
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Z0-9]{2,5}$")


@dataclass(frozen=True)
class MemberRecord:
    record_number: int
    values: dict[str, Optional[str]]


@dataclass(frozen=True)
class RedemptionRecord:
    record_number: int
    member_id: str
    feed_date: date
    transaction_id: str
    transaction_date: date
    partner: str
    miles_redeemed: int
    status: str
    payload: dict[str, Any]


def _parse_date(
    value: Optional[str], fmt: str, field: str, record_number: int, required: bool = False
) -> Optional[date]:
    if value is None or not value.strip():
        if required:
            raise ValueError(f"Record {record_number}: required field {field} is empty")
        return None
    try:
        return datetime.strptime(value.strip(), fmt).date()
    except ValueError as exc:
        raise ValueError(
            f"Record {record_number}: {field} must match {fmt}, got {value!r}"
        ) from exc


def _split_record(line: str) -> list[str]:
    parts = line.rstrip("\r\n").split("|")
    if parts and parts[-1] == "":
        parts.pop()
    return parts


def iter_member_records(path: Path) -> Iterator[MemberRecord]:
    """Read the pipe-delimited feed one record at a time."""
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        header_line_number = None
        columns: tuple[str, ...] = ()
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            parts = _split_record(line)
            if len(parts) < 2 or parts[0] != "" or parts[1] != "H":
                raise ValueError(f"Line {line_number}: expected a |H| header record")
            columns = tuple(parts[2:])
            if columns not in (MEMBER_HEADERS, MEMBER_HEADERS_WITH_POSTCODE):
                raise ValueError(
                    f"Line {line_number}: unexpected header. Expected Member_Name, "
                    "Member_Id, Enrollment_Date, Last_Flight_Date, Tier_Code, "
                    "Agent_Name, State, Country, optional Post_Code, DOB, Is_Active"
                )
            header_line_number = line_number
            break
        if header_line_number is None:
            raise ValueError(f"{path}: file is empty")

        for line_number, line in enumerate(source, start=header_line_number + 1):
            if not line.strip():
                continue
            parts = _split_record(line)
            if len(parts) < 2 or parts[0] != "" or parts[1] != "D":
                raise ValueError(f"Line {line_number}: expected a |D| detail record")
            raw_values = parts[2:]
            if len(raw_values) != len(columns):
                raise ValueError(
                    f"Line {line_number}: expected {len(columns)} fields, "
                    f"found {len(raw_values)}"
                )

            values = dict(zip(columns, raw_values))
            if "Post_Code" not in values:
                values["Post_Code"] = ""
            for field, max_length in MAX_LENGTHS.items():
                value = values.get(field, "").strip()
                if len(value) > max_length:
                    raise ValueError(
                        f"Line {line_number}: {field} exceeds {max_length} characters"
                    )

            values = {key: value.strip() or None for key, value in values.items()}
            for required_field in ("Member_Name", "Member_Id", "Enrollment_Date"):
                if values.get(required_field) is None:
                    raise ValueError(
                        f"Line {line_number}: required field {required_field} is empty"
                    )
            if values["Post_Code"] is not None and not values["Post_Code"].isdigit():
                raise ValueError(f"Line {line_number}: Post_Code must contain digits")
            if values["Country"] is not None:
                country = values["Country"].upper()
                if not COUNTRY_CODE_PATTERN.fullmatch(country):
                    raise ValueError(
                        f"Line {line_number}: invalid country code {values['Country']!r}"
                    )
                values["Country"] = country

            _parse_date(values["Enrollment_Date"], "%Y%m%d", "Enrollment_Date", line_number, True)
            _parse_date(values["Last_Flight_Date"], "%Y%m%d", "Last_Flight_Date", line_number)
            _parse_date(values["DOB"], "%m%d%Y", "DOB", line_number)
            yield MemberRecord(line_number, values)


def calculate_age(dob: Optional[date], as_of: date) -> Optional[int]:
    if dob is None:
        return None
    return as_of.year - dob.year - ((as_of.month, as_of.day) < (dob.month, dob.day))


def stale_member_flag(flight_date: Optional[date], as_of: date) -> Optional[str]:
    if flight_date is None:
        return None
    return "Y" if as_of - flight_date > timedelta(days=90) else "N"


def iter_redemption_records(path: Path) -> Iterator[RedemptionRecord]:
    """Stream redemptions from the feed; feed metadata must precede the array."""
    with path.open("rb") as source:
        events = iter(ijson.parse(source, use_float=True))
        member_id = None
        feed_date = None
        found_array = False
        for prefix, event, value in events:
            if prefix == "member_id" and event in ("string", "number"):
                member_id = str(value).strip()
            elif prefix == "feed_date" and event == "string":
                feed_date = _parse_date(str(value), "%Y%m%d", "feed_date", 1, True)
            elif prefix == "redemptions" and event == "start_array":
                found_array = True
                break
        if not found_array:
            raise ValueError(f"{path}: missing top-level redemptions array")
        if not member_id:
            raise ValueError(f"{path}: member_id must precede the redemptions array")
        if feed_date is None:
            raise ValueError(f"{path}: feed_date must precede the redemptions array")
        if len(member_id) > 18:
            raise ValueError(f"{path}: member_id exceeds 18 characters")

        record_number = 0
        transaction: dict[str, Any] = {}
        for prefix, event, value in events:
            if prefix == "redemptions.item" and event == "start_map":
                transaction = {}
            elif prefix.startswith("redemptions.item.") and event in (
                "string",
                "number",
                "boolean",
                "null",
            ):
                field = prefix.rsplit(".", 1)[1]
                transaction[field] = value
            elif prefix == "redemptions.item" and event == "end_map":
                record_number += 1
                transaction_id = str(transaction.get("txn_id", "")).strip()
                partner = str(transaction.get("partner", "")).strip()
                status = str(transaction.get("status", "")).strip()
                try:
                    raw_miles = transaction["miles_redeemed"]
                    miles = int(raw_miles)
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Redemption {record_number}: miles_redeemed must be an integer"
                    ) from exc
                if not isinstance(raw_miles, int) or isinstance(raw_miles, bool):
                    raise ValueError(
                        f"Redemption {record_number}: miles_redeemed must be an integer"
                    )
                if not transaction_id or not partner or not status:
                    raise ValueError(
                        f"Redemption {record_number}: txn_id, partner, and status are required"
                    )
                if (
                    len(transaction_id) > 64
                    or len(partner) > 255
                    or len(status) > 32
                    or miles < 0
                ):
                    raise ValueError(f"Redemption {record_number}: invalid transaction values")
                transaction_date = _parse_date(
                    str(transaction.get("txn_date", "")),
                    "%Y%m%d",
                    "txn_date",
                    record_number,
                    True,
                )
                yield RedemptionRecord(
                    record_number=record_number,
                    member_id=member_id,
                    feed_date=feed_date,
                    transaction_id=transaction_id,
                    transaction_date=transaction_date,
                    partner=partner,
                    miles_redeemed=miles,
                    status=status,
                    payload=transaction.copy(),
                )


def _chunks(items: Iterator[Any], size: int) -> Iterator[list[Any]]:
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _member_tuple(
    record: MemberRecord, batch_id: uuid.UUID, source_file: str, loaded_at: datetime, as_of: date
) -> tuple[Any, ...]:
    values = record.values
    enrollment_date = _parse_date(
        values["Enrollment_Date"], "%Y%m%d", "Enrollment_Date", record.record_number, True
    )
    flight_date = _parse_date(
        values["Last_Flight_Date"], "%Y%m%d", "Last_Flight_Date", record.record_number
    )
    dob = _parse_date(values["DOB"], "%m%d%Y", "DOB", record.record_number)
    raw_payload = dict(values)
    raw_payload["source_record_number"] = record.record_number
    raw = (
        str(batch_id),
        source_file,
        record.record_number,
        loaded_at,
        Json(raw_payload),
    )
    staged = (
        str(batch_id),
        source_file,
        record.record_number,
        loaded_at,
        values["Member_Name"],
        values["Member_Id"],
        enrollment_date,
        flight_date,
        values["Tier_Code"],
        values["Agent_Name"],
        values["State"],
        values["Country"],
        values["Post_Code"],
        dob,
        values["Is_Active"],
        calculate_age(dob, as_of),
        stale_member_flag(flight_date, as_of),
    )
    return raw, staged


def _redemption_tuple(
    record: RedemptionRecord, batch_id: uuid.UUID, source_file: str, loaded_at: datetime
) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    raw = (
        str(batch_id),
        source_file,
        record.record_number,
        record.member_id,
        record.feed_date,
        loaded_at,
        Json(record.payload),
    )
    staged = (
        str(batch_id),
        source_file,
        record.record_number,
        loaded_at,
        record.member_id,
        record.feed_date,
        record.transaction_id,
        record.transaction_date,
        record.partner,
        record.miles_redeemed,
        record.status,
    )
    return raw, staged


MEMBER_RAW_INSERT = """
    INSERT INTO raw_data.raw_member_data
        (batch_id, source_file_name, source_record_number, load_timestamp, payload)
    VALUES %s
"""
MEMBER_STG_INSERT = """
    INSERT INTO stg_data.stg_member_data
        (batch_id, source_file_name, source_record_number, load_timestamp,
         member_name, member_id, enrollment_date, last_flight_date, tier_code,
         agent_name, state, country, post_code, date_of_birth, active_member,
         age, stale_member)
    VALUES %s
"""
REDEMPTION_RAW_INSERT = """
    INSERT INTO raw_data.raw_redemptions_data
        (batch_id, source_file_name, source_record_number, member_id, feed_date,
         load_timestamp, payload)
    VALUES %s
"""
REDEMPTION_STG_INSERT = """
    INSERT INTO stg_data.stg_redemption_data
        (batch_id, source_file_name, source_record_number, load_timestamp,
         member_id, feed_date, transaction_id, transaction_date, partner,
         miles_redeemed, status)
    VALUES %s
"""


def _country_table(country: str) -> tuple[str, str]:
    if country in COUNTRY_TABLES:
        return COUNTRY_TABLES[country]
    if not COUNTRY_CODE_PATTERN.fullmatch(country):
        raise ValueError(f"Invalid country code {country!r}")
    return f"table_{country.lower()}", country


def _create_country_table(cursor: Any, table_name: str) -> None:
    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS curated_data.{} (
                unique_id VARCHAR(18) PRIMARY KEY,
                member_name VARCHAR(255) NOT NULL,
                tier_type VARCHAR(5),
                date_of_birth DATE,
                date_of_enrollment DATE NOT NULL,
                date_of_flight DATE
            )
            """
        ).format(sql.Identifier(table_name))
    )


def _curate_batch(cursor: Any, batch_id: uuid.UUID) -> None:
    cursor.execute(
        """
        INSERT INTO curated_data.member_current AS current_member
            (member_id, member_name, country, tier_code, date_of_birth,
             enrollment_date, last_flight_date, source_batch_id,
             source_record_number, load_timestamp)
        SELECT member_id, member_name, country, tier_code, date_of_birth,
               enrollment_date, last_flight_date, batch_id,
               source_record_number, load_timestamp
        FROM stg_data.stg_member_data
        WHERE batch_id = %s
        ON CONFLICT (member_id) DO UPDATE SET
            member_name = EXCLUDED.member_name,
            country = EXCLUDED.country,
            tier_code = EXCLUDED.tier_code,
            date_of_birth = EXCLUDED.date_of_birth,
            enrollment_date = EXCLUDED.enrollment_date,
            last_flight_date = EXCLUDED.last_flight_date,
            source_batch_id = EXCLUDED.source_batch_id,
            source_record_number = EXCLUDED.source_record_number,
            load_timestamp = EXCLUDED.load_timestamp
        WHERE (EXCLUDED.load_timestamp, EXCLUDED.source_batch_id,
               EXCLUDED.source_record_number)
            > (current_member.load_timestamp,
               current_member.source_batch_id,
               current_member.source_record_number)
        """,
        (str(batch_id),),
    )
    cursor.execute(
        "SELECT DISTINCT country FROM curated_data.member_current "
        "WHERE source_batch_id = %s AND country IS NOT NULL",
        (str(batch_id),),
    )
    affected_countries = {row[0] for row in cursor.fetchall()}

    cursor.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'curated_data' AND left(table_name, 6) = 'table_'"
    )
    existing_tables = [row[0] for row in cursor.fetchall()]
    for table_name in existing_tables:
        cursor.execute(
            sql.SQL(
                """
                DELETE FROM curated_data.{} AS target
                USING curated_data.member_current AS current
                WHERE target.unique_id = current.member_id
                  AND current.source_batch_id = %s
                  AND (
                      current.country IS NULL
                      OR NOT (current.country = ANY(%s))
                  )
                """
            ).format(sql.Identifier(table_name)),
            (str(batch_id), _table_country_codes(table_name)),
        )

    for country in affected_countries:
        table_name, _ = _country_table(country)
        _create_country_table(cursor, table_name)
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO curated_data.{} AS target
                    (unique_id, member_name, tier_type, date_of_birth,
                     date_of_enrollment, date_of_flight)
                SELECT member_id, member_name, tier_code, date_of_birth,
                       enrollment_date, last_flight_date
                FROM curated_data.member_current
                WHERE source_batch_id = %s AND country = %s
                ON CONFLICT (unique_id) DO UPDATE SET
                    member_name = EXCLUDED.member_name,
                    tier_type = EXCLUDED.tier_type,
                    date_of_birth = EXCLUDED.date_of_birth,
                    date_of_enrollment = EXCLUDED.date_of_enrollment,
                    date_of_flight = EXCLUDED.date_of_flight
                """
            ).format(sql.Identifier(table_name)),
            (str(batch_id), country),
        )


def _table_country_codes(table_name: str) -> list[str]:
    codes = [country for country, (table, _) in COUNTRY_TABLES.items() if table == table_name]
    return codes or [table_name[len("table_") :].upper()]


def connect_from_environment() -> Any:
    required = ("PG_HOST", "PG_DATABASE", "PG_USER", "PG_PASSWORD")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"Missing database environment variables: {', '.join(missing)}")
    try:
        return psycopg2.connect(
            host=os.environ["PG_HOST"],
            port=os.environ.get("PG_PORT", "5432"),
            dbname=os.environ["PG_DATABASE"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
        )
    except psycopg2.OperationalError as exc:
        if "password authentication failed" in str(exc).lower():
            raise RuntimeError(
                f"PostgreSQL rejected PG_USER={os.environ['PG_USER']!r}. "
                "Set PG_PASSWORD to that role's current password. If using Docker "
                "Compose, changing POSTGRES_PASSWORD does not update an existing "
                "database volume; update the role password or use the original one."
            ) from exc
        raise


def run_pipeline(
    member_file: Path,
    redemption_file: Path,
    as_of: date,
    connection: Any = None,
) -> dict[str, Any]:
    """Load both feeds, derive staging data, and publish current country tables."""
    owns_connection = connection is None
    conn = connection or connect_from_environment()
    batch_id = uuid.uuid4()
    loaded_at = datetime.now(timezone.utc)
    member_count = 0
    redemption_count = 0
    member_name = member_file.name
    redemption_name = redemption_file.name

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(DDL_PATH.read_text(encoding="utf-8"))
                cursor.execute(
                    """
                    INSERT INTO raw_data.etl_batch
                        (batch_id, member_file_name, redemption_file_name,
                         load_timestamp, member_rows, redemption_rows)
                    VALUES (%s, %s, %s, %s, 0, 0)
                    """,
                    (str(batch_id), member_name, redemption_name, loaded_at),
                )

                for records in _chunks(iter_member_records(member_file), BATCH_SIZE):
                    paired = [
                        _member_tuple(record, batch_id, member_name, loaded_at, as_of)
                        for record in records
                    ]
                    execute_values(
                        cursor, MEMBER_RAW_INSERT, [row[0] for row in paired], page_size=BATCH_SIZE
                    )
                    execute_values(
                        cursor, MEMBER_STG_INSERT, [row[1] for row in paired], page_size=BATCH_SIZE
                    )
                    member_count += len(paired)

                for records in _chunks(iter_redemption_records(redemption_file), BATCH_SIZE):
                    paired = [
                        _redemption_tuple(record, batch_id, redemption_name, loaded_at)
                        for record in records
                    ]
                    execute_values(
                        cursor,
                        REDEMPTION_RAW_INSERT,
                        [row[0] for row in paired],
                        page_size=BATCH_SIZE,
                    )
                    execute_values(
                        cursor,
                        REDEMPTION_STG_INSERT,
                        [row[1] for row in paired],
                        page_size=BATCH_SIZE,
                    )
                    redemption_count += len(paired)

                _curate_batch(cursor, batch_id)
                cursor.execute(
                    """
                    UPDATE raw_data.etl_batch
                    SET member_rows = %s, redemption_rows = %s
                    WHERE batch_id = %s
                    """,
                    (member_count, redemption_count, str(batch_id)),
                )
    finally:
        if owns_connection:
            conn.close()

    return {
        "batch_id": str(batch_id),
        "member_rows": member_count,
        "redemption_rows": redemption_count,
        "loaded_at": loaded_at.isoformat(),
    }


def export_country_tables(connection: Any, output_dir: Path) -> list[Path]:
    """Export the curated country tables to CSV for review and handoff."""
    output_dir.mkdir(parents=True, exist_ok=True)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'curated_data' AND left(table_name, 6) = 'table_' "
            "ORDER BY table_name"
        )
        table_names = [row[0] for row in cursor.fetchall()]
        exported = []
        aliases = {
            "table_india": "IND",
            "table_australia": "AUS",
            "table_usa": "USA",
            "table_philippines": "PHIL",
            "table_canada": "CAN",
        }
        for table_name in table_names:
            output_path = output_dir / f"{aliases.get(table_name, table_name[6:].upper())}.csv"
            query = sql.SQL(
                "COPY (SELECT unique_id, member_name, tier_type, date_of_birth, "
                "date_of_enrollment, date_of_flight FROM curated_data.{} "
                "ORDER BY unique_id) TO STDOUT WITH CSV HEADER"
            ).format(sql.Identifier(table_name))
            with output_path.open("w", encoding="utf-8", newline="") as output:
                cursor.copy_expert(query.as_string(connection), output)
            exported.append(output_path)
    return exported


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--member-file", type=Path, default=DEFAULT_MEMBER_FILE)
    parser.add_argument("--redemption-file", type=Path, default=DEFAULT_REDEMPTION_FILE)
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=date.today(),
        help="Date used for Age and Stale_Member calculations (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        help="Optional directory for CSV snapshots of curated country tables",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    for path in (args.member_file, args.redemption_file):
        if not path.is_file():
            raise FileNotFoundError(f"Input file not found: {path}")
    connection = connect_from_environment()
    try:
        result = run_pipeline(
            args.member_file, args.redemption_file, args.as_of, connection=connection
        )
        if args.export_dir:
            exports = export_country_tables(connection, args.export_dir)
            print("Country CSVs: " + ", ".join(str(path) for path in exports))
    finally:
        connection.close()
    print(
        f"Batch {result['batch_id']} committed: "
        f"{result['member_rows']} members, {result['redemption_rows']} redemptions"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ETL failed: {exc}", file=sys.stderr)
        raise
