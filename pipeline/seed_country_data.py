"""Seed the curated country tables from the supplied CSV and Excel examples."""

from __future__ import annotations

import argparse
import csv
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import psycopg2
from openpyxl import load_workbook
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from pipeline.etl import DDL_PATH, connect_from_environment


@dataclass(frozen=True)
class SeedMember:
    record_number: int
    unique_id: str
    member_name: str
    tier_type: Optional[str]
    date_of_birth: Optional[date]
    date_of_enrollment: date
    date_of_flight: Optional[date]


@dataclass(frozen=True)
class SeedReject:
    record_number: int
    member_name: Optional[str]
    reason: str
    payload: dict[str, Any]


COUNTRY_FILES = {
    "IND": "table_india",
    "USA": "table_usa",
    "AUS": "table_australia",
}


def _header_key(value: Any) -> str:
    return re.sub(r"[^a-z]", "", str(value or "").lower())


def _string_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    result = str(value).strip()
    return result or None


def _date_value(value: Any, field: str, required: bool = False) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _string_value(value)
    if text is None or text.upper() in ("NULL", "NONE", "N/A"):
        if required:
            raise ValueError(f"{field} is required")
        return None

    formats = ("%Y-%m-%d", "%m/%d/%Y")
    for date_format in formats:
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            pass

    digits = text.strip()
    if digits.isdigit() and len(digits) in (7, 8):
        try:
            return datetime.strptime(digits.zfill(8), "%m%d%Y").date()
        except ValueError:
            pass
    raise ValueError(f"{field} has invalid date value {text!r}")


def _payload(raw: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _string_value(value) for key, value in raw.items()}


def _field_map(raw: dict[str, Any]) -> dict[str, Any]:
    return {_header_key(key): value for key, value in raw.items() if key is not None}


def _parse_row(
    raw: dict[str, Any], row_number: int
) -> SeedMember:
    fields = _field_map(raw)
    unique_id = _string_value(fields.get("uniqueid") or fields.get("id"))
    member_name = _string_value(fields.get("membername") or fields.get("name"))
    if not unique_id:
        raise ValueError("ID/Unique ID is required")
    if len(unique_id) > 18:
        raise ValueError("ID/Unique ID exceeds 18 characters")
    if not member_name:
        raise ValueError("Name/Member Name is required")
    if len(member_name) > 255:
        raise ValueError("Member Name exceeds 255 characters")

    tier_type = _string_value(fields.get("tiertype") or fields.get("tiercode"))
    if tier_type and len(tier_type) > 5:
        raise ValueError("Tier exceeds 5 characters")
    return SeedMember(
        record_number=row_number,
        unique_id=unique_id,
        member_name=member_name,
        tier_type=tier_type,
        date_of_birth=_date_value(
            fields.get("dob") or fields.get("dateofbirth"), "DOB"
        ),
        date_of_enrollment=_date_value(
            fields.get("dateofenrollment") or fields.get("enrollmentdate"),
            "Enrollment Date",
            required=True,
        ),
        date_of_flight=_date_value(
            fields.get("dateofflight")
            or fields.get("flightdate")
            or fields.get("lastflightdate"),
            "Flight Date",
        ),
    )


def _read_csv(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"{path}: missing CSV header")
        for row_number, row in enumerate(reader, start=2):
            yield row_number, row


def _read_xlsx(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    workbook = load_workbook(path, data_only=True, read_only=True)
    try:
        sheet = workbook.active
        rows = sheet.iter_rows(values_only=True)
        headers = next(rows, None)
        if not headers:
            raise ValueError(f"{path}: missing worksheet header")
        for row_number, row in enumerate(rows, start=2):
            yield row_number, dict(zip(headers, row))
    finally:
        workbook.close()


def parse_seed_file(
    path: Path, country_code: str
) -> tuple[list[SeedMember], list[SeedReject]]:
    if country_code not in COUNTRY_FILES:
        raise ValueError(f"Unsupported country code {country_code!r}")
    if path.suffix.lower() == ".csv":
        rows = _read_csv(path)
    elif path.suffix.lower() in (".xlsx", ".xlsm"):
        rows = _read_xlsx(path)
    else:
        raise ValueError(f"Unsupported seed file type: {path.suffix}")

    accepted = []
    rejected = []
    for row_number, raw in rows:
        try:
            accepted.append(_parse_row(raw, row_number))
        except ValueError as exc:
            fields = _field_map(raw)
            rejected.append(
                SeedReject(
                    record_number=row_number,
                    member_name=_string_value(
                        fields.get("membername") or fields.get("name")
                    ),
                    reason=str(exc),
                    payload=_payload(raw),
                )
            )
    return accepted, rejected


def _newest_per_name(records: list[SeedMember]) -> list[SeedMember]:
    newest: dict[str, SeedMember] = {}
    for record in records:
        previous = newest.get(record.member_name)
        if previous is None or (
            record.date_of_flight is not None
            and (
                previous.date_of_flight is None
                or record.date_of_flight > previous.date_of_flight
            )
        ):
            newest[record.member_name] = record
    return list(newest.values())


def load_seed_files(
    india_csv: Path,
    usa_csv: Path,
    australia_xlsx: Path,
    connection: Any = None,
) -> dict[str, Any]:
    files = (
        ("IND", india_csv),
        ("USA", usa_csv),
        ("AUS", australia_xlsx),
    )
    parsed: dict[str, list[SeedMember]] = {}
    rejected: list[tuple[str, Path, SeedReject]] = []
    for country, path in files:
        if not path.is_file():
            raise FileNotFoundError(f"Seed file not found: {path}")
        accepted, invalid = parse_seed_file(path, country)
        parsed[country] = _newest_per_name(accepted)
        rejected.extend((country, path, item) for item in invalid)

    owns_connection = connection is None
    conn = connection or connect_from_environment()
    batch_id = uuid.uuid4()
    loaded_at = datetime.now(timezone.utc)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(DDL_PATH.read_text(encoding="utf-8"))
                for country, records in parsed.items():
                    if not records:
                        continue
                    table = COUNTRY_FILES[country]
                    insert = sql.SQL(
                        """
                        INSERT INTO curated_data.{} AS current_member
                            (unique_id, member_name, tier_type, date_of_birth,
                             date_of_enrollment, date_of_flight)
                        VALUES %s
                        ON CONFLICT (member_name) DO UPDATE SET
                            unique_id = EXCLUDED.unique_id,
                            tier_type = EXCLUDED.tier_type,
                            date_of_birth = EXCLUDED.date_of_birth,
                            date_of_enrollment = EXCLUDED.date_of_enrollment,
                            date_of_flight = EXCLUDED.date_of_flight
                        WHERE EXCLUDED.date_of_flight IS NOT NULL
                          AND (
                              current_member.date_of_flight IS NULL
                              OR EXCLUDED.date_of_flight > current_member.date_of_flight
                          )
                        """
                    ).format(sql.Identifier(table))
                    values = [
                        (
                            record.unique_id,
                            record.member_name,
                            record.tier_type,
                            record.date_of_birth,
                            record.date_of_enrollment,
                            record.date_of_flight,
                        )
                        for record in records
                    ]
                    execute_values(
                        cursor, insert.as_string(conn), values, page_size=1_000
                    )

                if rejected:
                    reject_values = [
                        (
                            str(batch_id),
                            country,
                            path.name,
                            item.record_number,
                            item.member_name,
                            item.reason,
                            Json(item.payload),
                            loaded_at,
                        )
                        for country, path, item in rejected
                    ]
                    execute_values(
                        cursor,
                        """
                        INSERT INTO curated_data.seed_rejects
                            (seed_batch_id, country_code, source_file_name,
                             source_record_number, member_name, rejection_reason,
                             payload, load_timestamp)
                        VALUES %s
                        """,
                        reject_values,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s)",
                        page_size=1_000,
                    )
    finally:
        if owns_connection:
            conn.close()

    return {
        "seed_batch_id": str(batch_id),
        "records_processed": sum(len(records) for records in parsed.values()),
        "quarantined": len(rejected),
        "rejects": [
            f"{path.name} row {item.record_number}: {item.reason}"
            for _, path, item in rejected
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--india-csv", type=Path, required=True)
    parser.add_argument("--usa-csv", type=Path, required=True)
    parser.add_argument("--australia-xlsx", type=Path, required=True)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    result = load_seed_files(args.india_csv, args.usa_csv, args.australia_xlsx)
    print(
        f"Seed batch {result['seed_batch_id']}: "
        f"{result['records_processed']} valid rows processed, "
        f"{result['quarantined']} quarantined"
    )
    for reject in result["rejects"]:
        print(f"Rejected: {reject}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Seed load failed: {exc}", file=sys.stderr)
        raise
