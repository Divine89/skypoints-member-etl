# SkyPoints member ETL

A runnable Python/PostgreSQL reference pipeline for the assessment's pipe-delimited
member feed and JSON redemption feed. The pipeline lands source records, validates
and types them in staging, derives `Age` and `Stale_Member`, and maintains current
country-specific member tables. The public-safe `examples/` feeds are synthetic;
provided assessment inputs are excluded from Git because they contain member
names, IDs, and birth dates.

## Data flow

```text
pipe-delimited member feed ──> raw_data.raw_member_data
                            └─> stg_data.stg_member_data ──> curated_data.member_current
                                                            └─> curated_data.table_<country>

JSON redemption feed ────────> raw_data.raw_redemptions_data
                            └─> stg_data.stg_redemption_data
```

`stg_redemption_data.member_id` joins to `curated_data.member_current.member_id`.
The transaction ID, transaction date, partner, miles, status, and feed date are
available as typed columns; the original transaction object is retained in raw.

## Run locally (Windows PowerShell)

1. Install Python 3.9+ and Docker Desktop.
2. Install the project dependencies and start PostgreSQL:

   ```powershell
   python -m pip install -r requirements.txt
   docker compose up -d
   ```

3. Configure the local connection. These values match `compose.yaml` defaults:

   ```powershell
   $env:PG_HOST = "localhost"
   $env:PG_PORT = "5432"
   $env:PG_DATABASE = "skypoints"
   $env:PG_USER = "skypoints"
   $env:PG_PASSWORD = "change-me"
   ```

4. Run both synthetic demo feeds and export country tables as CSV:

   ```powershell
   python -m pipeline.etl --as-of 2024-01-15 --export-dir resource\master_data_county_wise
   ```

The DDL is applied automatically. A successful invocation commits both feeds and
the country-table updates in one database transaction. A validation or database
error rolls back the batch and is reported to the console. CSV exports are
optional and are ignored by Git because they are generated from source data.

Use `--member-file` and `--redemption-file` to supply other daily feeds, and
`--as-of YYYY-MM-DD` to make age/staleness calculations reproducible. Connection
settings are read from `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, and
`PG_PASSWORD`; never commit a real `.env` file or credentials.

To run the local assessment files instead of the safe demo data:

```powershell
python -m pipeline.etl --member-file resource\source_data\pipe-delimited.txt --redemption-file resource\source_data\redumption_feed.json --as-of 2024-01-15
```

## Tables

The DDL is in [`resource/ddl_scripts.sql`](resource/ddl_scripts.sql).

* `raw_data.raw_member_data`: one JSONB source record per member row.
* `raw_data.raw_redemptions_data`: one JSONB source transaction per redemption.
* `stg_data.stg_member_data`: validated typed member attributes, source metadata,
  age, and stale flag. Constraints prevent duplicate member IDs and key names
  within a batch.
* `stg_data.stg_redemption_data`: flattened, typed redemption attributes.
* `curated_data.member_current`: current canonical record keyed by member ID.
* `curated_data.table_india`, `table_australia`, `table_usa`,
  `table_philippines`, and `table_canada`: country outputs with the six columns
  shown in the Australian workbook. Other valid two-to-five-character country
  codes create a safely quoted `table_<code>` table automatically.
* `raw_data.etl_batch`: batch ID, source names, load time, and committed row counts.

Example redemption-to-profile join:

```sql
SELECT r.transaction_id, r.transaction_date, r.miles_redeemed,
       r.status, m.member_name, m.country
FROM stg_data.stg_redemption_data AS r
JOIN curated_data.member_current AS m USING (member_id);
```

## Validation and transformation decisions

* Required member fields: name, member ID, and enrollment date. Member IDs are
  limited to 18 characters; other source columns are checked against the PDF's
  maximum lengths. Enrollment date must be `YYYYMMDD`; DOB in the provided
  delimited feed is `MMDDYYYY`; optional flight dates use `YYYYMMDD`.
* Detail markers, the header layout, field count, dates, numeric postcodes,
  country-code shape, required JSON fields, non-negative integer miles, and
  duplicate member IDs / redemption keys are checked. Database primary keys
  enforce per-batch uniqueness. Errors identify the record and abort the batch.
* `Age` is the number of completed birthdays as of `--as-of`. `Stale_Member` is
  `Y` only when a known flight date is more than 90 days old, `N` otherwise;
  it is `NULL` when flight date is missing.
* Member ID is the stable identity for country movement and redemption joins.
  The PDF labels member name as a key, so duplicate names within a batch are
  rejected as well. Member IDs are also unique within a batch to safely track
  moves and join redemptions. Latest arrival wins because the source feeds do not
  provide a profile update timestamp. The file line number is retained for audit
  and tie-breaking; duplicate keys within a batch fail validation rather than
  being silently selected.
* The PDF's column spec includes optional `Post_Code`, while its sample header
  and supplied file omit it. Both layouts are supported. IDs are checked against
  the documented maximum rather than forced to 18 characters because the sample
  contains shorter IDs. Country code `AU` is routed to Australia and `PHIL` to
  the Philippines.

## Scale and production notes

Input parsing is incremental, transaction records are streamed with `ijson`, and
database writes are bounded to batches of 5,000 rows. PostgreSQL and a single
Python worker are suitable for a local demonstration, not billions of daily
records. For that volume, land files in cloud object storage, use Snowflake
`COPY INTO` (or equivalent parallel warehouse bulk loading) into raw `VARIANT`
and typed landing tables, then run set-based SQL transformations. Partition
large raw/staging tables by ingestion date, cluster/index by member ID, retain
batch IDs, and perform idempotent `MERGE` operations with a source event
timestamp when the producer can supply one. Keep the same validations and
country-routing semantics; don't move billions of rows through Python.

## Tests

```powershell
python -m unittest discover -s tests -v
```

The unit tests cover both supplied formats, optional postcode handling, date
validation, age/staleness boundaries, and JSON flattening without requiring a
running PostgreSQL server.

## License

This project is released under the Apache License 2.0. See [`LICENSE`](LICENSE).
