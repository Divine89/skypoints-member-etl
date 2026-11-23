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
   $env:PG_DATABASE = "postgres"
   $env:PG_USER = "postgres"
   $env:PG_PASSWORD = "change-me"
   ```

4. Run both synthetic demo feeds and export country tables as CSV:

   ```powershell
   python -m pipeline.etl --export-dir resource\master_data_county_wise
   ```

The DDL is applied automatically. A successful invocation commits both feeds and
the country-table updates in one database transaction. A validation or database
error rolls back the batch and is reported to the console. CSV exports are
optional and are ignored by Git because they are generated from source data.

Use `--member-file` and `--redemption-file` to supply other daily feeds, and
`--as-of YYYY-MM-DD` only when you need reproducible backfills. By default,
`Age` and `Stale_Member` use the local execution date, which is recorded in
`raw_data.etl_batch.as_of_date`. Connection settings are read from `PG_HOST`,
`PG_PORT`, `PG_DATABASE`, `PG_USER`, and `PG_PASSWORD`; never commit a real
`.env` file or credentials.

To run the local assessment files instead of the safe demo data:

```powershell
python -m pipeline.etl --member-file resource\source_data\pipe-delimited.txt --redemption-file resource\source_data\redumption_feed.json
```

## Seed the shared country examples

The supplied country CSV/XLSX files are already shaped like curated outputs, so
load them directly into the country tables before running the daily ETL. Their
source files are not copied into Git. Point the loader at the local files:

```powershell
python -m pipeline.seed_country_data --india-csv ".\IND(in).csv" --usa-csv ".\USA(in).csv" --australia-xlsx ".\AUS.xlsx"
```

Place `IND(in).csv`, `USA(in).csv`, and `AUS.xlsx` in the project root first.
These source files are ignored by Git and will not be published. The loader
accepts the date formats used in those files, applies the DDL, and
upserts by member name within each country table. A row is updated only when its
flight date is later than the stored flight date. The invalid Australian
enrollment date is written to `curated_data.seed_rejects`; valid rows in that
file still load. `NULL`/blank optional dates stay `NULL`.

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
  shown in the Australian workbook; `member_name` is the primary key within
  each country table.
* `curated_data.seed_rejects`: rows from curated-example seed files that failed
  validation, including the source file, row number, payload, and reason.
* `raw_data.etl_batch`: batch ID, source names, load time, the `as_of_date` used
  for derived columns, and committed row counts.

### Adding another country

For the daily member feed, no manual DDL is needed for a new country. The ETL
accepts a 2–5 character alphanumeric country code, uppercases it, and creates a
safely quoted `curated_data.table_<code>` table automatically. For example,
`CHN` creates `curated_data.table_chn` and `MYS` creates
`curated_data.table_mys`. Those tables use the same schema and
`Member_Name`/latest-flight upsert behavior as the existing country tables, and
the CSV export includes them automatically.

Use one canonical code consistently: country aliases are explicitly mapped only
for the existing supported codes, so `CN` and `CHN` would create separate
tables. The country seed loader currently accepts only India, USA, and
Australia; loading historical files for another country requires adding its
code and CLI argument to that loader. The example redemption join below and the
country-target section in `resource/assessment_showcase.sql` also list the
current static tables; add a `UNION ALL` branch there to include a new country.

Example redemption-to-country-target join:

```sql
WITH country_members AS (
    SELECT 'IND' AS country_code, member_name FROM curated_data.table_india
    UNION ALL
    SELECT 'AUS', member_name FROM curated_data.table_australia
    UNION ALL
    SELECT 'USA', member_name FROM curated_data.table_usa
    UNION ALL
    SELECT 'PHIL', member_name FROM curated_data.table_philippines
    UNION ALL
    SELECT 'CAN', member_name FROM curated_data.table_canada
),
member_profile AS (
    SELECT
        member_id,
        member_name,
        country,
        CASE upper(country)
            WHEN 'IN' THEN 'IND'
            WHEN 'AU' THEN 'AUS'
            WHEN 'US' THEN 'USA'
            ELSE upper(country)
        END AS country_code
    FROM curated_data.member_current
)
SELECT
    redemption.transaction_id,
    redemption.transaction_date,
    redemption.miles_redeemed,
    redemption.status,
    profile.member_name,
    profile.country,
    CASE
        WHEN target.member_name IS NULL THEN 'COUNTRY TARGET NOT FOUND'
        ELSE 'COUNTRY TARGET MATCHED'
    END AS target_match
FROM stg_data.stg_redemption_data AS redemption
LEFT JOIN member_profile AS profile
    ON profile.member_id = redemption.member_id
LEFT JOIN country_members AS target
    ON target.country_code = profile.country_code
   AND target.member_name = profile.member_name;
```

The redemption feed contains `member_id`, so that key first locates the
canonical profile. The country target lookup then uses `(country, member_name)`,
matching the target-table primary key and allowing same-name members in
different countries.

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
* The PDF identifies `Member_Name` as the profile key. Curated country tables
  therefore upsert by name within each country, and replace a row only when the
  incoming `Last_Flight_Date` is later; missing, equal, or older dates do not
  overwrite a dated row. Same-name records in different country tables remain
  separate. The internal `member_current` table is keyed by source member ID for
  redemption joins and to detect a country move; it accepts a profile update
  only when its flight date advances.
* Duplicate member IDs and key names within a daily batch are rejected. The
  file line number is retained for audit. The seed CSV/XLSX loader quarantines
  invalid records instead of dropping them silently.
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

The unit tests cover the daily feeds, curated seed formats, invalid seed-row
quarantine, date validation, age/staleness boundaries, and JSON flattening
without requiring a running PostgreSQL server.

## Assessment SQL showcase

After applying the DDL, loading the country examples, and running the ETL,
execute [`resource/assessment_showcase.sql`](resource/assessment_showcase.sql)
in pgAdmin, or run:

```powershell
psql -h localhost -U skypoints -d skypoints -f resource\assessment_showcase.sql
```

It has read-only evidence and validation queries for deliverables 1–5, a
synthetic country-move/latest-flight example, and a short live-demo checklist
for deliverable 6. The Age/Stale_Member audit reads the `as_of_date` recorded
with the latest ETL batch. Zero mismatches and `PASS` results are the expected
outcomes.

## License

This project is released under the Apache License 2.0. See [`LICENSE`](LICENSE).
