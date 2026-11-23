-- SkyPoints ETL assessment showcase and read-only verification queries
--
-- Run ddl_scripts.sql, seed the country examples, and run pipeline.etl first.
-- Every statement in this file is read-only. Empty discrepancy results and
-- PASS statuses indicate that the corresponding loaded data check succeeded.
--
-- Q2 uses the as-of date persisted with each ETL batch. Older batches created
-- before that column existed fall back to their load date.

-- ============================================================================
-- Q1. Raw, staging, and country-target DDL
-- ============================================================================
-- Verify that the assessment's required tables exist.
WITH required_tables (schema_name, table_name) AS (
    VALUES
        ('raw_data', 'raw_member_data'),
        ('raw_data', 'raw_redemptions_data'),
        ('stg_data', 'stg_member_data'),
        ('stg_data', 'stg_redemption_data'),
        ('curated_data', 'member_current'),
        ('curated_data', 'table_india'),
        ('curated_data', 'table_australia'),
        ('curated_data', 'table_usa'),
        ('curated_data', 'table_philippines'),
        ('curated_data', 'table_canada')
)
SELECT
    expected.schema_name,
    expected.table_name,
    CASE
        WHEN actual.table_name IS NULL THEN 'FAIL - missing'
        ELSE 'PASS'
    END AS result
FROM required_tables AS expected
LEFT JOIN information_schema.tables AS actual
    ON actual.table_schema = expected.schema_name
   AND actual.table_name = expected.table_name
   AND actual.table_type = 'BASE TABLE'
ORDER BY expected.schema_name, expected.table_name;

-- Show the primary-key columns, including Member_Name on each country target.
SELECT
    tc.table_name,
    string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS primary_key
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON kcu.constraint_catalog = tc.constraint_catalog
   AND kcu.constraint_schema = tc.constraint_schema
   AND kcu.constraint_name = tc.constraint_name
   AND kcu.table_schema = tc.table_schema
   AND kcu.table_name = tc.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = 'curated_data'
  AND left(tc.table_name, 6) = 'table_'
GROUP BY tc.table_name
ORDER BY tc.table_name;

-- ============================================================================
-- Q2. Staging derived columns: Age and Stale_Member
-- ============================================================================
-- Compare stored values to the assessment rules for the latest ETL batch.
WITH latest_batch AS (
    SELECT
        batch_id,
        coalesce(as_of_date, load_timestamp::date) AS as_of_date
    FROM raw_data.etl_batch
    ORDER BY load_timestamp DESC, batch_id DESC
    LIMIT 1
),
calculated AS (
    SELECT
        staging.batch_id,
        staging.member_id,
        staging.age,
        staging.stale_member,
        CASE
            WHEN staging.date_of_birth IS NULL THEN NULL
            ELSE
                EXTRACT(YEAR FROM batch.as_of_date)::INTEGER
                - EXTRACT(YEAR FROM staging.date_of_birth)::INTEGER
                - CASE
                    WHEN (
                        EXTRACT(MONTH FROM batch.as_of_date)::INTEGER,
                        EXTRACT(DAY FROM batch.as_of_date)::INTEGER
                    ) < (
                        EXTRACT(MONTH FROM staging.date_of_birth)::INTEGER,
                        EXTRACT(DAY FROM staging.date_of_birth)::INTEGER
                    )
                    THEN 1
                    ELSE 0
                  END
        END AS expected_age,
        CASE
            WHEN staging.last_flight_date IS NULL THEN NULL
            WHEN batch.as_of_date - staging.last_flight_date > 90 THEN 'Y'
            ELSE 'N'
        END AS expected_stale_member
    FROM latest_batch AS batch
    JOIN stg_data.stg_member_data AS staging USING (batch_id)
)
SELECT
    batch_id,
    count(*) AS rows_checked,
    count(*) FILTER (
        WHERE age IS DISTINCT FROM expected_age
           OR stale_member IS DISTINCT FROM expected_stale_member
    ) AS derived_value_mismatches
FROM calculated
GROUP BY batch_id;

-- ============================================================================
-- Q3. Country routing and latest-record-wins behavior
-- ============================================================================
-- Illustrate a member moving from USA to India. This sample is in a CTE only;
-- it does not insert or update any database rows. The later flight date wins.
WITH member_versions (member_id, member_name, country, last_flight_date, version_number) AS (
    VALUES
        ('MOVE-DEMO-001', 'Demo Member', 'USA', DATE '2023-12-01', 1),
        ('MOVE-DEMO-001', 'Demo Member', 'IND', DATE '2024-01-10', 2)
),
ranked_versions AS (
    SELECT
        member_id,
        member_name,
        country,
        last_flight_date,
        row_number() OVER (
            PARTITION BY member_id
            ORDER BY last_flight_date DESC, version_number DESC
        ) AS version_rank
    FROM member_versions
)
SELECT
    member_id,
    member_name,
    country,
    last_flight_date,
    CASE WHEN version_rank = 1 THEN 'LATEST - KEEP' ELSE 'OLDER - SUPERSEDED' END AS result
FROM ranked_versions
ORDER BY version_rank;

-- Inspect the latest canonical profile routing from the ETL.
SELECT
    member_id,
    member_name,
    country,
    CASE upper(country)
        WHEN 'IND' THEN 'table_india'
        WHEN 'IN' THEN 'table_india'
        WHEN 'AU' THEN 'table_australia'
        WHEN 'AUS' THEN 'table_australia'
        WHEN 'USA' THEN 'table_usa'
        WHEN 'US' THEN 'table_usa'
        WHEN 'PHIL' THEN 'table_philippines'
        WHEN 'CAN' THEN 'table_canada'
        ELSE 'table_' || lower(country)
    END AS routed_target_table,
    last_flight_date,
    source_batch_id
FROM curated_data.member_current
ORDER BY load_timestamp DESC, member_id
LIMIT 100;

-- Show current row counts in the static country targets.
SELECT 'IND' AS country_code, count(*) AS member_rows FROM curated_data.table_india
UNION ALL
SELECT 'AUS', count(*) FROM curated_data.table_australia
UNION ALL
SELECT 'USA', count(*) FROM curated_data.table_usa
UNION ALL
SELECT 'PHIL', count(*) FROM curated_data.table_philippines
UNION ALL
SELECT 'CAN', count(*) FROM curated_data.table_canada
ORDER BY country_code;

-- ============================================================================
-- Q4. Flattened JSON redemption records and profile join
-- ============================================================================
-- Raw has one JSON transaction object per row; staging exposes its typed fields.
-- These counts should agree for the latest batch.
WITH latest_batch AS (
    SELECT batch_id
    FROM raw_data.etl_batch
    ORDER BY load_timestamp DESC, batch_id DESC
    LIMIT 1
)
SELECT
    latest_batch.batch_id,
    (
        SELECT count(*)
        FROM raw_data.raw_redemptions_data AS raw
        WHERE raw.batch_id = latest_batch.batch_id
    ) AS raw_transaction_rows,
    (
        SELECT count(*)
        FROM stg_data.stg_redemption_data AS staging
        WHERE staging.batch_id = latest_batch.batch_id
    ) AS flattened_transaction_rows
FROM latest_batch;

-- The redemption feed supplies Member_Id, which locates the canonical profile.
-- The country output is then looked up by (country, Member_Name), its primary key.
WITH latest_batch AS (
    SELECT batch_id
    FROM raw_data.etl_batch
    ORDER BY load_timestamp DESC, batch_id DESC
    LIMIT 1
),
country_members AS (
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
    redemption.batch_id,
    redemption.member_id,
    profile.member_name,
    profile.country,
    redemption.transaction_id,
    redemption.transaction_date,
    redemption.partner,
    redemption.miles_redeemed,
    redemption.status,
    redemption.feed_date,
    CASE
        WHEN profile.member_id IS NULL THEN 'PROFILE NOT FOUND'
        WHEN target.member_name IS NULL THEN 'COUNTRY TARGET NOT FOUND'
        ELSE 'COUNTRY TARGET MATCHED'
    END AS profile_join_result
FROM stg_data.stg_redemption_data AS redemption
JOIN latest_batch USING (batch_id)
LEFT JOIN member_profile AS profile
    ON profile.member_id = redemption.member_id
LEFT JOIN country_members AS target
    ON target.country_code = profile.country_code
   AND target.member_name = profile.member_name
ORDER BY redemption.transaction_date, redemption.transaction_id;

-- ============================================================================
-- Q5. Mandatory fields, keys, and data-quality checks
-- ============================================================================
-- These checks inspect the latest daily batch. Invalid feed records are rejected
-- by the Python parser before staging; a failed daily batch is rolled back.
WITH latest_batch AS (
    SELECT batch_id
    FROM raw_data.etl_batch
    ORDER BY load_timestamp DESC, batch_id DESC
    LIMIT 1
),
duplicate_member_ids AS (
    SELECT staging.batch_id, staging.member_id
    FROM stg_data.stg_member_data AS staging
    JOIN latest_batch USING (batch_id)
    GROUP BY staging.batch_id, staging.member_id
    HAVING count(*) > 1
),
duplicate_member_names AS (
    SELECT staging.batch_id, staging.member_name
    FROM stg_data.stg_member_data AS staging
    JOIN latest_batch USING (batch_id)
    GROUP BY staging.batch_id, staging.member_name
    HAVING count(*) > 1
),
duplicate_redemptions AS (
    SELECT staging.batch_id, staging.member_id, staging.transaction_id
    FROM stg_data.stg_redemption_data AS staging
    JOIN latest_batch USING (batch_id)
    GROUP BY staging.batch_id, staging.member_id, staging.transaction_id
    HAVING count(*) > 1
),
checks AS (
    SELECT
        'ETL batch exists' AS check_name,
        CASE WHEN count(*) = 0 THEN 1 ELSE 0 END::BIGINT AS violations
    FROM latest_batch

    UNION ALL
    SELECT
        'Member required fields and field formats' AS check_name,
        count(*) FILTER (
            WHERE staging.member_name IS NULL
               OR btrim(staging.member_name) = ''
               OR length(staging.member_name) > 255
               OR staging.member_id IS NULL
               OR btrim(staging.member_id) = ''
               OR length(staging.member_id) > 18
               OR staging.enrollment_date IS NULL
               OR length(coalesce(staging.tier_code, '')) > 5
               OR length(coalesce(staging.agent_name, '')) > 255
               OR length(coalesce(staging.state, '')) > 5
               OR (
                   staging.country IS NOT NULL
                   AND staging.country !~ '^[A-Z0-9]{2,5}$'
               )
               OR (
                   staging.post_code IS NOT NULL
                   AND staging.post_code !~ '^[0-9]{1,5}$'
               )
               OR length(coalesce(staging.active_member, '')) > 1
               OR staging.age < 0
               OR (
                   staging.stale_member IS NOT NULL
                   AND staging.stale_member NOT IN ('Y', 'N')
               )
        )::BIGINT AS violations
    FROM stg_data.stg_member_data AS staging
    JOIN latest_batch USING (batch_id)

    UNION ALL
    SELECT 'Duplicate Member_Id within batch', count(*)::BIGINT
    FROM duplicate_member_ids

    UNION ALL
    SELECT 'Duplicate Member_Name within batch', count(*)::BIGINT
    FROM duplicate_member_names

    UNION ALL
    SELECT
        'Redemption required fields and values',
        count(*) FILTER (
            WHERE staging.member_id IS NULL
               OR btrim(staging.member_id) = ''
               OR length(staging.member_id) > 18
               OR staging.transaction_id IS NULL
               OR btrim(staging.transaction_id) = ''
               OR length(staging.transaction_id) > 64
               OR staging.feed_date IS NULL
               OR staging.transaction_date IS NULL
               OR staging.partner IS NULL
               OR btrim(staging.partner) = ''
               OR length(staging.partner) > 255
               OR staging.miles_redeemed < 0
               OR staging.status IS NULL
               OR btrim(staging.status) = ''
               OR length(staging.status) > 32
        )::BIGINT
    FROM stg_data.stg_redemption_data AS staging
    JOIN latest_batch USING (batch_id)

    UNION ALL
    SELECT 'Duplicate redemption key within batch', count(*)::BIGINT
    FROM duplicate_redemptions
)
SELECT
    check_name,
    violations,
    CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM checks
ORDER BY check_name;

-- Seed-file validation rejects are quarantined separately. This lists metadata
-- only; the raw payload can contain member PII and is intentionally not selected.
SELECT
    country_code,
    source_file_name,
    source_record_number,
    rejection_reason,
    load_timestamp
FROM curated_data.seed_rejects
ORDER BY load_timestamp DESC, source_file_name, source_record_number
LIMIT 20;

-- ============================================================================
-- Q6. Live demonstration evidence
-- ============================================================================
-- Suggested demo:
--   1. Start PostgreSQL and run ddl_scripts.sql (the ETL also applies it).
--   2. Run the country seed loader; point out that invalid seed rows are listed
--      in curated_data.seed_rejects while valid rows load.
--   3. Run pipeline.etl with the sample member/JSON feeds. By default, Age and
--      Stale_Member use the local execution date, persisted as etl_batch.as_of_date.
--   4. Run this SQL file and walk through Q1-Q5, including the redemption join.
--   5. For a safe upsert demonstration, use a disposable synthetic member feed:
--      submit the same Member_Name with a later Last_Flight_Date and a changed
--      country, then show the latest profile in member_current and its target.
--      Do not edit or publish the supplied member data to perform the demo.
--
-- Batch history is the reproducible run record for the live demonstration.
SELECT
    batch_id,
    member_file_name,
    redemption_file_name,
    load_timestamp,
    as_of_date,
    member_rows,
    redemption_rows
FROM raw_data.etl_batch
ORDER BY load_timestamp DESC, batch_id DESC
LIMIT 10;
