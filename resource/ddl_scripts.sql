CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS stg_data;
CREATE SCHEMA IF NOT EXISTS curated_data;

CREATE TABLE IF NOT EXISTS raw_data.etl_batch (
    batch_id             UUID PRIMARY KEY,
    member_file_name     TEXT NOT NULL,
    redemption_file_name TEXT NOT NULL,
    load_timestamp       TIMESTAMPTZ NOT NULL,
    member_rows          BIGINT NOT NULL,
    redemption_rows      BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_data.raw_member_data (
    batch_id             UUID NOT NULL,
    source_file_name     TEXT NOT NULL,
    source_record_number BIGINT NOT NULL,
    load_timestamp       TIMESTAMPTZ NOT NULL,
    payload              JSONB NOT NULL,
    PRIMARY KEY (batch_id, source_record_number)
);

CREATE TABLE IF NOT EXISTS raw_data.raw_redemptions_data (
    batch_id             UUID NOT NULL,
    source_file_name     TEXT NOT NULL,
    source_record_number BIGINT NOT NULL,
    member_id            VARCHAR(18) NOT NULL,
    feed_date            DATE NOT NULL,
    load_timestamp       TIMESTAMPTZ NOT NULL,
    payload              JSONB NOT NULL,
    PRIMARY KEY (batch_id, source_record_number)
);

CREATE TABLE IF NOT EXISTS stg_data.stg_member_data (
    batch_id             UUID NOT NULL,
    source_file_name     TEXT NOT NULL,
    source_record_number BIGINT NOT NULL,
    load_timestamp       TIMESTAMPTZ NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    member_id            VARCHAR(18) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    agent_name           VARCHAR(255),
    state                VARCHAR(5),
    country              VARCHAR(5),
    post_code            VARCHAR(5),
    date_of_birth        DATE,
    active_member        CHAR(1),
    age                  SMALLINT,
    stale_member         CHAR(1),
    PRIMARY KEY (batch_id, member_id),
    UNIQUE (batch_id, member_name),
    CHECK (stale_member IS NULL OR stale_member IN ('Y', 'N'))
);

CREATE INDEX IF NOT EXISTS ix_stg_member_id_latest
    ON stg_data.stg_member_data (member_id, load_timestamp DESC);

CREATE TABLE IF NOT EXISTS stg_data.stg_redemption_data (
    batch_id             UUID NOT NULL,
    source_file_name     TEXT NOT NULL,
    source_record_number BIGINT NOT NULL,
    load_timestamp       TIMESTAMPTZ NOT NULL,
    member_id            VARCHAR(18) NOT NULL,
    feed_date            DATE NOT NULL,
    transaction_id       VARCHAR(64) NOT NULL,
    transaction_date     DATE NOT NULL,
    partner              VARCHAR(255) NOT NULL,
    miles_redeemed       BIGINT NOT NULL CHECK (miles_redeemed >= 0),
    status               VARCHAR(32) NOT NULL,
    PRIMARY KEY (batch_id, member_id, transaction_id)
);

CREATE INDEX IF NOT EXISTS ix_stg_redemption_member_date
    ON stg_data.stg_redemption_data (member_id, transaction_date);

CREATE TABLE IF NOT EXISTS curated_data.member_current (
    member_id            VARCHAR(18) PRIMARY KEY,
    member_name          VARCHAR(255) NOT NULL,
    country              VARCHAR(5),
    tier_code            VARCHAR(5),
    date_of_birth        DATE,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    source_batch_id      UUID NOT NULL,
    source_record_number BIGINT NOT NULL,
    load_timestamp       TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_member_current_country
    ON curated_data.member_current (country, member_id);

CREATE TABLE IF NOT EXISTS curated_data.table_india (
    unique_id            VARCHAR(18) PRIMARY KEY,
    member_name          VARCHAR(255) NOT NULL,
    tier_type            VARCHAR(5),
    date_of_birth        DATE,
    date_of_enrollment   DATE NOT NULL,
    date_of_flight       DATE
);

CREATE TABLE IF NOT EXISTS curated_data.table_australia (
    unique_id            VARCHAR(18) PRIMARY KEY,
    member_name          VARCHAR(255) NOT NULL,
    tier_type            VARCHAR(5),
    date_of_birth        DATE,
    date_of_enrollment   DATE NOT NULL,
    date_of_flight       DATE
);

CREATE TABLE IF NOT EXISTS curated_data.table_usa (
    unique_id            VARCHAR(18) PRIMARY KEY,
    member_name          VARCHAR(255) NOT NULL,
    tier_type            VARCHAR(5),
    date_of_birth        DATE,
    date_of_enrollment   DATE NOT NULL,
    date_of_flight       DATE
);

CREATE TABLE IF NOT EXISTS curated_data.table_philippines (
    unique_id            VARCHAR(18) PRIMARY KEY,
    member_name          VARCHAR(255) NOT NULL,
    tier_type            VARCHAR(5),
    date_of_birth        DATE,
    date_of_enrollment   DATE NOT NULL,
    date_of_flight       DATE
);

CREATE TABLE IF NOT EXISTS curated_data.table_canada (
    unique_id            VARCHAR(18) PRIMARY KEY,
    member_name          VARCHAR(255) NOT NULL,
    tier_type            VARCHAR(5),
    date_of_birth        DATE,
    date_of_enrollment   DATE NOT NULL,
    date_of_flight       DATE
);
