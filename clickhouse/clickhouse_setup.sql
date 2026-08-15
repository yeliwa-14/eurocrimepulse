-- ═══════════════════════════════════════════════════════════════════════════
-- EuroCrimePulse — ClickHouse Star Schema DDL
--
-- Creates the analytical serving layer for the EuroCrimePulse Star Schema.
-- Run against a ClickHouse instance after the Star Schema Parquet has been
-- built by gold_star_schema.py.
--
-- Usage:
--   clickhouse-client --multiquery < clickhouse_setup.sql
-- ═══════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS eurocrimepulse;

-- ─── Dimension Tables ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_date
(
    date_key   Int64,
    full_date  Date,
    month      Int32,
    month_name String,
    quarter    Int32,
    year       Int32,
    day_name   String
)
ENGINE = MergeTree()
ORDER BY (date_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_crime_type
(
    crime_type_key Int64,
    crime_type     String
)
ENGINE = MergeTree()
ORDER BY (crime_type_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_city
(
    city_key     Int64,
    city_name    String,
    country_name String
)
ENGINE = MergeTree()
ORDER BY (city_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_geolocation
(
    geolocation_key Int64,
    latitude        Float64,
    longitude       Float64
)
ENGINE = MergeTree()
ORDER BY (geolocation_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_court
(
    court_key  Int64,
    court_name String
)
ENGINE = MergeTree()
ORDER BY (court_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_judge
(
    judge_key               Int64,
    judge_id                String,
    judge_name              Nullable(String),
    judge_gender            Nullable(String),
    judge_years_of_experience Nullable(Int32)
)
ENGINE = MergeTree()
ORDER BY (judge_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_officer
(
    officer_key  Int64,
    officer_id   String,
    officer_name Nullable(String),
    badge_number Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (officer_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_victim
(
    victim_key                   Int64,
    victim_national_id           String,
    victim_full_name             Nullable(String),
    victim_date_of_birth         Nullable(Date),
    victim_age                   Nullable(Int32),
    victim_gender                Nullable(String),
    victim_nationality           Nullable(String),
    victim_is_age_corrected_flag Nullable(UInt8)
)
ENGINE = MergeTree()
ORDER BY (victim_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_defendant
(
    defendant_key                   Int64,
    defendant_national_id           String,
    record_id                       Nullable(String),
    defendant_full_name             Nullable(String),
    defendant_age                   Nullable(Int32),
    is_cross_imputed_flag           Nullable(UInt8),
    defendant_is_age_corrected_flag Nullable(UInt8)
)
ENGINE = MergeTree()
ORDER BY (defendant_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_sentence_type
(
    sentence_type_key Int64,
    sentence_type     String
)
ENGINE = MergeTree()
ORDER BY (sentence_type_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_verdict_type
(
    verdict_type_key Int64,
    verdict_type     String
)
ENGINE = MergeTree()
ORDER BY (verdict_type_key);


CREATE TABLE IF NOT EXISTS eurocrimepulse.dim_release_reason
(
    release_reason_key Int64,
    release_reason     String
)
ENGINE = MergeTree()
ORDER BY (release_reason_key);


-- ─── Fact Table ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS eurocrimepulse.fact_crime_case
(
    crime_case_key                Int64,
    crime_id                      String,
    case_id                       String,
    crime_type_key                Nullable(Int64),
    city_key                      Nullable(Int64),
    geolocation_key               Nullable(Int64),
    court_key                     Nullable(Int64),
    judge_key                     Nullable(Int64),
    officer_key                   Nullable(Int64),
    victim_key                    Nullable(Int64),
    sentence_type_key             Nullable(Int64),
    verdict_type_key              Nullable(Int64),
    defendant_key                 Nullable(Int64),
    crime_date_key                Nullable(Int64),
    verdict_date_key              Nullable(Int64),
    imprisonment_start_date_key   Nullable(Int64),
    release_date_key              Nullable(Int64),
    release_reason_key            Nullable(Int64),
    sentence_duration             Nullable(Int32),
    is_missing_location_flag      UInt8       DEFAULT 0,
    is_missing_verdict_flag       UInt8       DEFAULT 0,
    has_corrections_record_flag   UInt8       DEFAULT 0,
    is_still_incarcerated_flag    UInt8       DEFAULT 0,
    is_duration_invalid_flag      UInt8       DEFAULT 0
)
ENGINE = MergeTree()
ORDER BY (crime_case_key)
SETTINGS index_granularity = 8192;

-- ─── Machine-Learning Output Tables ──────────────────────────────────────
-- These tables store the actual Parquet outputs from the Spark ML pipeline.
-- The primary production task is clustering; verdict prediction is optional.

CREATE TABLE IF NOT EXISTS eurocrimepulse.ml_metrics
(
    task                 String,
    status               String,
    k                    Nullable(Int32),
    silhouette           Nullable(Float64),
    total_rows           Nullable(Int64),
    train_rows           Nullable(Int64),
    test_rows            Nullable(Int64),
    accuracy             Nullable(Float64),
    weighted_precision   Nullable(Float64),
    weighted_recall      Nullable(Float64),
    weighted_f1          Nullable(Float64),
    macro_f1             Nullable(Float64),
    num_trees            Nullable(Int32),
    max_depth            Nullable(Int32),
    selected_k           Nullable(Int32),
    reason               Nullable(String),
    created_at           DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (task, created_at);

CREATE TABLE IF NOT EXISTS eurocrimepulse.ml_cluster_predictions
(
    crime_case_key                  Int64,
    crime_id                        String,
    case_id                         String,
    crime_type                      String,
    crime_year                      Nullable(Float64),
    crime_month                     Nullable(Float64),
    has_corrections_record_flag     Nullable(Float64),
    is_still_incarcerated_flag      Nullable(Float64),
    sentence_duration_safe          Nullable(Float64),
    cluster_id                      Int32,
    created_at                      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (cluster_id, crime_case_key);

CREATE TABLE IF NOT EXISTS eurocrimepulse.ml_verdict_predictions
(
    crime_case_key                  Int64,
    crime_id                        String,
    case_id                         String,
    crime_type                      String,
    actual_verdict                  Nullable(String),
    predicted_verdict               Nullable(String),
    prediction                      Nullable(Int32),
    created_at                      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (crime_case_key);

CREATE TABLE IF NOT EXISTS eurocrimepulse.ml_per_class_metrics
(
    verdict     String,
    precision   Float64,
    recall      Float64,
    f1          Float64,
    support     Int64,
    created_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (verdict, created_at);

CREATE TABLE IF NOT EXISTS eurocrimepulse.ml_feature_importance
(
    feature     String,
    importance  Float64,
    rank        Int32,
    created_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (rank, feature);

CREATE TABLE IF NOT EXISTS eurocrimepulse.ml_cluster_profile
(
    cluster_id         Int32,
    cluster_count      Int64,
    percentage         Float64,
    avg_victim_age     Float64,
    avg_defendant_age  Float64,
    top_crime_type     String,
    top_city           String,
    created_at         DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (cluster_id, created_at);

CREATE TABLE IF NOT EXISTS eurocrimepulse.ml_confusion_matrix
(
    actual_verdict    String,
    predicted_verdict String,
    count             Int64,
    created_at        DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (actual_verdict, predicted_verdict, created_at);


-- ─── Sample Analytical Queries ──────────────────────────────────────────

-- Q1: Crime count by type and year
-- SELECT
--     ct.crime_type,
--     d.year,
--     count(*) AS total_cases
-- FROM eurocrimepulse.fact_crime_case f
-- JOIN eurocrimepulse.dim_crime_type ct ON f.crime_type_key = ct.crime_type_key
-- JOIN eurocrimepulse.dim_date d ON f.crime_date_key = d.date_key
-- GROUP BY ct.crime_type, d.year
-- ORDER BY d.year, total_cases DESC;

-- Q2: Top cities by crime count
-- SELECT
--     c.city_name,
--     c.country_name,
--     count(*) AS total_crimes
-- FROM eurocrimepulse.fact_crime_case f
-- JOIN eurocrimepulse.dim_city c ON f.city_key = c.city_key
-- GROUP BY c.city_name, c.country_name
-- ORDER BY total_crimes DESC
-- LIMIT 20;

-- Q3: Average sentence duration by crime type
-- SELECT
--     ct.crime_type,
--     round(avg(f.sentence_duration), 1) AS avg_sentence_days,
--     count(*) AS cases_with_sentence
-- FROM eurocrimepulse.fact_crime_case f
-- JOIN eurocrimepulse.dim_crime_type ct ON f.crime_type_key = ct.crime_type_key
-- WHERE f.sentence_duration IS NOT NULL
-- GROUP BY ct.crime_type
-- ORDER BY avg_sentence_days DESC;

-- Q4: Verdict distribution
-- SELECT
--     vt.verdict_type,
--     count(*) AS total,
--     round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
-- FROM eurocrimepulse.fact_crime_case f
-- JOIN eurocrimepulse.dim_verdict_type vt ON f.verdict_type_key = vt.verdict_type_key
-- GROUP BY vt.verdict_type
-- ORDER BY total DESC;

-- Q5: Still incarcerated counts
-- SELECT
--     count(*) AS still_incarcerated
-- FROM eurocrimepulse.fact_crime_case
-- WHERE is_still_incarcerated_flag = 1;
