#!/usr/bin/env python3
"""
EuroCrimePulse — Court Spark Structured Streaming Job

Pipeline:
    Kafka (eurocrimepulse.court)
      → Parse JSON / structural check
      → Bronze  (raw parsed, HDFS Parquet)
      → Clean / normalize
      → Validate (local DQ)
        ├─ FAIL → DROP
        └─ PASS
             ↓
         FK check: linked_crime_id must exist in Police Silver
           ├─ ORPHAN → DROP
           └─ MATCH  → Silver & Gold

Usage (inside Docker):
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \\
        court_stream.py
"""

import os
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "eurocrimepulse.court"

HDFS_BASE = os.getenv(
    "EUROCRIMEPULSE_HDFS_BASE", "hdfs://localhost:9000/eurocrimepulse"
)
BRONZE_PATH = f"{HDFS_BASE}/bronze/court"
SILVER_PATH = f"{HDFS_BASE}/silver/court"
GOLD_PATH = f"{HDFS_BASE}/gold/court"
CHECKPOINT_PATH = f"{os.getenv('EUROCRIMEPULSE_CHECKPOINT_BASE', 'hdfs://localhost:9000/tmp/eurocrimepulse/checkpoints')}/court"

# Police Silver path — needed for FK validation.
POLICE_SILVER_PATH = f"{HDFS_BASE}/silver/police"


# ═══════════════════════════════════════════════════════════════════════════
# Explicit schema (must match generator output)
# ═══════════════════════════════════════════════════════════════════════════
COURT_SCHEMA = StructType(
    [
        StructField("source_system", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("linked_crime_id", StringType(), True),
        StructField("crime_type", StringType(), True),
        StructField("case_status", StringType(), True),
        StructField("verdict", StringType(), True),
        StructField("verdict_date", StringType(), True),
        StructField(
            "judge",
            StructType(
                [
                    StructField("judge_id", StringType(), True),
                    StructField("judge_name", StringType(), True),
                    StructField("judge_gender", StringType(), True),
                    StructField(
                        "years_of_experience", IntegerType(), True
                    ),
                    StructField("court_name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("bail_amount", DoubleType(), True),
        StructField("fine_amount", DoubleType(), True),
        StructField("sentence_type", StringType(), True),
        StructField("sentence_duration", IntegerType(), True),
        StructField("sentence_unit", StringType(), True),
        StructField(
            "defendant",
            StructType(
                [
                    StructField("national_id", StringType(), True),
                    StructField("full_name", StringType(), True),
                    StructField("date_of_birth", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True),
                    StructField("nationality", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("_ingested_at", StringType(), True),
    ]
)


# ═══════════════════════════════════════════════════════════════════════════
# Reference data
# ═══════════════════════════════════════════════════════════════════════════
SUPPORTED_CRIME_TYPES = [
    "Theft", "Burglary", "Assault", "Fraud", "Drug Possession", "Robbery",
    "Vandalism", "Cybercrime", "DUI", "Homicide", "Money Laundering",
    "Bribery", "Smuggling",
]
SUPPORTED_VERDICTS = [
    "Guilty - Custodial", "Guilty - Fine Only", "Guilty - Probation",
    "Not Guilty", "Case Dismissed",
]
SUPPORTED_STATUSES = ["Closed", "Under Appeal"]


# ═══════════════════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════════════════
def canonical_national_id(col):
    """
    Normalise a National ID to canonical 9-digit form.
    Only explicitly supported formats are accepted; everything else → NULL.
    """
    c = F.upper(F.trim(col))
    return (
        F.when(c.rlike(r"^[0-9]{9}$"), c)
        .when(
            c.rlike(r"^ID-[0-9]{5}-[0-9]{4}$"),
            F.regexp_replace(F.regexp_replace(c, r"^ID-", ""), "-", ""),
        )
        .when(
            c.rlike(r"^[0-9]{3}-[0-9]{3}-[0-9]{3}$"),
            F.regexp_replace(c, "-", ""),
        )
        .when(
            c.rlike(r"^[0-9]{4} [0-9]{5}$"),
            F.regexp_replace(c, " ", ""),
        )
        .when(c.rlike(r"^NID[0-9]{9}$"), F.substring(c, 4, 9))
        .otherwise(F.lit(None))
    )


def exact_age(event_date_col, dob_col):
    """Birthday-aware exact age (NOT months_between / 12)."""
    return F.year(event_date_col) - F.year(dob_col) - F.when(
        F.date_format(event_date_col, "MM-dd")
        < F.date_format(dob_col, "MM-dd"),
        1,
    ).otherwise(0)


# ═══════════════════════════════════════════════════════════════════════════
# Transform & validate
# ═══════════════════════════════════════════════════════════════════════════
def transform_and_validate(raw_df):
    """
    Flatten, trim, normalise, validate.
    Returns DataFrame with ``blocking_issue_count`` (0 = locally valid).
    """
    # --- Flatten ---
    df = raw_df.select(
        "source_system", "case_id", "linked_crime_id", "crime_type",
        "case_status", "verdict", "verdict_date",
        F.col("judge.judge_id").alias("judge_id"),
        F.col("judge.judge_name").alias("judge_name"),
        F.col("judge.judge_gender").alias("judge_gender"),
        F.col("judge.years_of_experience").alias("judge_experience"),
        F.col("judge.court_name").alias("court_name"),
        "bail_amount", "fine_amount", "sentence_type",
        "sentence_duration", "sentence_unit",
        F.col("defendant.national_id").alias("defendant_national_id_raw"),
        F.col("defendant.full_name").alias("defendant_name"),
        F.col("defendant.date_of_birth").alias("defendant_dob_raw"),
        F.col("defendant.age").alias("defendant_age_reported"),
        F.col("defendant.gender").alias("defendant_gender"),
        F.col("defendant.nationality").alias("defendant_nationality"),
        "_ingested_at",
    )

    # --- Trim strings, empty → NULL ---
    str_cols = [
        "source_system", "case_id", "linked_crime_id", "crime_type",
        "case_status", "verdict", "verdict_date",
        "judge_id", "judge_name", "judge_gender", "court_name",
        "sentence_type", "sentence_unit",
        "defendant_national_id_raw", "defendant_name",
        "defendant_dob_raw", "defendant_gender", "defendant_nationality",
        "_ingested_at",
    ]
    for c in str_cols:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "", None).otherwise(
                F.trim(F.col(c))
            ),
        )

    # --- Normalise dates, IDs, derived columns ---
    df = (
        df
        # Verdict date: source formats MM/dd/yyyy  or  dd-MMM-yyyy
        .withColumn(
            "verdict_date_clean",
            F.coalesce(
                F.to_date("verdict_date", "MM/dd/yyyy"),
                F.to_date("verdict_date", "dd-MMM-yyyy"),
            ),
        )
        # Defendant DOB: source format dd/MM/yyyy
        .withColumn(
            "defendant_dob_clean",
            F.to_date("defendant_dob_raw", "dd/MM/yyyy"),
        )
        .withColumn("_ingested_at_ts", F.to_timestamp("_ingested_at"))
        .withColumn(
            "defendant_national_id_canonical",
            canonical_national_id(F.col("defendant_national_id_raw")),
        )
        .withColumn(
            "defendant_age_expected",
            exact_age(
                F.col("verdict_date_clean"), F.col("defendant_dob_clean")
            ),
        )
        .withColumn(
            "sentence_unit_normalized",
            F.lower(F.trim(F.col("sentence_unit"))),
        )
        .withColumn(
            "sentence_duration_months",
            F.when(
                F.col("sentence_unit_normalized").isin("year", "years"),
                F.col("sentence_duration") * 12,
            ).when(
                F.col("sentence_unit_normalized").isin("month", "months"),
                F.col("sentence_duration"),
            ),
        )
    )

    # --- Blocking validation rules ---
    blocking_rules = [
        # Required fields / format
        (
            "case_id_invalid",
            F.col("case_id").isNull()
            | ~F.col("case_id").rlike(r"^CASE-[A-F0-9]{32}$"),
        ),
        (
            "linked_crime_id_invalid",
            F.col("linked_crime_id").isNull()
            | ~F.col("linked_crime_id").rlike(r"^CR-[A-F0-9]{32}$"),
        ),
        (
            "case_status_invalid",
            F.col("case_status").isNull()
            | ~F.col("case_status").isin(SUPPORTED_STATUSES),
        ),
        # Defendant identity
        (
            "defendant_id_invalid",
            F.col("defendant_national_id_canonical").isNull(),
        ),
        # Negative monetary values  (FIX: cast to int, not boolean)
        (
            "negative_bail",
            (F.col("bail_amount").isNotNull() & (F.col("bail_amount") < 0)),
        ),
        (
            "negative_fine",
            (
                F.col("fine_amount").isNotNull()
                & (F.col("fine_amount") < 0)
            ),
        ),
        # Verdict date
        ("verdict_date_invalid", F.col("verdict_date_clean").isNull()),
    ]

    for name, expr in blocking_rules:
        df = df.withColumn(name, expr.cast("int"))

    # --- Age mismatch (blocking) ---
    df = df.withColumn(
        "age_mismatch",
        (
            F.col("defendant_age_reported").isNotNull()
            & F.col("defendant_age_expected").isNotNull()
            & (
                F.col("defendant_age_reported")
                != F.col("defendant_age_expected")
            )
        ).cast("int"),
    )

    # --- Aggregate blocking count ---
    blocking_col_names = [n for n, _ in blocking_rules] + ["age_mismatch"]
    df = df.withColumn(
        "blocking_issue_count",
        reduce(
            lambda a, b: a + b,
            [F.coalesce(F.col(c), F.lit(0)) for c in blocking_col_names],
            F.lit(0),
        ),
    )

    # --- Partition key ---
    df = df.withColumn(
        "event_date",
        F.coalesce(
            F.col("verdict_date_clean"),
            F.to_date(F.col("_ingested_at_ts")),
        ),
    )

    return df


# ═══════════════════════════════════════════════════════════════════════════
# Spark session
# ═══════════════════════════════════════════════════════════════════════════
spark = (
    SparkSession.builder.appName("EuroCrimePulse-Court-Streaming")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ═══════════════════════════════════════════════════════════════════════════
# Kafka source
# ═══════════════════════════════════════════════════════════════════════════
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# ═══════════════════════════════════════════════════════════════════════════
# Helpers for FK check
# ═══════════════════════════════════════════════════════════════════════════
def _read_police_silver_ref(spark_session):
    """
    Read the Police Silver crime_id set for FK validation.
    Returns an empty DataFrame if Police Silver does not exist yet.
    """
    try:
        return (
            spark_session.read.parquet(POLICE_SILVER_PATH)
            .select("crime_id")
            .dropDuplicates(["crime_id"])
        )
    except Exception:
        # Police Silver hasn't been created yet — every court record
        # will be treated as an orphan for this batch.
        return spark_session.createDataFrame(
            [], StructType([StructField("crime_id", StringType(), True)])
        )


# ═══════════════════════════════════════════════════════════════════════════
# foreachBatch processor
# ═══════════════════════════════════════════════════════════════════════════
def process_court_batch(batch_df, batch_id):
    """Process one micro-batch from Kafka."""
    if batch_df.head(1) is None or len(batch_df.head(1)) == 0:
        return

    # --- Parse JSON ---
    parsed = (
        batch_df.select(F.col("value").cast("string").alias("raw_json"))
        .withColumn("data", F.from_json(F.col("raw_json"), COURT_SCHEMA))
        .filter(F.col("data").isNotNull())
        .select("data.*")
    )

    if parsed.head(1) is None or len(parsed.head(1)) == 0:
        return

    # --- Bronze ---
    bronze = parsed.withColumn(
        "ingestion_date", F.to_date(F.to_timestamp(F.col("_ingested_at")))
    )
    bronze.write.mode("append").partitionBy("ingestion_date").parquet(
        BRONZE_PATH
    )

    # --- Transform + local validation ---
    validated = transform_and_validate(parsed)
    validated = validated.dropDuplicates(["case_id"])

    locally_valid = validated.filter(F.col("blocking_issue_count") == 0)

    total_count = validated.count()
    local_pass = locally_valid.count()
    local_dropped = total_count - local_pass

    # --- FK check: linked_crime_id → Police Silver ---
    # FIX for Bug #7: use explicit aliases and INNER join to drop orphans.
    police_ref = _read_police_silver_ref(spark)

    fk_matched = (
        locally_valid.alias("court")
        .join(
            police_ref.alias("police"),
            F.col("court.linked_crime_id") == F.col("police.crime_id"),
            "inner",                                  # orphans are dropped
        )
        .select("court.*")
    )

    fk_count = fk_matched.count()
    orphan_count = local_pass - fk_count

    # --- Silver & Gold ---
    if fk_count > 0:
        fk_matched.write.mode("append").partitionBy("event_date").parquet(
            SILVER_PATH
        )
        fk_matched.write.mode("append").partitionBy("event_date").parquet(
            GOLD_PATH
        )

    print(
        f"[Court batch {batch_id}] "
        f"total={total_count}  local_pass={local_pass}  "
        f"fk_match={fk_count}  orphan={orphan_count}  "
        f"local_dropped={local_dropped}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Start streaming query
# ═══════════════════════════════════════════════════════════════════════════
query = (
    kafka_stream.writeStream.outputMode("append")
    .foreachBatch(process_court_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Court streaming started.  Checkpoint: {CHECKPOINT_PATH}")
print(f"Reading from Kafka topic:  {KAFKA_TOPIC}")
print(f"Bronze → {BRONZE_PATH}")
print(f"Silver → {SILVER_PATH}")
print(f"Gold   → {GOLD_PATH}")
print(f"FK ref → {POLICE_SILVER_PATH}")

query.awaitTermination()
