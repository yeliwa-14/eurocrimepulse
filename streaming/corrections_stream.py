#!/usr/bin/env python3
"""
EuroCrimePulse — Corrections Spark Structured Streaming Job

Pipeline:
    Kafka (eurocrimepulse.corrections)
      → Parse JSON / structural check
      → Bronze  (raw parsed, HDFS Parquet)
      → Clean / normalize
      → Validate (local DQ)
        ├─ FAIL → DROP
        └─ PASS
             ↓
         FK check: linked_case_id must exist in Court Silver
         AND defendant canonical ID must match Court defendant
           ├─ ORPHAN / MISMATCH → DROP
           └─ MATCH              → Silver & Gold

Usage (inside Docker):
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \\
        corrections_stream.py
"""

import os
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "eurocrimepulse.corrections"

HDFS_BASE = os.getenv(
    "EUROCRIMEPULSE_HDFS_BASE", "hdfs://localhost:9000/eurocrimepulse"
)
BRONZE_PATH = f"{HDFS_BASE}/bronze/corrections"
SILVER_PATH = f"{HDFS_BASE}/silver/corrections"
GOLD_PATH = f"{HDFS_BASE}/gold/corrections"

# Corrections checkpoint is intentionally on HDFS (not local /tmp) so that
# checkpoint state survives container restarts.  The base can be overridden
# via EUROCRIMEPULSE_CHECKPOINT_BASE (must also be an HDFS path if used).
_CHECKPOINT_BASE = os.getenv("EUROCRIMEPULSE_CHECKPOINT_BASE", "hdfs://localhost:9000/tmp/eurocrimepulse/checkpoints")
CHECKPOINT_PATH = f"{_CHECKPOINT_BASE}/corrections"

# Court Silver path — needed for FK + defendant identity validation.
COURT_SILVER_PATH = f"{HDFS_BASE}/silver/court"


# ═══════════════════════════════════════════════════════════════════════════
# Explicit schema (must match generator output)
# ═══════════════════════════════════════════════════════════════════════════
CORRECTIONS_SCHEMA = StructType(
    [
        StructField("source_system", StringType(), True),
        StructField("record_id", StringType(), True),
        StructField("linked_case_id", StringType(), True),
        StructField(
            "defendant",
            StructType(
                [
                    StructField("national_id", StringType(), True),
                    StructField("full_name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("prison_name", StringType(), True),
        StructField("imprisonment_start_date", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("release_reason", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)

VALID_RELEASE_REASONS = [
    "Served Full Term",
    "Released on Bail",
    "Early Parole",
    "Pardoned",
    "Sentence Commuted",
    "Escaped (Recaptured)",
]


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


def parse_source_date(col):
    """
    Parse date strings from Corrections source.
    Supports:  DD.MM.YYYY  and  YYYY-MM-DD
    """
    return F.coalesce(
        F.to_date(col, "dd.MM.yyyy"),
        F.to_date(col, "yyyy-MM-dd"),
    )


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
        "source_system",
        "record_id",
        "linked_case_id",
        F.col("defendant.national_id").alias("defendant_national_id_raw"),
        F.col("defendant.full_name").alias("defendant_full_name"),
        "prison_name",
        "imprisonment_start_date",
        "release_date",
        "release_reason",
        "_ingested_at",
    )

    # --- Trim strings, empty → NULL ---
    str_cols = [
        "source_system", "record_id", "linked_case_id",
        "defendant_national_id_raw", "defendant_full_name",
        "prison_name", "imprisonment_start_date", "release_date",
        "release_reason", "_ingested_at",
    ]
    for c in str_cols:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "", None).otherwise(
                F.trim(F.col(c))
            ),
        )

    # --- Normalise dates & IDs ---
    df = (
        df
        .withColumn(
            "defendant_national_id_canonical",
            canonical_national_id(F.col("defendant_national_id_raw")),
        )
        .withColumn(
            "imprisonment_start_date_clean",
            parse_source_date(F.col("imprisonment_start_date")),
        )
        .withColumn(
            "release_date_clean",
            parse_source_date(F.col("release_date")),
        )
        .withColumn("_ingested_at_ts", F.to_timestamp("_ingested_at"))
    )

    # --- Blocking validation rules ---
    # FIX for Bug #9: defendant_id_invalid appears ONCE, not twice.
    blocking_rules = [
        # Required fields / format
        (
            "record_id_invalid",
            F.col("record_id").isNull()
            | ~F.col("record_id").rlike(r"^COR-[A-F0-9]{32}$"),
        ),
        (
            "linked_case_id_invalid",
            F.col("linked_case_id").isNull()
            | ~F.col("linked_case_id").rlike(r"^CASE-[A-F0-9]{32}$"),
        ),
        # Defendant identity
        (
            "defendant_id_invalid",
            F.col("defendant_national_id_canonical").isNull(),
        ),
        # Start date must be parseable
        (
            "start_date_invalid",
            F.col("imprisonment_start_date_clean").isNull(),
        ),
        # Release date: if raw value exists but can't be parsed
        (
            "release_date_invalid",
            F.col("release_date").isNotNull()
            & F.col("release_date_clean").isNull(),
        ),
        # Release BEFORE start → DROP (do NOT fix the date)
        (
            "release_before_start",
            F.col("release_date_clean").isNotNull()
            & F.col("imprisonment_start_date_clean").isNotNull()
            & (
                F.col("release_date_clean")
                < F.col("imprisonment_start_date_clean")
            ),
        ),
    ]

    for name, expr in blocking_rules:
        df = df.withColumn(name, expr.cast("int"))

    # --- Aggregate blocking count (no double-counting) ---
    blocking_col_names = [n for n, _ in blocking_rules]
    df = df.withColumn(
        "blocking_issue_count",
        reduce(
            lambda a, b: a + b,
            [F.coalesce(F.col(c), F.lit(0)) for c in blocking_col_names],
            F.lit(0),
        ),
    )

    # --- Derived columns ---
    df = (
        df
        .withColumn(
            "release_status",
            F.when(F.col("release_date_clean").isNull(), "Still Imprisoned")
            .otherwise("Released"),
        )
        .withColumn(
            "event_date",
            F.coalesce(
                F.col("imprisonment_start_date_clean"),
                F.to_date(F.col("_ingested_at_ts")),
            ),
        )
    )

    return df


# ═══════════════════════════════════════════════════════════════════════════
# Spark session
# ═══════════════════════════════════════════════════════════════════════════
spark = (
    SparkSession.builder.appName("EuroCrimePulse-Corrections-Streaming")
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
# Helpers for FK + identity check
# ═══════════════════════════════════════════════════════════════════════════
def _read_court_silver_ref(spark_session):
    """
    Read Court Silver for FK + defendant identity validation.
    Returns an empty DataFrame if Court Silver does not exist yet.
    """
    try:
        return (
            spark_session.read.parquet(COURT_SILVER_PATH)
            .select("case_id", "defendant_national_id_canonical")
            .dropDuplicates(["case_id"])
        )
    except Exception:
        return spark_session.createDataFrame(
            [],
            StructType(
                [
                    StructField("case_id", StringType(), True),
                    StructField(
                        "defendant_national_id_canonical",
                        StringType(),
                        True,
                    ),
                ]
            ),
        )


# ═══════════════════════════════════════════════════════════════════════════
# foreachBatch processor
# ═══════════════════════════════════════════════════════════════════════════
def process_corrections_batch(batch_df, batch_id):
    """Process one micro-batch from Kafka."""
    if batch_df.head(1) is None or len(batch_df.head(1)) == 0:
        return

    # --- Parse JSON ---
    parsed = (
        batch_df.select(F.col("value").cast("string").alias("raw_json"))
        .withColumn(
            "data", F.from_json(F.col("raw_json"), CORRECTIONS_SCHEMA)
        )
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
    validated = validated.dropDuplicates(["record_id"])

    locally_valid = validated.filter(F.col("blocking_issue_count") == 0)

    total_count = validated.count()
    local_pass = locally_valid.count()
    local_dropped = total_count - local_pass

    # --- FK check: linked_case_id → Court Silver ---
    # --- AND: defendant canonical ID must match ---
    # FIX for Bug #8: correct variable names, explicit aliases.
    court_ref = _read_court_silver_ref(spark)

    fk_and_identity_matched = (
        locally_valid.alias("corr")
        .join(
            court_ref.alias("ct"),
            F.col("corr.linked_case_id") == F.col("ct.case_id"),
            "inner",                                  # orphans dropped
        )
        .filter(
            # Defendant identity must match after canonical normalisation.
            F.col("corr.defendant_national_id_canonical")
            == F.col("ct.defendant_national_id_canonical")
        )
        .select("corr.*")
    )

    matched_count = fk_and_identity_matched.count()
    ref_dropped = local_pass - matched_count

    # --- Silver & Gold ---
    if matched_count > 0:
        fk_and_identity_matched.write.mode("append").partitionBy(
            "event_date"
        ).parquet(SILVER_PATH)
        fk_and_identity_matched.write.mode("append").partitionBy(
            "event_date"
        ).parquet(GOLD_PATH)

    print(
        f"[Corrections batch {batch_id}] "
        f"total={total_count}  local_pass={local_pass}  "
        f"fk+id_match={matched_count}  ref_dropped={ref_dropped}  "
        f"local_dropped={local_dropped}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Start streaming query
# ═══════════════════════════════════════════════════════════════════════════
query = (
    kafka_stream.writeStream.outputMode("append")
    .foreachBatch(process_corrections_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Corrections streaming started.  Checkpoint: {CHECKPOINT_PATH}")
print(f"Reading from Kafka topic:  {KAFKA_TOPIC}")
print(f"Bronze → {BRONZE_PATH}")
print(f"Silver → {SILVER_PATH}")
print(f"Gold   → {GOLD_PATH}")
print(f"FK ref → {COURT_SILVER_PATH}")

query.awaitTermination()
