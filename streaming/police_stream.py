#!/usr/bin/env python3
"""
EuroCrimePulse — Police Spark Structured Streaming Job

Pipeline:
    Kafka (eurocrimepulse.police)
      → Parse JSON / structural check
      → Bronze  (raw parsed, HDFS Parquet)
      → Clean / normalize
      → Validate (local DQ)
        ├─ FAIL → DROP
        └─ PASS → Silver & Gold

Police has no FK dependencies, so Gold == Silver.

Usage (inside Docker):
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \\
        police_stream.py
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
KAFKA_TOPIC = "eurocrimepulse.police"

HDFS_BASE = os.getenv(
    "EUROCRIMEPULSE_HDFS_BASE", "hdfs://localhost:9000/eurocrimepulse"
)
BRONZE_PATH = f"{HDFS_BASE}/bronze/police"
SILVER_PATH = f"{HDFS_BASE}/silver/police"
GOLD_PATH = f"{HDFS_BASE}/gold/police"
CHECKPOINT_PATH = f"{os.getenv('EUROCRIMEPULSE_CHECKPOINT_BASE', 'hdfs://localhost:9000/tmp/eurocrimepulse/checkpoints')}/police"


# ═══════════════════════════════════════════════════════════════════════════
# Explicit schema (must match generator output)
# ═══════════════════════════════════════════════════════════════════════════
POLICE_SCHEMA = StructType(
    [
        StructField("source_system", StringType(), True),
        StructField("crime_id", StringType(), True),
        StructField("crime_type", StringType(), True),
        StructField("crime_date", StringType(), True),
        StructField(
            "location",
            StructType(
                [
                    StructField("country", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "arresting_officer",
            StructType(
                [
                    StructField("officer_id", StringType(), True),
                    StructField("officer_name", StringType(), True),
                    StructField("badge_number", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "victim",
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
        StructField("narrative", StringType(), True),
        StructField("_data_quality_flag", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)


# ═══════════════════════════════════════════════════════════════════════════
# Reference data
# ═══════════════════════════════════════════════════════════════════════════
SUPPORTED_CRIME_TYPES = [
    "Theft",
    "Burglary",
    "Assault",
    "Fraud",
    "Drug Possession",
    "Robbery",
    "Vandalism",
    "Cybercrime",
    "DUI",
    "Homicide",
    "Money Laundering",
    "Bribery",
    "Smuggling",
]


# ═══════════════════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════════════════
def canonical_national_id(col):
    """
    Normalise a National ID to the canonical 9-digit form.

    Only explicitly supported presentation formats are accepted.
    Unsupported / ambiguous formats → NULL (which causes the record to be
    dropped downstream).

    Supported formats:
        000000010              ^[0-9]{9}$
        ID-00000-0010          ^ID-[0-9]{5}-[0-9]{4}$
        000-000-010            ^[0-9]{3}-[0-9]{3}-[0-9]{3}$
        0000 00010             ^[0-9]{4} [0-9]{5}$
        nid000000010           ^NID[0-9]{9}$   (case-insensitive)
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
    """
    Calculate exact calendar age using birthday-aware logic.
    NOT months_between/12 which is approximate.
    """
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
    Flatten nested JSON, trim strings (empty → NULL), normalise canonical
    IDs and dates, apply blocking validation rules.

    Returns a DataFrame with all business columns plus a
    ``blocking_issue_count`` column (0 = record is valid).
    """
    # --- Flatten ---
    df = raw_df.select(
        "source_system",
        "crime_id",
        "crime_type",
        "crime_date",
        F.col("location.country").alias("country"),
        F.col("location.city").alias("city_raw"),
        F.col("location.latitude").alias("latitude"),
        F.col("location.longitude").alias("longitude"),
        F.col("arresting_officer.officer_id").alias("officer_id"),
        F.col("arresting_officer.officer_name").alias("officer_name"),
        F.col("arresting_officer.badge_number").alias("badge_number"),
        F.col("victim.national_id").alias("victim_national_id_raw"),
        F.col("victim.full_name").alias("victim_full_name"),
        F.col("victim.date_of_birth").alias("victim_dob_raw"),
        F.col("victim.age").alias("victim_age_reported"),
        F.col("victim.gender").alias("victim_gender"),
        F.col("victim.nationality").alias("victim_nationality"),
        "narrative",
        "_data_quality_flag",
        "_ingested_at",
    )

    # --- Trim strings, empty → NULL ---
    str_cols = [
        "source_system", "crime_id", "crime_type", "country", "city_raw",
        "officer_id", "officer_name", "badge_number",
        "victim_national_id_raw", "victim_full_name", "victim_gender",
        "victim_nationality", "narrative", "_data_quality_flag",
        "_ingested_at",
    ]
    for c in str_cols:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))),
        )

    # --- Normalise dates & IDs ---
    df = (
        df.withColumn(
            "crime_timestamp",
            F.to_timestamp("crime_date", "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .withColumn("crime_date_clean", F.to_date("crime_timestamp"))
        .withColumn("_ingested_at_ts", F.to_timestamp("_ingested_at"))
        .withColumn(
            "victim_dob_clean", F.to_date("victim_dob_raw", "yyyy-MM-dd")
        )
        .withColumn(
            "victim_national_id_canonical",
            canonical_national_id(F.col("victim_national_id_raw")),
        )
        .withColumn(
            "victim_age_expected",
            exact_age(F.col("crime_date_clean"), F.col("victim_dob_clean")),
        )
        .withColumn(
            "city", F.coalesce(F.col("city_raw"), F.lit("Unknown"))
        )
    )

    # --- Blocking validation rules ---
    # Every check that, if true, means the record is INVALID and must be
    # dropped.  Cast to int so we can sum them.
    blocking_rules = [
        # Required fields / format
        (
            "crime_id_invalid",
            F.col("crime_id").isNull()
            | ~F.col("crime_id").rlike(r"^CR-[A-F0-9]{32}$"),
        ),
        (
            "crime_type_invalid",
            F.col("crime_type").isNull()
            | ~F.col("crime_type").isin(SUPPORTED_CRIME_TYPES),
        ),
        ("crime_date_invalid", F.col("crime_timestamp").isNull()),
        # National ID
        (
            "victim_national_id_invalid",
            F.col("victim_national_id_canonical").isNull(),
        ),
        # DOB
        ("victim_dob_invalid", F.col("victim_dob_clean").isNull()),
        # Age / DOB mismatch (exact birthday logic)
        (
            "age_mismatch",
            F.col("victim_age_reported").isNotNull()
            & F.col("victim_age_expected").isNotNull()
            & (
                F.col("victim_age_reported")
                != F.col("victim_age_expected")
            ),
        ),
    ]

    for name, expr in blocking_rules:
        df = df.withColumn(name, expr.cast("int"))

    blocking_col_names = [name for name, _ in blocking_rules]
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
        F.coalesce(F.col("crime_date_clean"), F.to_date("_ingested_at_ts")),
    )

    return df


# ═══════════════════════════════════════════════════════════════════════════
# Spark session
# ═══════════════════════════════════════════════════════════════════════════
spark = (
    SparkSession.builder.appName("EuroCrimePulse-Police-Streaming")
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
# foreachBatch processor
# ═══════════════════════════════════════════════════════════════════════════
def process_police_batch(batch_df, batch_id):
    """Process one micro-batch from Kafka."""
    if batch_df.head(1) is None or len(batch_df.head(1)) == 0:
        return

    # --- Parse JSON from Kafka value ---
    parsed = (
        batch_df.select(F.col("value").cast("string").alias("raw_json"))
        .withColumn("data", F.from_json(F.col("raw_json"), POLICE_SCHEMA))
        .filter(F.col("data").isNotNull())          # drop unparseable
        .select("data.*")
    )

    if parsed.head(1) is None or len(parsed.head(1)) == 0:
        return

    # --- Bronze: raw parsed records (no cleaning) ---
    bronze = parsed.withColumn(
        "ingestion_date", F.to_date(F.to_timestamp(F.col("_ingested_at")))
    )
    bronze.write.mode("append").partitionBy("ingestion_date").parquet(
        BRONZE_PATH
    )

    # --- Transform + validate ---
    validated = transform_and_validate(parsed)

    # --- Deduplicate within batch by business key ---
    validated = validated.dropDuplicates(["crime_id"])

    # --- Silver / Gold: only records with zero blocking issues ---
    silver = validated.filter(F.col("blocking_issue_count") == 0)

    total_count = validated.count()
    silver_count = silver.count()
    dropped = total_count - silver_count

    if silver_count > 0:
        silver.write.mode("append").partitionBy("event_date").parquet(
            SILVER_PATH
        )
        # Police has no FK dependencies → Gold == Silver
        silver.write.mode("append").partitionBy("event_date").parquet(
            GOLD_PATH
        )

    print(
        f"[Police batch {batch_id}] "
        f"total={total_count}  silver={silver_count}  dropped={dropped}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Start streaming query
# ═══════════════════════════════════════════════════════════════════════════
query = (
    kafka_stream.writeStream.outputMode("append")
    .foreachBatch(process_police_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Police streaming started.  Checkpoint: {CHECKPOINT_PATH}")
print(f"Reading from Kafka topic:  {KAFKA_TOPIC}")
print(f"Bronze → {BRONZE_PATH}")
print(f"Silver → {SILVER_PATH}")
print(f"Gold   → {GOLD_PATH}")

query.awaitTermination()
