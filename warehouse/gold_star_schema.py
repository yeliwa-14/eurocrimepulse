#!/usr/bin/env python3
"""
EuroCrimePulse — Gold Star Schema Builder (Batch)

Reads the per-source Gold Parquet datasets produced by the streaming
pipeline and builds a dimensional Star Schema suitable for ClickHouse
ingestion and analytical queries.

Architecture:
    HDFS Gold (per-source, streaming)
      → Column Mapping (bridge streaming names → star-schema names)
      → Case-Level Spine (Police ⟕ Court ⟕ Corrections)
      → 12 Dimension Tables + 1 Fact Table
      → HDFS Warehouse (star schema Parquet)

Usage (inside Docker):
    spark-submit /opt/eurocrimepulse/warehouse/gold_star_schema.py

    # Or with custom paths:
    spark-submit /opt/eurocrimepulse/warehouse/gold_star_schema.py \
        --gold-base hdfs://localhost:9000/eurocrimepulse/gold \
        --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse

Column Mapping Note:
    The streaming Gold layer uses certain column names (e.g.
    ``victim_national_id_canonical``, ``crime_date_clean``) that differ
    from the names used in the original ``gold.ipynb`` Colab notebook
    (e.g. ``victim_national_id``, ``crime_date_only``).

    This script applies a compatibility mapping so that the star-schema
    logic works identically to the notebook without modifying the
    streaming code or the analyst's existing sample data.
"""

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType


# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════
def default_service_host():
    try:
        import socket
        socket.getaddrinfo("host.docker.internal", None)
        return "host.docker.internal"
    except Exception:
        return "localhost"


SERVICE_HOST = default_service_host()
HDFS_BASE = os.getenv(
    "EUROCRIMEPULSE_HDFS_BASE",
    "hdfs://localhost:9000/eurocrimepulse",
)
DEFAULT_GOLD_BASE = f"{HDFS_BASE}/gold"
DEFAULT_WAREHOUSE_BASE = f"{HDFS_BASE}/warehouse"


# ═══════════════════════════════════════════════════════════════════════════
# Column mapping — bridge streaming Gold names to star-schema names
# ═══════════════════════════════════════════════════════════════════════════
# Key insight: the streaming pipeline produces validated Gold Parquet
# with specific column names.  The star-schema logic (from gold.ipynb)
# was developed against old CSV exports that used different names.
# Rather than changing either side, we apply a transparent mapping here.

POLICE_COLUMN_MAP = {
    "crime_date_clean": "crime_date_only",
    "victim_dob_clean": "victim_date_of_birth",
    "victim_national_id_canonical": "victim_national_id",
    "victim_age_reported": "victim_age",
}

COURT_COLUMN_MAP = {
    "defendant_national_id_canonical": "defendant_national_id",
    "defendant_age_reported": "defendant_age",
    "defendant_dob_clean": "defendant_DOB_clean",
    "judge_experience": "judge_years_of_experience",
    "verdict": "verdict_outcome",
}

CORRECTIONS_COLUMN_MAP = {
    "defendant_national_id_canonical": "defendant_national_id",
}


def apply_column_map(df: DataFrame, col_map: dict) -> DataFrame:
    """Rename columns per the mapping dict (old_name → new_name)."""
    for old_name, new_name in col_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Shared helpers  (mirrored from gold.ipynb)
# ═══════════════════════════════════════════════════════════════════════════
def calc_age_col(dob_col: str, as_of_col: str):
    """Birthday-aware exact age."""
    dob = F.col(dob_col)
    as_of = F.col(as_of_col)
    as_of_md = F.month(as_of) * 100 + F.dayofmonth(as_of)
    dob_md = F.month(dob) * 100 + F.dayofmonth(dob)
    had_birthday_yet = as_of_md >= dob_md
    raw_years = F.year(as_of) - F.year(dob)
    return F.when(had_birthday_yet, raw_years).otherwise(raw_years - 1)


def make_surrogate_key(
    df: DataFrame, key_col: str, dense: bool = False
) -> DataFrame:
    """Add a surrogate key column."""
    if dense:
        w = Window.orderBy(F.lit(1))
        return df.withColumn(
            key_col, F.row_number().over(w).cast(LongType())
        )
    return df.withColumn(
        key_col, F.monotonically_increasing_id().cast(LongType())
    )


def coalesce_group(
    df: DataFrame, group_col: str, value_cols: list
) -> DataFrame:
    """
    For each group_col, pick the first non-null value of each value_col
    and return one row per group.
    """
    w = (
        Window.partitionBy(group_col)
        .orderBy(F.monotonically_increasing_id())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    out = df
    for c in value_cols:
        out = out.withColumn(
            c, F.first(F.col(c), ignorenulls=True).over(w)
        )

    dedup_w = Window.partitionBy(group_col).orderBy(
        F.monotonically_increasing_id()
    )
    out = (
        out.withColumn("_rn", F.row_number().over(dedup_w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return out.select(group_col, *value_cols)


from collections import Counter

# ═══════════════════════════════════════════════════════════════════════════
# Validation helpers
# ═══════════════════════════════════════════════════════════════════════════

def assert_no_duplicate_columns(df, name="DataFrame"):
    cols = df.columns
    counts = Counter(cols)
    duplicates = [c for c, n in counts.items() if n > 1]
    if duplicates:
        raise ValueError(f"{name} has duplicate columns: {duplicates}")


def validate_spine_schema(spine: DataFrame):
    # Required canonical fields after spine normalization
    required = [
        "crime_id",
        "case_id",
        "crime_date_only",
        "defendant_national_id",
        "victim_national_id",
    ]
    missing = [c for c in required if c not in spine.columns]
    if missing:
        raise ValueError(f"Spine missing required canonical columns: {missing}")
    assert_no_duplicate_columns(spine, "spine")


# ═══════════════════════════════════════════════════════════════════════════
# Resolve helpers
# ═══════════════════════════════════════════════════════════════════════════
def resolve_officer(police: DataFrame) -> DataFrame:
    return coalesce_group(
        police, "officer_id", ["officer_name", "badge_number"]
    )


def resolve_judge(court: DataFrame) -> DataFrame:
    return coalesce_group(
        court,
        "judge_id",
        ["judge_name", "judge_gender", "judge_years_of_experience"],
    )


def resolve_defendant(
    court_side: DataFrame, corrections: DataFrame
) -> DataFrame:
    court_def = (
        coalesce_group(
            court_side.filter(F.col("defendant_national_id").isNotNull()),
            "defendant_national_id",
            [
                "defendant_name",
                "defendant_gender",
                "defendant_nationality",
                "defendant_DOB_clean",
                "defendant_age",
            ],
        ).withColumnRenamed(
            "defendant_name", "defendant_full_name_court"
        )
    )

    corr_def = (
        coalesce_group(
            corrections.filter(
                F.col("defendant_national_id").isNotNull()
            ),
            "defendant_national_id",
            ["defendant_full_name", "record_id"],
        ).withColumnRenamed(
            "defendant_full_name", "defendant_full_name_corr"
        )
    )

    merged = court_def.join(
        corr_def, on="defendant_national_id", how="left"
    )

    merged = merged.withColumn(
        "defendant_full_name",
        F.coalesce("defendant_full_name_court", "defendant_full_name_corr"),
    )

    merged = merged.withColumn(
        "is_cross_imputed_flag",
        F.col("defendant_full_name_court").isNull()
        & F.col("defendant_full_name").isNotNull(),
    )

    return merged.drop(
        "defendant_full_name_court", "defendant_full_name_corr"
    )


# ═══════════════════════════════════════════════════════════════════════════
# Age correction helper
# ═══════════════════════════════════════════════════════════════════════════
def apply_age_correction(
    df: DataFrame,
    dob_col: str,
    asof_col: str,
    provided_age_col: str,
    out_prefix: str,
) -> DataFrame:
    calculated = calc_age_col(dob_col, asof_col)
    provided = F.col(provided_age_col).cast(IntegerType())
    mismatch = (
        calculated.isNotNull()
        & provided.isNotNull()
        & (calculated != provided)
    )
    final_age = F.coalesce(calculated, provided)
    return (
        df.withColumn(f"{out_prefix}_age", final_age).withColumn(
            f"{out_prefix}_is_age_corrected_flag",
            F.coalesce(mismatch, F.lit(False)),
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# Dimension builders
# ═══════════════════════════════════════════════════════════════════════════
def build_dim_date(spine: DataFrame) -> DataFrame:
    # Collect any available date fields into a unified 'full_date' column.
    date_candidates = [
        "crime_date_only",
        "verdict_date_clean",
        "imprisonment_start_date_clean",
        "release_date_clean",
        "imprisonment_start_date_clean_corrections",
        "release_date_clean_corrections",
    ]

    date_dfs = []
    for d in date_candidates:
        if d in spine.columns:
            date_dfs.append(spine.select(F.col(d).alias("full_date")))

    if len(date_dfs) == 0:
        # No date columns present — return empty date dim
        empty = spine.limit(0).select(F.lit(None).alias("full_date"))
        return make_surrogate_key(empty, "date_key", dense=True)

    # Union all found date dataframes
    dates = date_dfs[0]
    for df in date_dfs[1:]:
        dates = dates.unionByName(df)

    dates = dates.filter(F.col("full_date").isNotNull()).distinct()

    dim = make_surrogate_key(dates, "date_key", dense=True)
    dim = (
        dim.withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
    )
    return dim


def build_dim_crime_type(spine: DataFrame) -> DataFrame:
    # Use canonical crime_type from the spine
    dim = (
        spine.select(F.col("crime_type").alias("crime_type"))
        .filter(F.col("crime_type").isNotNull())
        .distinct()
    )
    return make_surrogate_key(dim, "crime_type_key", dense=True)


def build_dim_city(spine: DataFrame) -> DataFrame:
    dim = (
        spine.select(
            F.col("city").alias("city_name"),
            F.col("country").alias("country_name"),
        )
        .filter(F.col("city_name").isNotNull())
        .distinct()
    )
    return make_surrogate_key(dim, "city_key", dense=True)


def build_dim_geolocation(spine: DataFrame) -> DataFrame:
    dim = spine.select("latitude", "longitude").dropna().distinct()
    return make_surrogate_key(dim, "geolocation_key")


def build_dim_court(spine: DataFrame) -> DataFrame:
    dim = (
        spine.select("court_name")
        .filter(F.col("court_name").isNotNull())
        .distinct()
    )
    return make_surrogate_key(dim, "court_key", dense=True)


def build_dim_judge(judge_resolved: DataFrame) -> DataFrame:
    dim = (
        judge_resolved.filter(F.col("judge_id").isNotNull())
        .dropDuplicates(["judge_id"])
    )
    return make_surrogate_key(dim, "judge_key")


def build_dim_officer(officer_resolved: DataFrame) -> DataFrame:
    dim = (
        officer_resolved.filter(F.col("officer_id").isNotNull())
        .dropDuplicates(["officer_id"])
    )
    return make_surrogate_key(dim, "officer_key")


def build_dim_sentence_type(spine: DataFrame) -> DataFrame:
    dim = (
        spine.select("sentence_type")
        .filter(F.col("sentence_type").isNotNull())
        .distinct()
    )
    return make_surrogate_key(dim, "sentence_type_key", dense=True)


def build_dim_verdict_type(spine: DataFrame) -> DataFrame:
    dim = (
        spine.select(
            F.col("verdict_outcome").alias("verdict_type")
        )
        .filter(F.col("verdict_type").isNotNull())
        .distinct()
    )
    return make_surrogate_key(dim, "verdict_type_key", dense=True)


def build_dim_release_reason(spine: DataFrame) -> DataFrame:
    # release_reason may come from Corrections and be namespaced as release_reason_corrections
    if "release_reason" in spine.columns:
        colname = "release_reason"
    elif "release_reason_corrections" in spine.columns:
        colname = "release_reason_corrections"
    else:
        # No release reasons available — return an empty dim
        empty = spine.limit(0).select(F.lit(None).alias("release_reason"))
        return make_surrogate_key(empty, "release_reason_key", dense=True)

    dim = (
        spine.select(F.col(colname).alias("release_reason"))
        .filter(F.col("release_reason").isNotNull())
        .distinct()
    )
    return make_surrogate_key(dim, "release_reason_key", dense=True)


def build_dim_victim(spine: DataFrame) -> DataFrame:
    # Ensure canonical victim fields exist in spine
    expected = [
        "victim_national_id",
        "victim_full_name",
        "victim_date_of_birth",
        "victim_age",
        "victim_gender",
        "victim_nationality",
        "crime_date_only",
    ]
    missing = [c for c in expected if c not in spine.columns]
    if missing:
        raise ValueError(f"build_dim_victim: missing required spine columns: {missing}")

    corrected = apply_age_correction(
        spine,
        dob_col="victim_date_of_birth",
        asof_col="crime_date_only",
        provided_age_col="victim_age",
        out_prefix="victim",
    )

    dim = (
        corrected.select(
            "victim_national_id",
            "victim_full_name",
            "victim_date_of_birth",
            F.col("victim_age").alias("victim_age"),
            "victim_gender",
            "victim_nationality",
            "victim_is_age_corrected_flag",
        )
        .filter(F.col("victim_national_id").isNotNull())
        .dropDuplicates(["victim_national_id"])
    )
    return make_surrogate_key(dim, "victim_key")


def build_dim_defendant(
    spine: DataFrame, corrections_raw: DataFrame
) -> DataFrame:
    # Use canonical defendant fields from spine
    expected = [
        "defendant_national_id",
        "defendant_name",
        "defendant_DOB_clean",
        "defendant_age",
        "crime_date_only",
    ]
    missing = [c for c in expected if c not in spine.columns]
    if missing:
        raise ValueError(f"build_dim_defendant: missing required spine columns: {missing}")

    court_side = spine.select(
        "defendant_national_id",
        "defendant_name",
        "defendant_gender",
        "defendant_nationality",
        "defendant_DOB_clean",
        "defendant_age",
        "crime_date_only",
    )

    resolved = resolve_defendant(court_side, corrections_raw)

    # Ensure crime_date_only is present for age correction; prefer spine mapping
    crime_date_lookup = spine.select(
        "defendant_national_id", "crime_date_only"
    ).dropDuplicates(["defendant_national_id"])

    resolved = resolved.join(
        crime_date_lookup, on="defendant_national_id", how="left"
    )

    corrected = apply_age_correction(
        resolved,
        dob_col="defendant_DOB_clean",
        asof_col="crime_date_only",
        provided_age_col="defendant_age",
        out_prefix="defendant",
    )

    dim = (
        corrected.select(
            "defendant_national_id",
            "record_id",
            F.col("defendant_full_name"),
            F.col("defendant_age").alias("defendant_age"),
            "is_cross_imputed_flag",
            "defendant_is_age_corrected_flag",
        )
        .dropDuplicates(["defendant_national_id"])
    )
    return make_surrogate_key(dim, "defendant_key")


# ═══════════════════════════════════════════════════════════════════════════
# Case-level spine
# ═══════════════════════════════════════════════════════════════════════════
def build_case_spine(
    police: DataFrame,
    court: DataFrame,
    corrections: DataFrame,
) -> DataFrame:
    """
    Build the case-level spine joining Police + Court + Corrections.

    The join is Police INNER Court (on crime_id = linked_crime_id)
    then LEFT Corrections (on case_id = linked_case_id).
    """
    # Robust automatic namespacing: detect overlapping columns between police and court
    # and rename them to side-specific namespaces to prevent duplicate column names.
    police_cols = set(police.columns)
    court_cols = set(court.columns)

    # Define join key names that must be preserved (do not rename them)
    police_keep = {"crime_id", "case_id"}
    court_keep = {"linked_crime_id", "case_id", "linked_case_id"}

    # Columns to never rename even if overlapping (canonical keys)
    never_rename = police_keep | court_keep | {
        "crime_id",
        "linked_crime_id",
    }

    overlaps = (police_cols & court_cols) - never_rename

    # Namespace overlapping columns on both sides to preserve all data and avoid collisions
    for c in sorted(overlaps):
        if c in police.columns:
            police = police.withColumnRenamed(c, f"{c}_police")
        if c in court.columns:
            court = court.withColumnRenamed(c, f"{c}_court")

    # Also namespace any Corrections columns that would collide with the spine after join
    # We'll compute collisions after the join and then rename corrections accordingly before the left join.

    # Police + Court (inner join — only matched records)
    spine = police.join(
        court,
        police["crime_id"] == court["linked_crime_id"],
        how="inner",
    )

    # Build canonical fields using authoritative source rules
    # Business rules: court authoritative for defendant fields; police authoritative for victim fields and crime_date
    canonical_rules = {
        "crime_date_only": ["crime_date_only_police", "crime_date_only_court"],
        "crime_type": ["crime_type_police", "crime_type_court"],
        "source_system": ["source_system_police", "source_system_court"],

        # Victim: police authoritative
        "victim_national_id": ["victim_national_id_police", "victim_national_id_court"],
        "victim_full_name": ["victim_full_name_police", "victim_full_name_court"],
        "victim_date_of_birth": ["victim_date_of_birth_police", "victim_date_of_birth_court"],
        "victim_age": ["victim_age_police", "victim_age_court"],
        "victim_gender": ["victim_gender_police", "victim_gender_court"],
        "victim_nationality": ["victim_nationality_police", "victim_nationality_court"],

        # Defendant: court authoritative
        "defendant_national_id": ["defendant_national_id_court", "defendant_national_id_police"],
        "defendant_name": ["defendant_name_court", "defendant_name_police"],
        "defendant_gender": ["defendant_gender_court", "defendant_gender_police"],
        "defendant_nationality": ["defendant_nationality_court", "defendant_nationality_police"],
        "defendant_DOB_clean": ["defendant_DOB_clean_court", "defendant_DOB_clean_police"],
        "defendant_age": ["defendant_age_court", "defendant_age_police"],
    }

    # Apply canonical coalescing where rules exist, otherwise leave both-sided fields as-is
    for canonical, fallbacks in canonical_rules.items():
        present = [c for c in fallbacks if c in spine.columns]
        if present:
            spine = spine.withColumn(canonical, F.coalesce(*[F.col(c) for c in present]))
        # drop temporary side-specific columns if present
        for c in fallbacks:
            if c in spine.columns:
                spine = spine.drop(c)

    # Now prepare corrections namespacing: detect collisions between corrections and current spine
    corrections_cols = set(corrections.columns)
    spine_cols = set(spine.columns)
    corr_overlaps = corrections_cols & spine_cols

    for c in sorted(corr_overlaps):
        # Do not rename the corrections join key here; we'll namespace selected corrections
        if c == "linked_case_id":
            continue
        if c in corrections.columns:
            corrections = corrections.withColumnRenamed(c, f"{c}_corrections")

    # Explicitly select and namespace only the Corrections columns needed downstream to avoid
    # ambiguous lineage and to keep the spine compact. Preserve linked_case_id as a namespaced
    # column to make the join unambiguous.
    corrections_needed = [
        "linked_case_id",
        "defendant_national_id",
        "defendant_full_name",
        "prison_name",
        "imprisonment_start_date_clean",
        "release_date_clean",
        "release_reason",
        "release_status",
        "record_id",
        "event_date",
        "blocking_issue_count",
        "defendant_id_invalid",
        "defendant_national_id_raw",
    ]

    present_corr = [c for c in corrections_needed if c in corrections.columns]

    # Build a reduced corrections dataframe with explicit _corrections suffixes
    corr_select = []
    for c in present_corr:
        # namespace every selected corrections field
        corr_select.append(F.col(c).alias(f"{c}_corrections"))

    # If nothing is present, create an empty corrections selection with only a dummy join key
    if len(corr_select) == 0:
        # Keep linked_case_id if present in original corrections; otherwise create a literal NULL column
        if "linked_case_id" in corrections.columns:
            corr_df = corrections.select(F.col("linked_case_id").alias("linked_case_id_corrections"))
        else:
            corr_df = corrections.select(F.lit(None).alias("linked_case_id_corrections"))
    else:
        # Ensure linked_case_id is included for the join condition
        if "linked_case_id" in [c for c in present_corr]:
            # Ensure it's part of corr_select (it will be aliased to linked_case_id_corrections)
            corr_df = corrections.select(*corr_select)
        else:
            # If linked_case_id wasn't present but other corrections fields were, we still need a join key column
            if "linked_case_id" in corrections.columns:
                corr_df = corrections.select(F.col("linked_case_id").alias("linked_case_id_corrections"), *corr_select)
            else:
                # No linked_case_id and no join key — select only the namespaced fields (will produce a cross join-like behavior on join condition failure)
                corr_df = corrections.select(*corr_select)

    # Alias DataFrames to avoid ambiguous self-join lineage
    sp = spine.alias("sp")
    corr = corr_df.alias("corr")

    # Build safe join condition using fully-qualified column names
    # Use linked_case_id_corrections on corr side
    if "linked_case_id_corrections" in corr_df.columns:
        join_cond = F.col("sp.case_id") == F.col("corr.linked_case_id_corrections")
    elif "linked_case_id" in corrections.columns:
        # fallback if we didn't alias linked_case_id for some reason
        join_cond = F.col("sp.case_id") == F.col("corr.linked_case_id")
    else:
        # No linked_case_id available — perform left join but this is likely an upstream data issue
        join_cond = F.lit(False)

    joined = sp.join(corr, join_cond, how="left")

    # Explicit final projection: preserve canonical spine columns (from sp) and bring in namespaced corrections fields (from corr)
    spine_cols_after = [c for c in spine.columns]
    corr_cols_after = [c for c in corr_df.columns if c != "linked_case_id_corrections"]

    final_select = [F.col(f"sp.{c}").alias(c) for c in spine_cols_after]
    for c in corr_cols_after:
        final_select.append(F.col(f"corr.{c}").alias(c))

    spine_final = joined.select(*final_select)

    # Final check: ensure unique column names and required canonical columns exist
    assert_no_duplicate_columns(spine_final, "spine")
    validate_spine_schema(spine_final)

    police_count = police.count()
    spine_count = spine_final.count()

    print(
        f"[Spine] Police rows: {police_count}, "
        f"After Police⟕Court join + Corrections left-join: {spine_count}"
    )

    if "crime_date_only" not in spine_final.columns:
        raise ValueError("crime_date_only missing after spine construction")

    return spine_final


# ═══════════════════════════════════════════════════════════════════════════
# Fact table builder
# ═══════════════════════════════════════════════════════════════════════════
def build_fact_crime_case(
    spine: DataFrame, dims: dict
) -> DataFrame:
    f = spine

    def join_date_role(base, date_col, key_alias, out_key):
        # Resolve the actual date column name in the base (check for corrections-suffixed)
        actual_date_col = None
        if date_col in base.columns:
            actual_date_col = date_col
        elif f"{date_col}_corrections" in base.columns:
            actual_date_col = f"{date_col}_corrections"

        if actual_date_col is None:
            # No date to join for this role
            return base

        d = dims["date"].select("full_date", "date_key").alias(key_alias)
        joined = base.join(
            d,
            F.col(actual_date_col) == F.col(f"{key_alias}.full_date"),
            "left",
        )
        return (
            joined.withColumn(out_key, F.col(f"{key_alias}.date_key"))
            .drop(F.col(f"{key_alias}.date_key"))
            .drop(F.col(f"{key_alias}.full_date"))
        )

    # Date roles
    f = join_date_role(f, "crime_date_only", "d_crime", "crime_date_key")
    f = join_date_role(f, "verdict_date_clean", "d_verdict", "verdict_date_key")
    f = join_date_role(
        f, "imprisonment_start_date_clean", "d_impstart", "imprisonment_start_date_key"
    )
    f = join_date_role(f, "release_date_clean", "d_release", "release_date_key")

    # Dimension joins
    f = f.join(
        dims["crime_type"],
        f["crime_type"] == dims["crime_type"]["crime_type"],
        "left",
    ).drop(dims["crime_type"]["crime_type"])

    f = f.join(
        dims["city"],
        (f["city"] == dims["city"]["city_name"]) & (f["country"] == dims["city"]["country_name"]),
        "left",
    )

    f = f.join(dims["geolocation"], on=["latitude", "longitude"], how="left")

    f = f.join(dims["court"], on="court_name", how="left")

    f = f.join(dims["judge"].select("judge_key", "judge_id"), on="judge_id", how="left")

    f = f.join(dims["officer"].select("officer_key", "officer_id"), on="officer_id", how="left")

    f = f.join(dims["victim"].select("victim_key", "victim_national_id"), on="victim_national_id", how="left")

    f = f.join(dims["sentence_type"], on="sentence_type", how="left")

    f = f.join(
        dims["verdict_type"],
        f["verdict_outcome"] == dims["verdict_type"]["verdict_type"],
        "left",
    ).drop(dims["verdict_type"]["verdict_type"])

    f = f.join(
        dims["defendant"].select("defendant_key", "defendant_national_id"),
        on="defendant_national_id",
        how="left",
    )

    # Release reason dimension: may be namespaced from Corrections
    if "release_reason" in f.columns:
        f = f.join(dims["release_reason"], on="release_reason", how="left")
    elif "release_reason_corrections" in f.columns:
        f = f.join(dims["release_reason"], f["release_reason_corrections"] == dims["release_reason"]["release_reason"], how="left").drop(dims["release_reason"]["release_reason"])

    # Derived measures: use corrections-suffixed date columns if canonical absent
    def pick_date(col_base):
        if col_base in f.columns:
            return col_base
        if f"{col_base}_corrections" in f.columns:
            return f"{col_base}_corrections"
        return None

    imp_col = pick_date("imprisonment_start_date_clean")
    rel_col = pick_date("release_date_clean")

    if imp_col and rel_col:
        raw_duration = F.datediff(F.col(rel_col), F.col(imp_col))
    else:
        raw_duration = F.lit(None)

    f = f.withColumn("is_duration_invalid_flag", (raw_duration < 0))
    f = f.withColumn(
        "sentence_duration",
        F.when(raw_duration < 0, F.lit(None)).otherwise(raw_duration),
    )

    # Data quality flags
    f = f.withColumn(
        "is_missing_location_flag",
        F.col("latitude").isNull() | F.col("longitude").isNull(),
    )
    f = f.withColumn("is_missing_verdict_flag", F.col("verdict_date_key").isNull())

    # has_corrections_record_flag: either original record_id or namespaced record_id_corrections
    record_id_cols = [c for c in ["record_id", "record_id_corrections"] if c in f.columns]
    if len(record_id_cols) == 0:
        f = f.withColumn("has_corrections_record_flag", F.lit(False))
    else:
        # check any of the available record_id columns
        cond = None
        for c in record_id_cols:
            if cond is None:
                cond = F.col(c).isNotNull()
            else:
                cond = cond | F.col(c).isNotNull()
        f = f.withColumn("has_corrections_record_flag", cond)

    f = f.withColumn(
        "is_still_incarcerated_flag",
        F.col("imprisonment_start_date_key").isNotNull() & F.col("release_date_key").isNull(),
    )

    # Fact surrogate key
    f = make_surrogate_key(f, "crime_case_key")

    fact_cols = [
        "crime_case_key",
        "crime_id",
        "case_id",
        "crime_type_key",
        "city_key",
        "geolocation_key",
        "court_key",
        "judge_key",
        "officer_key",
        "victim_key",
        "sentence_type_key",
        "verdict_type_key",
        "defendant_key",
        "crime_date_key",
        "verdict_date_key",
        "imprisonment_start_date_key",
        "release_date_key",
        "release_reason_key",
        "sentence_duration",
        "is_missing_location_flag",
        "is_missing_verdict_flag",
        "has_corrections_record_flag",
        "is_still_incarcerated_flag",
        "is_duration_invalid_flag",
    ]

    return f.select(*fact_cols)


# ═══════════════════════════════════════════════════════════════════════════
# Main pipeline
# ═══════════════════════════════════════════════════════════════════════════
def run(gold_base: str, warehouse_base: str):
    spark = (
        SparkSession.builder.appName("EuroCrimePulse-GoldStarSchema")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Extract: read Gold Parquet from HDFS ──
    print("=" * 60)
    print("  EuroCrimePulse — Gold Star Schema Builder")
    print("=" * 60)
    print(f"  Gold base:      {gold_base}")
    print(f"  Warehouse base: {warehouse_base}")
    print()

    try:
        police_raw = spark.read.parquet(f"{gold_base}/police")
        court_raw = spark.read.parquet(f"{gold_base}/court")
    except Exception as e:
        print(f"ERROR: Cannot read Gold data: {e}")
        print("Ensure the streaming pipeline has produced Gold output.")
        sys.exit(1)

    # Corrections may be empty if no custodial verdicts passed
    try:
        corrections_raw = spark.read.parquet(
            f"{gold_base}/corrections"
        )
    except Exception:
        print("WARNING: No corrections Gold data found. Continuing.")
        from pyspark.sql.types import (
            StringType,
            StructField,
            StructType,
        )

        corrections_raw = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("record_id", StringType()),
                    StructField("linked_case_id", StringType()),
                    StructField(
                        "defendant_national_id_canonical", StringType()
                    ),
                    StructField("defendant_full_name", StringType()),
                    StructField("prison_name", StringType()),
                    StructField(
                        "imprisonment_start_date_clean", StringType()
                    ),
                    StructField("release_date_clean", StringType()),
                    StructField("release_reason", StringType()),
                    StructField("release_status", StringType()),
                ]
            ),
        )

    p_count = police_raw.count()
    ct_count = court_raw.count()
    cr_count = corrections_raw.count()
    print(f"  Police Gold:      {p_count:>8,} rows")
    print(f"  Court Gold:       {ct_count:>8,} rows")
    print(f"  Corrections Gold: {cr_count:>8,} rows")
    print()

    # ── Apply column mapping ──
    print("Applying column mapping...")
    police = apply_column_map(police_raw, POLICE_COLUMN_MAP)
    court = apply_column_map(court_raw, COURT_COLUMN_MAP)
    corrections = apply_column_map(
        corrections_raw, CORRECTIONS_COLUMN_MAP
    )

    # ── Ensure date columns are date type ──
    police = (
        police.withColumn(
            "crime_date_only",
            F.to_date("crime_date_only", "yyyy-MM-dd"),
        ).withColumn(
            "victim_date_of_birth",
            F.to_date("victim_date_of_birth", "yyyy-MM-dd"),
        )
    )

    court = (
        court.withColumn(
            "verdict_date_clean",
            F.to_date("verdict_date_clean", "yyyy-MM-dd"),
        ).withColumn(
            "defendant_DOB_clean",
            F.to_date("defendant_DOB_clean", "yyyy-MM-dd"),
        )
    )

    corrections = (
        corrections.withColumn(
            "imprisonment_start_date_clean",
            F.to_date(
                "imprisonment_start_date_clean", "yyyy-MM-dd"
            ),
        ).withColumn(
            "release_date_clean",
            F.to_date("release_date_clean", "yyyy-MM-dd"),
        )
    )

    # ── Build case-level spine ──
    print("Building case-level spine...")
    spine = build_case_spine(police, court, corrections)
    spine_count = spine.count()
    print(f"  Spine rows: {spine_count:>8,}")
    print()

    if spine_count == 0:
        print("ERROR: Spine is empty — no matching records.")
        print("This usually means Police or Court Gold is empty.")
        sys.exit(1)

    # Validate spine schema to fail early with clear message
    try:
        validate_spine_schema(spine)
    except Exception as exc:
        print(f"Spine schema validation failed: {exc}")
        raise

    # ── Resolve entities ──
    print("Resolving entities...")
    officer_resolved = resolve_officer(police)
    judge_resolved = resolve_judge(court)

    # ── Build dimensions ──
    print("Building dimension tables...")
    dims = {}
    dims["crime_type"] = build_dim_crime_type(spine)
    dims["city"] = build_dim_city(spine)
    dims["geolocation"] = build_dim_geolocation(spine)
    dims["court"] = build_dim_court(spine)
    dims["judge"] = build_dim_judge(judge_resolved)
    dims["officer"] = build_dim_officer(officer_resolved)
    dims["sentence_type"] = build_dim_sentence_type(spine)
    dims["verdict_type"] = build_dim_verdict_type(spine)
    dims["release_reason"] = build_dim_release_reason(spine)
    dims["victim"] = build_dim_victim(spine)
    dims["defendant"] = build_dim_defendant(spine, corrections)
    dims["date"] = build_dim_date(spine)

    print("\n  Dimension row counts:")
    for name, d in dims.items():
        count = d.count()
        print(f"    dim_{name:20s} {count:>8,} rows")

    # ── Build fact table ──
    print("\nBuilding fact table...")
    fact = build_fact_crime_case(spine, dims)
    fact_count = fact.count()
    print(f"  fact_crime_case: {fact_count:>8,} rows")

    # ── Write to warehouse ──
    print(f"\nWriting star schema to {warehouse_base}/...")

    for name, dim_df in dims.items():
        path = f"{warehouse_base}/dim_{name}"
        dim_df.write.mode("overwrite").parquet(path)
        print(f"  ✓ dim_{name}")

    fact_path = f"{warehouse_base}/fact_crime_case"
    fact.write.mode("overwrite").parquet(fact_path)
    print(f"  ✓ fact_crime_case")

    # ── Summary ──
    print()
    print("=" * 60)
    print("  Star Schema build complete")
    print("=" * 60)
    print(f"  Total dimensions: {len(dims)}")
    print(f"  Fact rows:        {fact_count:>8,}")
    print(f"  Output:           {warehouse_base}/")
    print("=" * 60)

    spark.stop()


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════
def parse_args():
    p = argparse.ArgumentParser(
        description="Build EuroCrimePulse Star Schema from Gold Parquet."
    )
    p.add_argument(
        "--gold-base",
        default=DEFAULT_GOLD_BASE,
        help=f"HDFS path to Gold Parquet (default: {DEFAULT_GOLD_BASE})",
    )
    p.add_argument(
        "--warehouse-base",
        default=DEFAULT_WAREHOUSE_BASE,
        help=f"HDFS output path for Star Schema "
        f"(default: {DEFAULT_WAREHOUSE_BASE})",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.gold_base, args.warehouse_base)
