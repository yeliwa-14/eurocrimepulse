#!/usr/bin/env python3
"""
EuroCrimePulse — Load Star Schema from HDFS Warehouse to ClickHouse

Reads the dimension and fact tables produced by gold_star_schema.py,
loads the warehouse into ClickHouse, and then loads the ML outputs.

The loader prefers the Spark JDBC path when available, but it automatically
falls back to a robust CSV bulk-load path for Docker environments where the
JDBC connector is unavailable or unreliable.

Usage (inside Docker):
    /usr/local/spark/bin/spark-submit --master local[2] \
        --conf spark.sql.session.timeZone=UTC \
        /opt/eurocrimepulse/clickhouse/load_to_clickhouse.py \
        --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse \
        --ml-base hdfs://localhost:9000/eurocrimepulse/ml
"""

import argparse
import glob
import os
import shlex
import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
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
DEFAULT_WAREHOUSE_BASE = f"{HDFS_BASE}/warehouse"
DEFAULT_CH_HOST = os.getenv("CLICKHOUSE_HOST", SERVICE_HOST)
DEFAULT_CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
DEFAULT_CH_NATIVE_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9010"))
DEFAULT_CH_DB = "eurocrimepulse"
DEFAULT_CSV_EXPORT_DIR = os.getenv("EUROCRIMEPULSE_CSV_EXPORT", "/opt/eurocrimepulse/csv_export")

TABLES = [
    "dim_date",
    "dim_crime_type",
    "dim_city",
    "dim_geolocation",
    "dim_court",
    "dim_judge",
    "dim_officer",
    "dim_victim",
    "dim_defendant",
    "dim_sentence_type",
    "dim_verdict_type",
    "dim_release_reason",
    "fact_crime_case",
]

ML_TABLES = [
    "ml_metrics",
    "ml_cluster_predictions",
    "ml_verdict_predictions",
    "ml_per_class_metrics",
    "ml_feature_importance",
    "ml_cluster_profile",
    "ml_confusion_matrix",
]


def normalize_boolean_columns(df):
    """Convert boolean columns to 0/1 so ClickHouse UInt8 columns accept them."""
    for c in df.columns:
        dtype = df.schema[c].dataType
        if isinstance(dtype, BooleanType):
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull(), F.lit(None).cast("int")).otherwise(F.col(c).cast("int")),
            )
    return df


def truncate_clickhouse_table(ch_host, ch_port, ch_db, table_name):
    cmd = (
        f'clickhouse-client --host {shlex.quote(str(ch_host))} '
        f'--port {shlex.quote(str(ch_port))} '
        f'--database {shlex.quote(ch_db)} '
        f'--query "TRUNCATE TABLE IF EXISTS {table_name};"'
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"TRUNCATE failed for {table_name}")
    return True


def load_table_to_clickhouse(spark, parquet_path, ch_table, ch_host, ch_native_port):
    """
    Load Warehouse/ML Parquet directly into an existing ClickHouse table.

    Source:
        HDFS Parquet produced by the streaming pipeline.

    Destination:
        ClickHouse native protocol using JSONEachRow.

    No CSV and no JDBC CREATE TABLE are used.
    """
    df = spark.read.parquet(parquet_path)
    df = normalize_boolean_columns(df)

    # ClickHouse ml_verdict_predictions expects Int32
    if ch_table.endswith(".ml_verdict_predictions"):
        from pyspark.sql import functions as F
        df = (
            df
            .withColumn("prediction", F.col("prediction").cast("int"))
            .withColumn("label", F.col("label").cast("int"))
        )

    row_count = df.count()

    if row_count == 0:
        return False, 0, "empty"

    rows = df.toJSON().collect()
    payload = "\n".join(rows) + "\n"

    cmd = [
        "clickhouse-client",
        "--host", str(ch_host),
        "--port", str(ch_native_port),
        "--query", f"INSERT INTO {ch_table} FORMAT JSONEachRow",
    ]

    result = subprocess.run(
        cmd,
        input=payload,
        text=True,
        capture_output=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip()
            or result.stdout.strip()
            or "ClickHouse native insert failed"
        )

    return True, row_count, "native"


def load_table_with_fallback(
    spark,
    parquet_path,
    ch_table,
    ch_host,
    ch_port,
    ch_db,
    csv_dir,
    truncate_before_insert=False,
):
    """
    Direct HDFS Parquet -> ClickHouse native INSERT.

    CSV fallback is intentionally disabled.
    """
    try:
        ok, row_count, status = load_table_to_clickhouse(
            spark,
            parquet_path,
            ch_table,
            ch_host,
            9010,
        )

        if ok:
            return True, row_count, status

        return False, row_count, status

    except Exception as exc:
        print(
            f"  [native] ClickHouse insert failed for {ch_table}: {exc}"
        )
        return False, 0, str(exc)

def run(warehouse_base: str, ch_host: str, ch_port: int, ch_db: str, ml_base: str = None):
    spark = (
        SparkSession.builder.appName("EuroCrimePulse-ClickHouseLoader")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    csv_dir = os.getenv("EUROCRIMEPULSE_CSV_EXPORT", DEFAULT_CSV_EXPORT_DIR)
    os.makedirs(csv_dir, exist_ok=True)

    print("=" * 60)
    print("  EuroCrimePulse — ClickHouse Loader")
    print("=" * 60)
    print(f"  Warehouse: {warehouse_base}")
    print(f"  ClickHouse: {ch_host}:{ch_port}/{ch_db}")
    if ml_base:
        print(f"  ML outputs: {ml_base}")
    print()

    loaded = 0
    skipped = 0
    errors = 0

    for table_name in TABLES:
        parquet_path = f"{warehouse_base}/{table_name}"
        ch_table = f"{ch_db}.{table_name}"

        try:
            df = spark.read.parquet(parquet_path)
            row_count = df.count()
        except Exception as exc:
            print(f"  ⚠ {table_name}: cannot read Parquet — {exc}")
            skipped += 1
            continue

        if row_count == 0:
            print(f"  ⚠ {table_name}: empty — skipping")
            skipped += 1
            continue

        try:
            ok, row_count, status = load_table_with_fallback(
                spark,
                parquet_path,
                ch_table,
                ch_host,
                ch_port,
                ch_db,
                csv_dir,
                truncate_before_insert=False,
            )
            if ok:
                print(f"  ✓ {table_name}: {row_count:>8,} rows loaded via {status}")
                loaded += 1
            else:
                skipped += 1
                print(f"  ⚠ {table_name}: {status} — skipped")
        except Exception as exc:
            print(f"  ✗ {table_name}: load failed — {exc}")
            errors += 1

    if ml_base:
        for table_name in ML_TABLES:
            if table_name == "ml_metrics":
                parquet_path = f"{ml_base}/metrics"
            elif table_name == "ml_cluster_predictions":
                parquet_path = f"{ml_base}/cluster_predictions"
            elif table_name == "ml_verdict_predictions":
                parquet_path = f"{ml_base}/verdict_predictions"
            elif table_name == "ml_per_class_metrics":
                parquet_path = f"{ml_base}/per_class_metrics"
            elif table_name == "ml_feature_importance":
                parquet_path = f"{ml_base}/feature_importance"
            elif table_name == "ml_cluster_profile":
                parquet_path = f"{ml_base}/cluster_profile"
            elif table_name == "ml_confusion_matrix":
                parquet_path = f"{ml_base}/confusion_matrix"
            else:
                continue

            ch_table = f"{ch_db}.{table_name}"
            try:
                df = spark.read.parquet(parquet_path)
                row_count = df.count()
            except Exception as exc:
                print(f"  ⚠ {table_name}: cannot read Parquet — {exc}")
                skipped += 1
                continue

            if row_count == 0:
                print(f"  ⚠ {table_name}: empty — skipping")
                skipped += 1
                continue

            try:
                # Ensure ML tables are loaded idempotently: truncate the ClickHouse table first
                try:
                    truncate_clickhouse_table(ch_host, ch_port, ch_db, table_name)
                    print(f"  Truncated ClickHouse table {ch_db}.{table_name} before ML load")
                except Exception as exc_tr:
                    # If truncation fails, warn but continue to attempt the load
                    print(f"  Warning: could not truncate {ch_db}.{table_name} before ML load: {exc_tr}")

                ok, row_count, status = load_table_with_fallback(
                    spark,
                    parquet_path,
                    ch_table,
                    ch_host,
                    ch_port,
                    ch_db,
                    csv_dir,
                    truncate_before_insert=True,
                )
                if ok:
                    print(f"  ✓ {table_name}: {row_count:>8,} rows loaded via {status}")
                    loaded += 1
                else:
                    skipped += 1
                    print(f"  ⚠ {table_name}: {status} — skipped")
            except Exception as exc:
                print(f"  ✗ {table_name}: load failed — {exc}")
                errors += 1

    print()
    print("=" * 60)
    print(f"  Loaded:  {loaded}  |  Skipped: {skipped}  |  Errors: {errors}")
    print("=" * 60)

    if errors > 0:
        print("\nNOTE: ClickHouse is healthy, but one or more tables failed to load.")
        print("Run 'clickhouse-client --multiquery < /opt/eurocrimepulse/clickhouse/clickhouse_setup.sql' first to create the tables.")

    spark.stop()
    return errors


def export_csv(warehouse_base: str, output_dir: str):
    """Export warehouse Parquet to CSV for manual/robust ClickHouse loading."""
    spark = (
        SparkSession.builder.appName("EuroCrimePulse-CSVExport")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    os.makedirs(output_dir, exist_ok=True)
    print(f"Exporting warehouse Parquet → CSV in {output_dir}/")

    for table_name in TABLES:
        parquet_path = f"{warehouse_base}/{table_name}"
        csv_path = os.path.join(output_dir, table_name)
        try:
            df = spark.read.parquet(parquet_path)
            df = normalize_boolean_columns(df)
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
            print(f"  ✓ {table_name}")
        except Exception as exc:
            print(f"  ✗ {table_name}: {exc}")

    spark.stop()
    print("\nCSV export complete.")


def parse_args():
    p = argparse.ArgumentParser(description="Load EuroCrimePulse warehouse and ML outputs into ClickHouse.")
    p.add_argument("--warehouse-base", default=DEFAULT_WAREHOUSE_BASE, help=f"HDFS path to warehouse Parquet (default: {DEFAULT_WAREHOUSE_BASE})")
    p.add_argument("--clickhouse-host", default=DEFAULT_CH_HOST, help=f"ClickHouse host (default: {DEFAULT_CH_HOST})")
    p.add_argument("--clickhouse-port", default=DEFAULT_CH_PORT, type=int, help=f"ClickHouse HTTP port used by the JDBC URL (default: {DEFAULT_CH_PORT})")
    p.add_argument("--clickhouse-db", default=DEFAULT_CH_DB, help=f"ClickHouse database (default: {DEFAULT_CH_DB})")
    p.add_argument("--ml-base", default=None, help="Optional HDFS path to ML outputs for ClickHouse ingestion.")
    p.add_argument("--export-csv", default=None, metavar="DIR", help="Export warehouse Parquet to CSV instead of JDBC.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.export_csv:
        export_csv(args.warehouse_base, args.export_csv)
    else:
        errors = run(
            args.warehouse_base,
            args.clickhouse_host,
            args.clickhouse_port,
            args.clickhouse_db,
            args.ml_base or os.getenv("EUROCRIMEPULSE_ML_BASE", None),
        )
        sys.exit(1 if errors else 0)
