#!/usr/bin/env python3
"""
EuroCrimePulse -- Export ML Metrics to JSON

Reads the ML metrics Parquet written by ml_clustering.py from HDFS
and writes them to a local JSON file that Streamlit can read.

Usage (inside Docker, after running the ML pipeline):
  /usr/local/spark/bin/spark-submit --master local[2] \
      /opt/eurocrimepulse/ml/ml_clustering.py \
      --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse \
      --ml-output hdfs://localhost:9000/eurocrimepulse/ml
  python3 /opt/eurocrimepulse/ml/export_ml_metrics.py

Output: <final_dir>/ml_metrics_export.json
  (override with ML_METRICS_JSON env variable)
"""

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_BASE = os.getenv("EUROCRIMEPULSE_HDFS_BASE", "hdfs://localhost:9000/eurocrimepulse")
ML_BASE   = f"{HDFS_BASE}/ml"
FINAL_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE  = os.getenv("ML_METRICS_JSON", os.path.join(FINAL_DIR, "ml_metrics_export.json"))


def run():
    spark = (
        SparkSession.builder.appName("EuroCrimePulse-MetricsExport")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading ML metrics from: {ML_BASE}/metrics")
    print(f"Output file: {OUT_FILE}")
    print()

    # Read metrics Parquet
    try:
        metrics_df = spark.read.parquet(f"{ML_BASE}/metrics")
        rows = metrics_df.collect()
    except Exception as exc:
        print(f"ERROR: cannot read metrics -- {exc}", file=sys.stderr)
        print(
            "Run the ML pipeline first:\n"
            "  /usr/local/spark/bin/spark-submit --master local[2] \\\n                /opt/eurocrimepulse/ml/ml_clustering.py \\\n                --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse \\\n                --ml-output hdfs://localhost:9000/eurocrimepulse/ml"
        )
        sys.exit(1)

    if not rows:
        print("ERROR: metrics Parquet is empty.", file=sys.stderr)
        sys.exit(1)

    # Convert Row objects to plain JSON-serialisable dicts
    metrics_list = []
    for row in rows:
        d = row.asDict()
        clean = {}
        for k, v in d.items():
            if v is None:
                clean[k] = None
            elif isinstance(v, float):
                clean[k] = round(v, 6)
            elif isinstance(v, int):
                clean[k] = int(v)
            else:
                clean[k] = str(v)
        metrics_list.append(clean)

    print('Metrics:')
    for m in metrics_list:
        print(f"  task={m.get('task')}  status={m.get('status')}")
    print()

    # Read cluster summary stats
    cluster_stats = []
    try:
        cluster_df = spark.read.parquet(f"{ML_BASE}/cluster_predictions")
        summary = (
            cluster_df.groupBy("cluster_id")
            .agg(
                F.count("*").alias("count"),
                F.round(F.avg("sentence_duration_safe"), 2).alias("avg_sentence"),
                F.round(F.avg("has_corrections_record_flag"), 4).alias("corrections_rate"),
                F.round(F.avg("is_still_incarcerated_flag"), 4).alias("incarceration_rate"),
            )
            .orderBy("cluster_id")
            .collect()
        )
        cluster_stats = [r.asDict() for r in summary]
        print(f'Cluster summary: {len(cluster_stats)} clusters')
        for cs in cluster_stats:
            print(f"  cluster {cs['cluster_id']}: n={cs['count']}  "
                  f"avg_sentence={cs['avg_sentence']}")
    except Exception as exc:
        print(f"  (cluster predictions not available: {exc})")

    # Write output JSON
    output = {
        "metrics": metrics_list,
        "cluster_stats": cluster_stats,
    }
    out_dir = os.path.dirname(OUT_FILE)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as fout:
        json.dump(output, fout, indent=2)
    print(f"\nMetrics exported to: {OUT_FILE}")

    spark.stop()


if __name__ == "__main__":
    run()
