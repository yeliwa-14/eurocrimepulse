#!/usr/bin/env python3
"""
EuroCrimePulse -- Spark ML Pipeline

Reads the HDFS warehouse and runs:
  1. KMeans clustering of crime cases (unsupervised)
  2. Random Forest classifier for verdict outcome (supervised)

Outputs written to HDFS:
  {ml_output}/cluster_predictions
  {ml_output}/verdict_predictions
  {ml_output}/metrics
  {ml_output}/per_class_metrics
  {ml_output}/feature_importance
  {ml_output}/cluster_profile
  {ml_output}/confusion_matrix

Usage:
  /usr/local/spark/bin/spark-submit --master local[2] \
      --conf spark.sql.session.timeZone=UTC \
      /opt/eurocrimepulse/ml/ml_clustering.py \
      --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse \
      --ml-output hdfs://localhost:9000/eurocrimepulse/ml
"""

import argparse
import json
import os
import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def default_service_host():
    try:
        import socket
        socket.getaddrinfo("host.docker.internal", None)
        return "host.docker.internal"
    except Exception:
        return "localhost"


SERVICE_HOST = default_service_host()
HDFS_BASE = os.getenv("EUROCRIMEPULSE_HDFS_BASE", "hdfs://localhost:9000/eurocrimepulse")
DEFAULT_WAREHOUSE_BASE = f"{HDFS_BASE}/warehouse"
DEFAULT_ML_OUTPUT = f"{HDFS_BASE}/ml"

KMEANS_K = 5
KMEANS_SEED = 42
KMEANS_MAX_ITER = 20
RF_NUM_TREES = 200
RF_MAX_DEPTH = 8
RF_SEED = 42
TRAIN_SPLIT = 0.8
TEST_SPLIT = 0.2


def safe_read(spark, path, name):
    try:
        df = spark.read.parquet(path)
        cnt = df.count()
        print(f"  ok {name}: {cnt} rows")
        return df
    except Exception as exc:
        print(f"  FAIL {name}: cannot read {path} -- {exc}", file=sys.stderr)
        sys.exit(1)


def safe_read_opt(spark, path, name):
    try:
        df = spark.read.parquet(path)
        cnt = df.count()
        print(f"  ok {name}: {cnt} rows")
        return df if cnt > 0 else None
    except Exception as exc:
        print(f"  skip {name}: {exc}")
        return None


def sanitize_numeric_columns(df, cols, value=0.0):
    for c in cols:
        if c not in df.columns:
            continue
        df = df.withColumn(
            c,
            F.when(
                F.col(c).cast("double").isNull()
                | F.isnan(F.col(c).cast("double"))
                | ((F.col(c).cast("double") == float("inf")) | (F.col(c).cast("double") == float("-inf"))),
                F.lit(value),
            ).otherwise(F.coalesce(F.col(c).cast("double"), F.lit(value))),
        )
    return df


def ensure_flag_numeric(df, flag_cols):
    for c in flag_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    F.col(c).cast("double").isNull()
                    | F.isnan(F.col(c).cast("double"))
                    | ((F.col(c).cast("double") == float("inf")) | (F.col(c).cast("double") == float("-inf"))),
                    F.lit(0.0),
                ).otherwise(F.coalesce(F.col(c).cast("double"), F.lit(0.0))),
            )
    return df


def stratified_split(df, label_col, train_fraction=0.8, seed=42):
    counts = df.groupBy(label_col).count().orderBy(label_col).collect()
    if not counts:
        raise ValueError(f"No rows available for stratified split on {label_col}")

    train_parts = []
    test_parts = []
    for row in counts:
        label_val = row[label_col]
        class_df = df.filter(F.col(label_col) == label_val).orderBy("crime_case_key")
        total = class_df.count()
        if total <= 1:
            train_parts.append(class_df.limit(1))
            test_parts.append(class_df.limit(0))
            continue
        train_n = max(1, int(round(total * train_fraction)))
        if train_n >= total:
            train_n = total - 1
        if train_n <= 0:
            train_n = 1

        rn = Window.partitionBy(label_col).orderBy("crime_case_key")
        class_df = class_df.withColumn("__row_num__", F.row_number().over(rn))
        train_part = class_df.filter(F.col("__row_num__") <= train_n).drop("__row_num__")
        test_part = class_df.filter(F.col("__row_num__") > train_n).drop("__row_num__")
        train_parts.append(train_part)
        test_parts.append(test_part)

    train_df = train_parts[0]
    for part in train_parts[1:]:
        train_df = train_df.unionByName(part)

    test_df = test_parts[0]
    for part in test_parts[1:]:
        test_df = test_df.unionByName(part)
    return train_df, test_df


def build_cluster_model(base, k_value):
    feature_cols = [
        "sentence_duration_safe",
        "has_corrections_record_flag",
        "is_still_incarcerated_flag",
        "is_duration_invalid_flag",
        "is_missing_verdict_flag",
        "crime_year",
        "crime_quarter",
        "crime_month",
    ]
    if "crime_type" in base.columns:
        feature_cols.append("crime_type_idx")
        indexer = StringIndexer(inputCol="crime_type", outputCol="crime_type_idx", handleInvalid="keep")
    else:
        indexer = None
    asm = VectorAssembler(inputCols=feature_cols, outputCol="cluster_features", handleInvalid="keep")
    if indexer is not None:
        stages = [indexer, asm]
    else:
        stages = [asm]
    kmeans = KMeans(featuresCol="cluster_features", predictionCol="cluster_id", k=k_value, seed=KMEANS_SEED, maxIter=KMEANS_MAX_ITER)
    pipeline = Pipeline(stages=stages + [kmeans])
    return pipeline


def safe_metrics_table(df):
    return df.select(
        F.col("task"),
        F.col("status"),
        F.col("k").cast("int"),
        F.col("silhouette").cast("double"),
        F.col("total_rows").cast("long"),
        F.col("train_rows").cast("long"),
        F.col("test_rows").cast("long"),
        F.col("accuracy").cast("double"),
        F.col("weighted_precision").cast("double"),
        F.col("weighted_recall").cast("double"),
        F.col("weighted_f1").cast("double"),
        F.col("macro_f1").cast("double"),
        F.col("num_trees").cast("int"),
        F.col("max_depth").cast("int"),
        F.col("reason"),
    )


def run(warehouse_base, ml_output):
    spark = (
        SparkSession.builder.appName("EuroCrimePulse-ML")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("EuroCrimePulse ? HDFS ML pipeline")
    print("=" * 70)
    print(f"Warehouse: {warehouse_base}")
    print(f"ML output: {ml_output}")

    fact = safe_read(spark, f"{warehouse_base}/fact_crime_case", "fact_crime_case")
    dim_crime = safe_read(spark, f"{warehouse_base}/dim_crime_type", "dim_crime_type")
    dim_verdict = safe_read(spark, f"{warehouse_base}/dim_verdict_type", "dim_verdict_type")
    dim_city = safe_read_opt(spark, f"{warehouse_base}/dim_city", "dim_city")
    dim_date = safe_read_opt(spark, f"{warehouse_base}/dim_date", "dim_date")
    dim_court = safe_read_opt(spark, f"{warehouse_base}/dim_court", "dim_court")
    dim_judge = safe_read_opt(spark, f"{warehouse_base}/dim_judge", "dim_judge")
    dim_victim = safe_read_opt(spark, f"{warehouse_base}/dim_victim", "dim_victim")
    dim_defendant = safe_read_opt(spark, f"{warehouse_base}/dim_defendant", "dim_defendant")

    base = fact.join(dim_crime.select("crime_type_key", "crime_type"), on="crime_type_key", how="left")
    base = base.join(dim_verdict.select("verdict_type_key", "verdict_type"), on="verdict_type_key", how="left")

    if dim_city is not None:
        base = base.join(dim_city.select("city_key", "city_name", "country_name"), on="city_key", how="left")
    else:
        base = base.withColumn("city_name", F.lit(None).cast("string"))
        base = base.withColumn("country_name", F.lit(None).cast("string"))

    if dim_date is not None:
        date_attr = dim_date.select(
            F.col("date_key").alias("crime_date_key"),
            F.col("year").alias("crime_year"),
            F.col("quarter").alias("crime_quarter"),
            F.col("month").alias("crime_month"),
            F.dayofweek(F.col("full_date")).alias("crime_day_of_week"),
        )
        base = base.join(date_attr, on="crime_date_key", how="left")
    else:
        base = base.withColumn("crime_year", F.lit(None).cast("int"))
        base = base.withColumn("crime_quarter", F.lit(None).cast("int"))
        base = base.withColumn("crime_month", F.lit(None).cast("int"))
        base = base.withColumn("crime_day_of_week", F.lit(None).cast("int"))

    if dim_court is not None:
        base = base.join(dim_court.select("court_key", "court_name"), on="court_key", how="left")
    else:
        base = base.withColumn("court_name", F.lit(None).cast("string"))

    if dim_judge is not None:
        base = base.join(dim_judge.select("judge_key", "judge_years_of_experience"), on="judge_key", how="left")
    else:
        base = base.withColumn("judge_years_of_experience", F.lit(None).cast("double"))

    if dim_victim is not None:
        base = base.join(dim_victim.select("victim_key", "victim_age", "victim_gender"), on="victim_key", how="left")
    else:
        base = base.withColumn("victim_age", F.lit(None).cast("double"))
        base = base.withColumn("victim_gender", F.lit(None).cast("string"))

    if dim_defendant is not None:
        base = base.join(dim_defendant.select("defendant_key", "defendant_age"), on="defendant_key", how="left")
    else:
        base = base.withColumn("defendant_age", F.lit(None).cast("double"))

    flag_cols = [
        "is_missing_location_flag",
        "is_missing_verdict_flag",
        "has_corrections_record_flag",
        "is_still_incarcerated_flag",
        "is_duration_invalid_flag",
    ]
    base = ensure_flag_numeric(base, flag_cols)
    base = base.withColumn(
        "sentence_duration_safe",
        F.when(
            F.col("sentence_duration").cast("double").isNull()
            | F.isnan(F.col("sentence_duration").cast("double"))
            | ((F.col("sentence_duration").cast("double") == float("inf")) | (F.col("sentence_duration").cast("double") == float("-inf"))),
            F.lit(0.0),
        ).otherwise(F.coalesce(F.col("sentence_duration").cast("double"), F.lit(0.0))),
    )
    base = sanitize_numeric_columns(base, [
        "crime_year",
        "crime_quarter",
        "crime_month",
        "crime_day_of_week",
        "victim_age",
        "defendant_age",
        "judge_years_of_experience",
    ], 0.0)

    total_rows = base.count()
    print(f"Base rows: {total_rows}")

    # ---------- KMeans ----------
    print("\n" + "=" * 60)
    print(f"TASK 1: KMeans evaluation (k in [2, 3, 4, 5, 6])")
    print("=" * 60)

    km_rows = []
    for k in [2, 3, 4, 5, 6]:
        km_base = base.filter(F.col("crime_type").isNotNull())

        if "crime_type_idx" in km_base.columns:
            km_base = km_base.drop("crime_type_idx")
        idx = StringIndexer(inputCol="crime_type", outputCol="crime_type_idx", handleInvalid="keep")
        asm = VectorAssembler(
            inputCols=[
                "sentence_duration_safe",
                "has_corrections_record_flag",
                "is_still_incarcerated_flag",
                "is_duration_invalid_flag",
                "is_missing_verdict_flag",
                "crime_year",
                "crime_quarter",
                "crime_month",
                "crime_type_idx",
            ],
            outputCol="cluster_features",
            handleInvalid="keep",
        )
        km_model = Pipeline(stages=[idx, asm, KMeans(featuresCol="cluster_features", predictionCol="cluster_id", k=k, seed=KMEANS_SEED, maxIter=KMEANS_MAX_ITER)]).fit(km_base)
        km_df = km_model.transform(km_base)
        evaluator = ClusteringEvaluator(featuresCol="cluster_features", predictionCol="cluster_id", metricName="silhouette")
        silhouette = evaluator.evaluate(km_df)
        km_rows.append({"k": k, "silhouette": float(silhouette)})
        print(f"  k={k:>1} silhouette={silhouette:.4f}")

    selected = max(km_rows, key=lambda r: r["silhouette"])
    selected_k = selected["k"]
    selected_sil = selected["silhouette"]
    print(f"Selected k = {selected_k}; best silhouette = {selected_sil:.4f}")

    km_base = base.filter(F.col("crime_type").isNotNull())
    idx = StringIndexer(inputCol="crime_type", outputCol="crime_type_idx", handleInvalid="keep")
    asm = VectorAssembler(
        inputCols=[
            "sentence_duration_safe",
            "has_corrections_record_flag",
            "is_still_incarcerated_flag",
            "is_duration_invalid_flag",
            "is_missing_verdict_flag",
            "crime_year",
            "crime_quarter",
            "crime_month",
            "crime_type_idx",
        ],
        outputCol="cluster_features",
        handleInvalid="keep",
    )
    cluster_pipe = Pipeline(stages=[idx, asm, KMeans(featuresCol="cluster_features", predictionCol="cluster_id", k=selected_k, seed=KMEANS_SEED, maxIter=KMEANS_MAX_ITER)])
    cluster_model = cluster_pipe.fit(km_base)
    cluster_result = cluster_model.transform(km_base)

    cluster_out = f"{ml_output}/cluster_predictions"
    cluster_result.select(
        "crime_case_key",
        "crime_id",
        "case_id",
        "crime_type",
        "cluster_id",
    ).write.mode("overwrite").parquet(cluster_out)
    print(f"Cluster predictions -> {cluster_out}")

    # ---------- Verdict prediction ----------
    print("\n" + "=" * 60)
    print("TASK 2: Verdict prediction (Random Forest with stratified split)")
    print("=" * 60)

    rf_base = base.filter(F.col("verdict_type").isNotNull())
    rf_base = rf_base.withColumn("verdict_type", F.trim(F.col("verdict_type")))
    rf_base = rf_base.fillna({
        "crime_type": "Unknown",
        "city_name": "Unknown",
        "country_name": "Unknown",
        "court_name": "Unknown",
        "victim_gender": "Unknown",
        "crime_year": 0.0,
        "crime_month": 0.0,
        "crime_quarter": 0.0,
        "crime_day_of_week": 0.0,
        "victim_age": 0.0,
        "defendant_age": 0.0,
        "judge_years_of_experience": 0.0,
    })

    class_counts = rf_base.groupBy("verdict_type").count().orderBy(F.col("count").desc()).collect()
    print("Overall verdict class distribution:")
    for row in class_counts:
        print(f"  {row['verdict_type']}: {row['count']}")

    train_df, test_df = stratified_split(rf_base, "verdict_type", train_fraction=0.8, seed=RF_SEED)

    print("\nTrain rows: %s | Test rows: %s" % (train_df.count(), test_df.count()))
    print("TRAIN CLASS DISTRIBUTION")
    train_dist = train_df.groupBy("verdict_type").count().orderBy("verdict_type")
    train_dist.show(truncate=False)
    print("TEST CLASS DISTRIBUTION")
    test_dist = test_df.groupBy("verdict_type").count().orderBy("verdict_type")
    test_dist.show(truncate=False)

    cat_cols = ["crime_type", "city_name", "country_name", "court_name", "victim_gender"]
    num_cols = [
        "crime_year",
        "crime_month",
        "crime_quarter",
        "crime_day_of_week",
        "victim_age",
        "defendant_age",
        "judge_years_of_experience",
        "is_missing_location_flag",
        "has_corrections_record_flag",
        "is_still_incarcerated_flag",
    ]
    indexers = []
    encoders = []
    encoded_cols = []
    for c in cat_cols:
        idx_col = f"{c}_idx"
        enc_col = f"{c}_vec"
        idx = StringIndexer(inputCol=c, outputCol=idx_col, handleInvalid="keep")
        enc = OneHotEncoder(inputCols=[idx_col], outputCols=[enc_col], handleInvalid="keep")
        indexers.append(idx)
        encoders.append(enc)
        encoded_cols.append(enc_col)

    label_indexer = StringIndexer(inputCol="verdict_type", outputCol="label", handleInvalid="keep")
    rf_assembler = VectorAssembler(inputCols=num_cols + encoded_cols, outputCol="rf_features", handleInvalid="keep")
    label_model = label_indexer.fit(rf_base)
    label_converter = IndexToString(inputCol="prediction", outputCol="predicted_verdict", labels=label_model.labels)

    total_count = float(rf_base.count())
    class_weight_map = {
        row["verdict_type"]: (total_count / (len(class_counts) * float(row["count"]))) if row["count"] else 1.0
        for row in class_counts
    }
    weight_pairs = []
    for verdict_name, verdict_weight in class_weight_map.items():
        weight_pairs.extend([F.lit(verdict_name), F.lit(float(verdict_weight))])
    weight_map_expr = F.create_map(*weight_pairs)
    train_df = train_df.withColumn("sample_weight", weight_map_expr.getItem(F.col("verdict_type")).cast("double"))
    train_df = train_df.fillna({"sample_weight": 1.0})
    test_df = test_df.withColumn("sample_weight", F.lit(1.0).cast("double"))

    rf_model = RandomForestClassifier(
        featuresCol="rf_features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="raw_prediction",
        numTrees=RF_NUM_TREES,
        maxDepth=RF_MAX_DEPTH,
        featureSubsetStrategy="sqrt",
        minInstancesPerNode=1,
        seed=RF_SEED,
        weightCol="sample_weight",
    )

    stages = indexers + encoders + [label_indexer, rf_assembler, rf_model, label_converter]
    rf_pipeline = Pipeline(stages=stages)
    fitted_model = rf_pipeline.fit(train_df)
    preds = fitted_model.transform(test_df)
    preds = preds.withColumn("prediction", F.col("prediction").cast("int"))

    # Evaluate metrics
    preds = preds.withColumn(
        "prediction",
        F.col("prediction").cast("double")
    )

    acc = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    ).evaluate(preds)
    weighted_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(preds)
    weighted_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(preds)
    weighted_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedFMeasure").evaluate(preds)
    macro_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(preds)

    print(f"Accuracy: {acc:.4f}")
    print(f"Weighted Precision: {weighted_precision:.4f}")
    print(f"Weighted Recall: {weighted_recall:.4f}")
    print(f"Weighted F1: {weighted_f1:.4f}")
    print(f"Macro F1: {macro_f1:.4f}")

    # confusion matrix and per-class metrics
    confusion = preds.groupBy("verdict_type", "predicted_verdict").count().withColumnRenamed("verdict_type", "actual_verdict")
    confusion_output = f"{ml_output}/confusion_matrix"
    confusion.write.mode("overwrite").parquet(confusion_output)
    print(f"Confusion matrix -> {confusion_output}")

    label_map = {idx: label for idx, label in enumerate(label_model.labels)}
    pred_summary = preds.select("label", "prediction", "verdict_type", "predicted_verdict")
    metric_rows = []
    for label_name in sorted(label_map.values()):
        actual_df = pred_summary.filter(F.col("verdict_type") == label_name)
        actual_total = actual_df.count()
        if actual_total == 0:
            continue
        true_positives = actual_df.filter(F.col("predicted_verdict") == label_name).count()
        predicted_total = pred_summary.filter(F.col("predicted_verdict") == label_name).count()
        c_precision = true_positives / max(1, predicted_total)
        c_recall = true_positives / max(1, actual_total)
        c_f1 = (2 * c_precision * c_recall) / max(1e-9, c_precision + c_recall)
        metric_rows.append({
            "verdict": label_name,
            "precision": float(c_precision),
            "recall": float(c_recall),
            "f1": float(c_f1),
            "support": int(actual_total),
        })

    per_class_df = spark.createDataFrame(metric_rows, schema=["verdict", "precision", "recall", "f1", "support"])
    per_class_output = f"{ml_output}/per_class_metrics"
    per_class_df.write.mode("overwrite").parquet(per_class_output)
    print(f"Per-class metrics -> {per_class_output}")

    # feature importance
    rf_estimator = fitted_model.stages[-2]
    feature_names = rf_assembler.getInputCols()
    importances = rf_estimator.featureImportances.toArray()
    feat_rows = []
    limit = min(len(feature_names), len(importances))

    for idx in range(limit):
        importance = importances[idx]
        feat_rows.append({
            "feature": feature_names[idx],
            "importance": float(importance),
            "rank": idx + 1,
        })

    # Keep a clear warning instead of failing if Spark returns
    # a different number of vector features.
    if len(importances) != len(feature_names):
        print(
            f"WARNING: feature name count ({len(feature_names)}) "
            f"!= importance count ({len(importances)}); "
            f"exported first {limit} features."
        )
    feat_df = spark.createDataFrame(feat_rows).orderBy(F.col("importance").desc())
    feat_df = feat_df.withColumn("rank", F.row_number().over(Window.orderBy(F.col("importance").desc())))
    feature_out = f"{ml_output}/feature_importance"
    feat_df.write.mode("overwrite").parquet(feature_out)
    print(f"Feature importance -> {feature_out}")

    verdict_out = f"{ml_output}/verdict_predictions"
    preds.select(
        "crime_case_key",
        "crime_id",
        "case_id",
        "crime_type",
        F.col("verdict_type").alias("actual_verdict"),
        F.col("predicted_verdict"),
        F.col("prediction").cast("int").alias("prediction"),
    ).write.mode("overwrite").parquet(verdict_out)
    print(f"Verdict predictions -> {verdict_out}")

    # cluster profile
    cluster_counts = cluster_result.groupBy("cluster_id").count().withColumnRenamed("count", "cluster_count")
    crime_profile = cluster_result.groupBy("cluster_id", "crime_type").count().withColumnRenamed("count", "crime_count")
    crime_profile = crime_profile.orderBy(F.col("cluster_id"), F.col("crime_count").desc())
    crime_top = crime_profile.groupBy("cluster_id").agg(F.first("crime_type").alias("top_crime_type"))
    city_profile = cluster_result.groupBy("cluster_id", "city_name").count().withColumnRenamed("count", "city_count")
    city_profile = city_profile.orderBy(F.col("cluster_id"), F.col("city_count").desc())
    city_top = city_profile.groupBy("cluster_id").agg(F.first("city_name").alias("top_city"))
    age_victim = cluster_result.groupBy("cluster_id").agg(F.avg("victim_age").alias("avg_victim_age"))
    age_defendant = cluster_result.groupBy("cluster_id").agg(F.avg("defendant_age").alias("avg_defendant_age"))
    cluster_profile = cluster_counts
    cluster_profile = cluster_profile.join(crime_top, on="cluster_id", how="left")
    cluster_profile = cluster_profile.join(city_top, on="cluster_id", how="left")
    cluster_profile = cluster_profile.join(age_victim, on="cluster_id", how="left")
    cluster_profile = cluster_profile.join(age_defendant, on="cluster_id", how="left")
    cluster_profile = cluster_profile.withColumn("percentage", F.col("cluster_count") / F.sum("cluster_count").over(Window.partitionBy()))
    cluster_profile = cluster_profile.withColumn("percentage", F.round(F.col("percentage") * 100.0, 2))
    cluster_profile_output = f"{ml_output}/cluster_profile"
    cluster_profile.write.mode("overwrite").parquet(cluster_profile_output)
    print(f"Cluster profile -> {cluster_profile_output}")

    # metrics aggregation
    metrics = [
        {
            "task": "kmeans_clustering",
            "status": "completed",
            "k": int(selected_k),
            "silhouette": float(selected_sil),
            "total_rows": int(total_rows),
            "train_rows": None,
            "test_rows": None,
            "accuracy": None,
            "weighted_precision": None,
            "weighted_recall": None,
            "weighted_f1": None,
            "macro_f1": None,
            "num_trees": None,
            "max_depth": None,
            "reason": None,
        },
        {
            "task": "verdict_prediction",
            "status": "completed",
            "k": None,
            "silhouette": None,
            "total_rows": int(rf_base.count()),
            "train_rows": int(train_df.count()),
            "test_rows": int(test_df.count()),
            "accuracy": float(acc),
            "weighted_precision": float(weighted_precision),
            "weighted_recall": float(weighted_recall),
            "weighted_f1": float(weighted_f1),
            "macro_f1": float(macro_f1),
            "num_trees": RF_NUM_TREES,
            "max_depth": RF_MAX_DEPTH,
            "reason": None,
        },
    ]
    # Normalize optional metric values so Spark can infer schema safely.
    normalized_metrics = []
    for m in metrics:
        row = {}
        for k, v in m.items():
            if v is None:
                row[k] = 0.0
            elif isinstance(v, bool):
                row[k] = bool(v)
            elif isinstance(v, (int, float)):
                row[k] = float(v)
            else:
                row[k] = str(v)
        normalized_metrics.append(row)

    metrics_df = spark.createDataFrame(normalized_metrics)
    metrics_out = f"{ml_output}/metrics"
    metrics_df.write.mode("overwrite").parquet(metrics_out)
    print(f"Metrics -> {metrics_out}")

    print("\n" + "=" * 70)
    print("ML pipeline complete")
    print("=" * 70)
    spark.stop()


def parse_args():
    p = argparse.ArgumentParser(description="EuroCrimePulse ML Pipeline")
    p.add_argument("--warehouse-base", default=DEFAULT_WAREHOUSE_BASE)
    p.add_argument("--ml-output", default=DEFAULT_ML_OUTPUT)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.warehouse_base, args.ml_output)
