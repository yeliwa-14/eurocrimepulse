"""
EuroCrimePulse — final Airflow orchestration DAG.
Long-lived streaming services are started/verified separately; DAG tasks are finite.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/eurocrimepulse"
HDFS_BASE = "hdfs://localhost:9000/eurocrimepulse"
WAREHOUSE = f"{HDFS_BASE}/warehouse"
ML_BASE = f"{HDFS_BASE}/ml"

default_args = {
    "owner": "eurocrimepulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="eurocrimepulse_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 13),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["eurocrimepulse", "streaming", "spark", "hdfs", "clickhouse", "ml"],
) as dag:

    start_project = BashOperator(
        task_id="start_project",
        bash_command=f"bash {PROJECT}/scripts/start_project.sh ",
    )

    verify_streaming = BashOperator(
        task_id="verify_streaming",
        bash_command=f"python3 {PROJECT}/monitoring/streaming_health_check.py",
    )

    build_star = BashOperator(
        task_id="build_star_schema",
        bash_command=(
            f"export EUROCRIMEPULSE_HDFS_BASE={HDFS_BASE}; "
            f"spark-submit --master local[2] "
            f"--conf spark.sql.session.timeZone=UTC "
            f"{PROJECT}/warehouse/gold_star_schema.py "
            f"--gold-base {HDFS_BASE}/gold "
            f"--warehouse-base {WAREHOUSE}"
        ),
    )

    run_ml = BashOperator(
        task_id="run_ml",
        bash_command=(
            f"export EUROCRIMEPULSE_HDFS_BASE={HDFS_BASE}; "
            f"/usr/local/spark/bin/spark-submit --master local[2] "
            f"--conf spark.sql.session.timeZone=UTC "
            f"{PROJECT}/ml/ml_clustering.py "
            f"--warehouse-base {WAREHOUSE} "
            f"--ml-output {ML_BASE}"
        ),
    )

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=(
            f"export EUROCRIMEPULSE_HDFS_BASE={HDFS_BASE}; "
            f"export CLICKHOUSE_HOST=localhost; export CLICKHOUSE_PORT=8123; "
            f"export CLICKHOUSE_NATIVE_PORT=9010; export CLICKHOUSE_DB=eurocrimepulse; "
            f"python3 {PROJECT}/clickhouse/load_to_clickhouse.py "
            f"--warehouse-base {WAREHOUSE} "
            f"--clickhouse-host localhost "
            f"--clickhouse-port 8123 "
            f"--clickhouse-db eurocrimepulse "
            f"--ml-base {ML_BASE}"
        ),
    )

    final_health = BashOperator(
        task_id="final_health_check",
        bash_command=f"python3 {PROJECT}/monitoring/health_check.py",
    )

    start_project >> verify_streaming >> build_star >> run_ml >> load_clickhouse >> final_health
