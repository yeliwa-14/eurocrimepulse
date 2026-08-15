# EuroCrimePulse — Final Runbook

## Production structure

- `analysis/` — data analysis and ML notebooks
- `streaming/` — generator, Kafka producer, Police/Court/Corrections streaming jobs
- `warehouse/` — Gold Star Schema builder
- `clickhouse/` — ClickHouse schema and loader
- `ml/` — production ML pipeline
- `dashboard/` — Streamlit dashboard
- `monitoring/` — health checks
- `scripts/` — project start/stop
- `airflow/` — final Airflow DAG

## Runtime environment

```bash
export EUROCRIMEPULSE_HDFS_BASE=hdfs://localhost:9000/eurocrimepulse
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_NATIVE_PORT=9010
export CLICKHOUSE_DB=eurocrimepulse
export EUROCRIMEPULSE_CHECKPOINT_BASE=hdfs://localhost:9000/tmp/eurocrimepulse/checkpoints
export EUROCRIMEPULSE_LANDING_DIR=/opt/eurocrimepulse/streaming/landing
```

## Start

```bash
bash /opt/eurocrimepulse/scripts/start_project.sh
```

This starts the continuous generator, continuous Kafka producer, the three
Spark Structured Streaming jobs, and Streamlit.

## Stop

```bash
bash /opt/eurocrimepulse/scripts/stop_project.sh
```

## Streaming test

```bash
bash /opt/eurocrimepulse/streaming/run_streaming.sh test
```

## Star Schema

```bash
spark-submit --master local[2]   --conf spark.sql.session.timeZone=UTC   /opt/eurocrimepulse/warehouse/gold_star_schema.py   --gold-base hdfs://localhost:9000/eurocrimepulse/gold   --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse
```

## Machine Learning

```bash
/usr/local/spark/bin/spark-submit --master local[2]   --conf spark.sql.session.timeZone=UTC   /opt/eurocrimepulse/ml/ml_clustering.py   --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse   --ml-output hdfs://localhost:9000/eurocrimepulse/ml
```

## ClickHouse

```bash
python3 /opt/eurocrimepulse/clickhouse/load_to_clickhouse.py   --warehouse-base hdfs://localhost:9000/eurocrimepulse/warehouse   --clickhouse-host localhost   --clickhouse-port 8123   --clickhouse-db eurocrimepulse   --ml-base hdfs://localhost:9000/eurocrimepulse/ml
```

## Streamlit

```bash
streamlit run /opt/eurocrimepulse/dashboard/streamlit_dashboard.py   --server.address 0.0.0.0   --server.port 8501
```

Open from Windows:

`http://localhost:8501`

## Airflow

DAG file:

`/opt/airflow/dags/eurocrimepulse_pipeline.py`

DAG ID:

`eurocrimepulse_pipeline`
