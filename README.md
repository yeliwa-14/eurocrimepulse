# EuroCrimePulse

End-to-end crime data engineering platform:

Continuous Generator → Kafka → Spark Structured Streaming → HDFS Bronze/Silver/Gold layers
→ Star Schema Warehouse → ClickHouse → Analytics/ML → Streamlit.
## 🎥 Demo

[Watch the full project demo](https://drive.google.com/file/d/11Z93jqm7_0wBkypEYluClIMePQCiTtc2/view?usp=drivesdk)


## Project structure

- `analysis/` — original data-analysis and ML notebooks
- `streaming/` — generator, Kafka producer and Spark streaming jobs
- `warehouse/` — Gold Star Schema builder
- `clickhouse/` — ClickHouse setup and loader
- `ml/` — production ML pipeline (Clustring)
- `dashboard/` — Streamlit dashboard
- `airflow/` — Airflow DAG
- `monitoring/` — health checks
- `scripts/` — project start/stop helpers

Production runtime is Docker/Linux. See `FINAL_RUNBOOK.md'.
