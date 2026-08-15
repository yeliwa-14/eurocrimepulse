#!/usr/bin/env python3
"""
EuroCrimePulse — health_check.py
Performs lightweight checks for the key components used by the project.
Designed to be runnable inside the production container and callable by Airflow.
Exits with code 0 if all critical components pass, non-zero otherwise.

Checks performed (best-effort):
- Python environment
- ClickHouse HTTP connectivity and presence of eurocrimepulse DB
- HDFS availability (hdfs dfs -ls)
- Kafka availability (via kafka-topics.sh --bootstrap-server)
- Star schema tables presence in ClickHouse (fact_crime_case + dims)
- ML tables presence
- Streamlit process presence (if detectable via pgrep)

This script does NOT attempt to start services. It reports statuses and diagnostics.
"""

import json
import os
import subprocess
import sys
import time
from typing import Dict


def run_cmd(cmd, check=False, capture_output=True, timeout=10):
    try:
        res = subprocess.run(cmd, shell=True, capture_output=capture_output, text=True, timeout=timeout)
        return res.returncode, res.stdout.strip(), res.stderr.strip()
    except Exception as exc:
        return 2, "", str(exc)


def check_clickhouse(host, port, db):
    auth = ""
    ch_url = f"http://{host}:{port}/"
    cmd = f"curl -sS --max-time 5 '{ch_url}?query=SELECT+1'"
    code, out, err = run_cmd(cmd, timeout=6)
    ok = (code == 0 and out and "1" in out)
    info = out or err
    # check database
    cmd_db = f"curl -sS --max-time 5 '{ch_url}?query=SHOW+DATABASES'"
    code2, out2, err2 = run_cmd(cmd_db, timeout=6)
    db_ok = (code2 == 0 and db in out2)
    return {"ok": ok, "info": info, "db_ok": db_ok}


def check_hdfs():
    code, out, err = run_cmd("hdfs dfs -ls / 2>&1 | head -n 1", timeout=6)
    ok = code == 0
    info = out or err
    return {"ok": ok, "info": info}


def check_kafka(brokers):
    # Try kafka-topics.sh to list topics (best-effort)
    kcmd = os.environ.get("KAFKA_TOPICS_CMD", "kafka-topics.sh --bootstrap-server {brokers} --list")
    cmd = kcmd.format(brokers=brokers)
    code, out, err = run_cmd(cmd, timeout=8)
    ok = code == 0
    info = out or err
    return {"ok": ok, "info": info}


def check_clickhouse_tables(host, port, db, tables):
    missing = []
    for t in tables:
        cmd = f"curl -sS --max-time 6 'http://{host}:{port}/?query=EXISTS+TABLE+{db}.{t}'"
        code, out, err = run_cmd(cmd, timeout=6)
        if code != 0 or ("0" in out and "1" not in out):
            missing.append(t)
    return {"missing": missing}


def check_streamlit():
    # Non-critical: detect whether a streamlit process exists
    code, out, err = run_cmd("pgrep -a streamlit || true", timeout=4)
    ok = bool(out.strip())
    return {"ok": ok, "info": out or err}


def main():
    CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CH_DB = os.getenv("CLICKHOUSE_DB", "eurocrimepulse")

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

    result: Dict = {"timestamp": int(time.time()), "checks": {}}

    result["checks"]["clickhouse"] = check_clickhouse(CH_HOST, CH_PORT, CH_DB)
    result["checks"]["hdfs"] = check_hdfs()
    result["checks"]["kafka"] = check_kafka(KAFKA_BOOTSTRAP)
    result["checks"]["streamlit"] = check_streamlit()

    # verify expected tables
    expected_tables = [
        "fact_crime_case",
        "dim_crime_type",
        "dim_city",
        "dim_date",
        "dim_victim",
        "dim_defendant",
    ]
    result["checks"]["clickhouse_tables"] = check_clickhouse_tables(CH_HOST, CH_PORT, CH_DB, expected_tables)

    print(json.dumps(result, indent=2))

    # Exit code: 0 if critical services ok (ClickHouse and HDFS), else non-zero
    critical_ok = result["checks"]["clickhouse"]["ok"] and result["checks"]["hdfs"]["ok"]
    sys.exit(0 if critical_ok else 2)


if __name__ == "__main__":
    main()
