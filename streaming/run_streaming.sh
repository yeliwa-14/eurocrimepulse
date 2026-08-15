#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# EuroCrimePulse — Docker Integration Test & Run Script
#
# Run this script INSIDE the Docker container 'eurocrimepulse'.
#
# Prerequisites:
#   - Hadoop / HDFS running  (hdfs://localhost:9000)
#   - Kafka + ZooKeeper running  (localhost:9092)
#   - Spark 3.4.1 available via spark-submit
#   - kafka-python installed:  pip install kafka-python faker
#
# Usage:
#   chmod +x run_streaming.sh
#   ./streaming/run_streaming.sh setup          # Create Kafka topics & HDFS dirs
#   ./streaming/run_streaming.sh generate 100   # Generate 100 records
#   ./streaming/run_streaming.sh produce        # Push to Kafka
#   ./streaming/run_streaming.sh start-police   # Start Police streaming (foreground)
#   ./streaming/run_streaming.sh start-court    # Start Court streaming
#   ./streaming/run_streaming.sh start-corr     # Start Corrections streaming
#   ./streaming/run_streaming.sh start-all      # Start all three (background)
#   ./streaming/run_streaming.sh stop-all       # Kill background streaming jobs
#   ./streaming/run_streaming.sh test           # Full integration test
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/usr/local/kafka}"
SPARK_HOME="${SPARK_HOME:-/usr/local/spark}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-${KAFKA_BOOTSTRAP:-localhost:9092}}"
HDFS_BASE="${EUROCRIMEPULSE_HDFS_BASE:-hdfs://localhost:9000/eurocrimepulse}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LANDING_DIR="${EUROCRIMEPULSE_LANDING_DIR:-${SCRIPT_DIR}/landing}"
PID_DIR="/tmp/eurocrimepulse/pids"

SPARK_KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

# ── Colours ──
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✓ $*${NC}"; }
warn() { echo -e "${YELLOW}⚠ $*${NC}"; }
fail() { echo -e "${RED}✗ $*${NC}"; }

# ═══════════════════════════════════════════════════════════════════════════
# setup — Create Kafka topics and HDFS directories
# ═══════════════════════════════════════════════════════════════════════════
cmd_setup() {
    echo "=== Setting up Kafka topics ==="
    for TOPIC in eurocrimepulse.police eurocrimepulse.court eurocrimepulse.corrections; do
        "${KAFKA_HOME}/bin/kafka-topics.sh" \
            --bootstrap-server "${KAFKA_BOOTSTRAP}" \
            --create --if-not-exists \
            --topic "${TOPIC}" \
            --partitions 1 \
            --replication-factor 1 2>/dev/null && ok "Topic: ${TOPIC}" || warn "Topic ${TOPIC} may already exist"
    done

    echo ""
    echo "=== Creating HDFS directories ==="
    for DIR in bronze/police bronze/court bronze/corrections \
               silver/police silver/court silver/corrections \
               gold/police gold/court gold/corrections \
               warehouse; do
        hdfs dfs -mkdir -p "${HDFS_BASE}/${DIR}" && ok "${DIR}"
    done

    mkdir -p "${PID_DIR}"
    ok "Setup complete"
}

# ═══════════════════════════════════════════════════════════════════════════
# generate — Generate a batch using the generator
# ═══════════════════════════════════════════════════════════════════════════
cmd_generate() {
    NUM="${1:-100}"
    echo "=== Generating ${NUM} crime records ==="
    python3 "${SCRIPT_DIR}/streaming_generator.py" \
        --num-crimes "${NUM}" \
        --outdir "${LANDING_DIR}" \
        --seed 42 \
        --generated-at "2026-08-12T00:00:00Z"
    ok "Generated ${NUM} records in ${LANDING_DIR}"
}

# ═══════════════════════════════════════════════════════════════════════════
# produce — Push the latest batch to Kafka
# ═══════════════════════════════════════════════════════════════════════════
cmd_produce() {
    BATCH_DIR="${1:-$(ls -dt "${LANDING_DIR}"/batch_* 2>/dev/null | head -1)}"
    if [ -z "${BATCH_DIR}" ] || [ ! -d "${BATCH_DIR}" ]; then
        fail "No batch directory found. Run 'generate' first."
        exit 1
    fi
    echo "=== Producing batch: ${BATCH_DIR} ==="
    python3 "${SCRIPT_DIR}/kafka_producer.py" \
        "${BATCH_DIR}" \
        --bootstrap "${KAFKA_BOOTSTRAP}"
}

# ═══════════════════════════════════════════════════════════════════════════
# start-* — Start streaming jobs
# ═══════════════════════════════════════════════════════════════════════════
_spark_submit() {
    local JOB_NAME="$1"
    local SCRIPT="$2"
    echo "=== Starting ${JOB_NAME} streaming ==="
    "${SPARK_HOME}/bin/spark-submit" \
        --packages "${SPARK_KAFKA_PKG}" \
        --master local[2] \
        --conf "spark.sql.session.timeZone=UTC" \
        "${SCRIPT}"
}

_spark_submit_bg() {
    local JOB_NAME="$1"
    local SCRIPT="$2"
    mkdir -p "${PID_DIR}"
    echo "=== Starting ${JOB_NAME} streaming (background) ==="
    nohup "${SPARK_HOME}/bin/spark-submit" \
        --packages "${SPARK_KAFKA_PKG}" \
        --master local[2] \
        --conf "spark.sql.session.timeZone=UTC" \
        "${SCRIPT}" \
        > "/tmp/eurocrimepulse/${JOB_NAME}.log" 2>&1 &
    echo $! > "${PID_DIR}/${JOB_NAME}.pid"
    ok "${JOB_NAME} PID=$(cat "${PID_DIR}/${JOB_NAME}.pid")"
}

cmd_start_police() { _spark_submit "police" "${SCRIPT_DIR}/police_stream.py"; }
cmd_start_court()  { _spark_submit "court"  "${SCRIPT_DIR}/court_stream.py"; }
cmd_start_corr()   { _spark_submit "corrections" "${SCRIPT_DIR}/corrections_stream.py"; }

cmd_start_all() {
    _spark_submit_bg "police"      "${SCRIPT_DIR}/police_stream.py"
    sleep 5
    _spark_submit_bg "court"       "${SCRIPT_DIR}/court_stream.py"
    sleep 5
    _spark_submit_bg "corrections" "${SCRIPT_DIR}/corrections_stream.py"
    echo ""
    ok "All streaming jobs started in background"
    echo "Logs: /tmp/eurocrimepulse/{police,court,corrections}.log"
}

cmd_stop_all() {
    echo "=== Stopping all streaming jobs ==="
    for JOB in police court corrections; do
        PID_FILE="${PID_DIR}/${JOB}.pid"
        if [ -f "${PID_FILE}" ]; then
            PID=$(cat "${PID_FILE}")
            kill "${PID}" 2>/dev/null && ok "Killed ${JOB} (PID=${PID})" || warn "${JOB} already stopped"
            rm -f "${PID_FILE}"
        fi
    done
}

# ═══════════════════════════════════════════════════════════════════════════
# verify — Verify HDFS outputs
# ═══════════════════════════════════════════════════════════════════════════
cmd_verify() {
    echo "=== Verifying HDFS outputs ==="
    for LAYER in bronze silver gold; do
        for SOURCE in police court corrections; do
            PATH_TO_CHECK="${HDFS_BASE}/${LAYER}/${SOURCE}"
            COUNT=$(hdfs dfs -ls -R "${PATH_TO_CHECK}" 2>/dev/null | grep "\.parquet$" | wc -l)
            if [ "${COUNT}" -gt 0 ]; then
                ok "${LAYER}/${SOURCE}: ${COUNT} parquet file(s)"
            else
                warn "${LAYER}/${SOURCE}: no parquet files"
            fi
        done
    done
}

# ═══════════════════════════════════════════════════════════════════════════
# star-schema — Build Star Schema from Gold Parquet
# ═══════════════════════════════════════════════════════════════════════════
cmd_star_schema() {
    echo "=== Building Star Schema from Gold ==="
    "${SPARK_HOME}/bin/spark-submit" \
        --master local[2] \
        --conf "spark.sql.session.timeZone=UTC" \
        "${PROJECT_ROOT}/warehouse/gold_star_schema.py" \
        --gold-base "${HDFS_BASE}/gold" \
        --warehouse-base "${HDFS_BASE}/warehouse"
    ok "Star Schema build complete"
}

# ═══════════════════════════════════════════════════════════════════════════
# clickhouse-load — Load Star Schema into ClickHouse
# ═══════════════════════════════════════════════════════════════════════════
cmd_clickhouse_load() {
    echo "=== Loading Star Schema into ClickHouse ==="
    python3 "${PROJECT_ROOT}/clickhouse/load_to_clickhouse.py" \
        --warehouse-base "${HDFS_BASE}/warehouse" \
        "${@}"
    ok "ClickHouse load complete"
}

# ═══════════════════════════════════════════════════════════════════════════
# test — Full integration test
# ═══════════════════════════════════════════════════════════════════════════
cmd_test() {
    echo "╔══════════════════════════════════════════════════════╗"
    echo "║  EuroCrimePulse — Full Integration Test             ║"
    echo "╚══════════════════════════════════════════════════════╝"
    echo ""

    # 1. Setup
    cmd_setup

    # 2. Clean previous data
    echo ""
    echo "=== Cleaning previous data ==="
    hdfs dfs -rm -r -f "${HDFS_BASE}/bronze" "${HDFS_BASE}/silver" "${HDFS_BASE}/gold" "${HDFS_BASE}/warehouse" 2>/dev/null || true
    rm -rf /tmp/eurocrimepulse/checkpoints 2>/dev/null || true
    rm -rf "${LANDING_DIR}" 2>/dev/null || true
    cmd_setup
    ok "Previous data cleaned"

    # 3. Generate batch 1
    echo ""
    cmd_generate 100

    # 4. Produce to Kafka
    echo ""
    cmd_produce

    # 5. Start streaming (background)
    echo ""
    cmd_start_all

    # 6. Wait for processing
    echo ""
    echo "=== Waiting 60 seconds for processing ==="
    sleep 60

    # 7. Verify
    echo ""
    cmd_verify

    # 8. Generate batch 2 (incremental test)
    echo ""
    echo "=== Incremental batch test ==="
    python3 "${SCRIPT_DIR}/streaming_generator.py" \
        --num-crimes 50 \
        --outdir "${LANDING_DIR}" \
        --seed 99 \
        --generated-at "2026-08-12T01:00:00Z"
    BATCH2=$(ls -dt "${LANDING_DIR}"/batch_* | head -1)
    cmd_produce "${BATCH2}"

    echo "=== Waiting 60 seconds for incremental processing ==="
    sleep 60
    cmd_verify

    # 9. Checkpoint recovery test
    echo ""
    echo "=== Checkpoint recovery test ==="
    cmd_stop_all
    sleep 10

    # Generate batch 3
    python3 "${SCRIPT_DIR}/streaming_generator.py" \
        --num-crimes 25 \
        --outdir "${LANDING_DIR}" \
        --seed 123 \
        --generated-at "2026-08-12T02:00:00Z"
    BATCH3=$(ls -dt "${LANDING_DIR}"/batch_* | head -1)
    cmd_produce "${BATCH3}"

    # Restart streaming
    cmd_start_all
    echo "=== Waiting 60 seconds for resumed processing ==="
    sleep 60
    cmd_verify

    # 10. Stop
    echo ""
    cmd_stop_all

    echo ""
    echo "╔══════════════════════════════════════════════════════╗"
    echo "║  Integration test complete — review output above    ║"
    echo "╚══════════════════════════════════════════════════════╝"
}

# ═══════════════════════════════════════════════════════════════════════════
# Main dispatcher
# ═══════════════════════════════════════════════════════════════════════════
case "${1:-help}" in
    setup)          cmd_setup ;;
    generate)       cmd_generate "${2:-100}" ;;
    produce)        cmd_produce "${2:-}" ;;
    start-police)   cmd_start_police ;;
    start-court)    cmd_start_court ;;
    start-corr)     cmd_start_corr ;;
    start-all)      cmd_start_all ;;
    stop-all)       cmd_stop_all ;;
    verify)         cmd_verify ;;
    star-schema)    cmd_star_schema ;;
    clickhouse-load) cmd_clickhouse_load "${@:2}" ;;
    test)           cmd_test ;;
    *)
        echo "Usage: $0 {setup|generate N|produce [dir]|start-police|start-court|start-corr|start-all|stop-all|verify|star-schema|clickhouse-load|test}"
        exit 1
        ;;
esac
