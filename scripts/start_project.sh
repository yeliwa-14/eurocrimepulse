#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/opt/eurocrimepulse"
cd "$PROJECT_ROOT"
STREAMING_DIR="${PROJECT_ROOT}/streaming"
SCRIPTS_DIR="${PROJECT_ROOT}/scripts"
MONITORING_DIR="${PROJECT_ROOT}/monitoring"
DASHBOARD="${PROJECT_ROOT}/dashboard/streamlit_dashboard.py"
LOG_DIR="/tmp/eurocrimepulse_logs"
mkdir -p "$LOG_DIR" "${STREAMING_DIR}/landing"

export EUROCRIMEPULSE_HDFS_BASE="${EUROCRIMEPULSE_HDFS_BASE:-hdfs://localhost:9000/eurocrimepulse}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
export CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT:-9010}"
export CLICKHOUSE_DB="${CLICKHOUSE_DB:-eurocrimepulse}"
export EUROCRIMEPULSE_CHECKPOINT_BASE="${EUROCRIMEPULSE_CHECKPOINT_BASE:-hdfs://localhost:9000/tmp/eurocrimepulse/checkpoints}"
export EUROCRIMEPULSE_LANDING_DIR="${EUROCRIMEPULSE_LANDING_DIR:-${STREAMING_DIR}/landing}"

start_bg() {
  local name="$1"
  shift
  local pidfile="$LOG_DIR/${name}.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "$name already running (pid $(cat "$pidfile"))"
    return 0
  fi
  echo "Starting $name: $*"
  nohup "$@" > "$LOG_DIR/${name}.out" 2>&1 &
  echo $! > "$pidfile"
  sleep 1
}

if [ -x "${STREAMING_DIR}/run_streaming.sh" ]; then
  bash "${STREAMING_DIR}/run_streaming.sh" setup || true
fi

start_bg generator python3 "${STREAMING_DIR}/streaming_generator.py"   --continuous --batch-size "${STREAM_BATCH_SIZE:-20}"   --interval "${STREAM_INTERVAL_SECONDS:-10}"   --outdir "${EUROCRIMEPULSE_LANDING_DIR}"

start_bg producer python3 "${STREAMING_DIR}/kafka_producer.py"   --outdir "${EUROCRIMEPULSE_LANDING_DIR}"   --continuous   --poll-interval "${KAFKA_POLL_INTERVAL_SECONDS:-5}"   --bootstrap "${KAFKA_BOOTSTRAP_SERVERS}"

# Start the three Spark Structured Streaming jobs.
if [ -x "${STREAMING_DIR}/run_streaming.sh" ]; then
  start_bg spark_streaming bash "${STREAMING_DIR}/run_streaming.sh" start-all
else
  echo "WARNING: ${STREAMING_DIR}/run_streaming.sh not executable"
fi

# Start the dashboard.
start_bg streamlit streamlit run "${DASHBOARD}"   --server.address 0.0.0.0   --server.port 8501

echo "EuroCrimePulse start commands issued."
echo "Logs: ${LOG_DIR}"
