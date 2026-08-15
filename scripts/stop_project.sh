#!/usr/bin/env bash
set -u

LOG_DIR="/tmp/eurocrimepulse_logs"
STREAMING_DIR="/opt/eurocrimepulse/streaming"
PID_DIR="/tmp/eurocrimepulse/pids"

if [ -d "$LOG_DIR" ]; then
  for pidfile in "$LOG_DIR"/*.pid; do
    [ -f "$pidfile" ] || continue
    pid=$(cat "$pidfile" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $pidfile (PID $pid)"
      kill "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  done
fi

if [ -x "${STREAMING_DIR}/run_streaming.sh" ]; then
  bash "${STREAMING_DIR}/run_streaming.sh" stop-all || true
fi

# Stop only the project's Streamlit process.
pkill -f "streamlit run /opt/eurocrimepulse/dashboard/streamlit_dashboard.py" 2>/dev/null || true

echo "EuroCrimePulse project processes stopped."
