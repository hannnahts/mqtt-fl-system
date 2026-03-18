#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -x "${ROOT_DIR}/venv/bin/python" ]]; then
  PYTHON_BIN="${ROOT_DIR}/venv/bin/python"
else
  PYTHON_BIN="python3"
fi

check_dependencies() {
  "${PYTHON_BIN}" -c "import pandas, numpy, sklearn, yaml, paho.mqtt.client" >/dev/null 2>&1 || {
    echo "Missing Python dependencies. Run: pip install -r requirements.txt" >&2
    exit 1
  }
}

check_dependencies

cleanup() {
  local exit_code=$?
  if [[ -n "${PIDS:-}" ]]; then
    kill ${PIDS} 2>/dev/null || true
  fi
  docker compose -f docker-compose.baseline.yml down --remove-orphans >/dev/null 2>&1 || true
  exit "${exit_code}"
}

trap cleanup EXIT

cd "${ROOT_DIR}"

"${PYTHON_BIN}" scripts/prepare_dataset.py
"${PYTHON_BIN}" scripts/split_dataset.py

docker compose -f docker-compose.baseline.yml up -d

"${PYTHON_BIN}" -m app.aggregator.aggregator --config configs/experiment_baseline.yaml &
PIDS="$!"

for client_id in 1 2 3 4 5; do
  "${PYTHON_BIN}" -m app.clients.client --config configs/experiment_baseline.yaml --client-id "${client_id}" &
  PIDS="${PIDS} $!"
done

sleep 2
ORCH_ARGS=( -m app.orchestrator.orchestrator --config configs/experiment_baseline.yaml )
if [[ -n "${FL_ROUNDS:-}" ]]; then
  ORCH_ARGS+=( --rounds "${FL_ROUNDS}" )
fi
"${PYTHON_BIN}" "${ORCH_ARGS[@]}"
sleep 3
