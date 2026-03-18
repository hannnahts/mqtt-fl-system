#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

docker compose -f docker-compose.baseline.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.proposed.yml down --remove-orphans 2>/dev/null || true

rm -rf data/processed data/splits
rm -f logs/metrics_round.csv logs/metrics_summary.csv
