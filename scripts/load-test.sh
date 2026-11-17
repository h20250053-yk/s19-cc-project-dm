#!/usr/bin/env bash
# Runs the k6 load test with the official k6 Docker image (nothing to install in Cloud Shell).
# Usage: scripts/load-test.sh [PEAK_VUS] [HOLD]      e.g. scripts/load-test.sh 40 6m
# Output is also saved to docs/evidence/phase4/k6-output.txt
set -euo pipefail
: "${IP:?export IP first (public IP of incident-intake)}"
cd "$(dirname "$0")/.."
mkdir -p docs/evidence/phase4
docker run --rm -i \
  -e BASE_URL="http://$IP" -e PEAK_VUS="${1:-40}" -e HOLD="${2:-6m}" -e CRIT_PCT="${CRIT_PCT:-1}" \
  grafana/k6:latest run - < loadtest/hpa-load.js 2>&1 | tee docs/evidence/phase4/k6-output.txt
