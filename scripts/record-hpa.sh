#!/usr/bin/env bash
# Records the HPAs, pods and CPU every 15 s into docs/evidence/phase4/hpa-scaling.log (read-only kubectl).
# Start it BEFORE the load test, stop it with Ctrl+C about 5 minutes after the load test ends.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p docs/evidence/phase4
OUT=docs/evidence/phase4/hpa-scaling.log
echo "# HPA evidence, started $(date -u +%FT%TZ)" | tee "$OUT"
while true; do
  {
    echo "=== $(date -u +%T) UTC ==="
    kubectl -n s19 get hpa
    kubectl -n s19 get pods -l 'app in (incident-intake,quorum-coordinator)' --no-headers
    kubectl -n s19 top pods 2>/dev/null | grep -E "NAME|intake|coordinator" || true
  } | tee -a "$OUT"
  sleep 15
done
