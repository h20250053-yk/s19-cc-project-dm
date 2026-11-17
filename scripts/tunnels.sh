#!/usr/bin/env bash
# Local tunnels (Cloud Shell -> cluster) to ALL internal services, in the background.
# Replaces scripts/port-forward.sh from Phase 2 (same ports + two more).
#   scripts/tunnels.sh        start     scripts/tunnels.sh stop     stop
# Tunnels stop when Cloud Shell disconnects or a pod restarts: just run the script again.
set -uo pipefail
pkill -f "kubectl -n s19 port-forward" 2>/dev/null
[ "${1:-}" = "stop" ] && { echo "tunnels stopped"; exit 0; }
declare -A PORTS=([quorum-coordinator]=8081 [resource-registry]=8082 [volunteer-coordination]=8083 \
                  [dispatch]=8084 [situation-analytics]=8085)
for svc in quorum-coordinator resource-registry volunteer-coordination dispatch situation-analytics; do
  if kubectl -n s19 get svc "$svc" >/dev/null 2>&1; then
    nohup kubectl -n s19 port-forward "svc/$svc" "${PORTS[$svc]}:8080" >/tmp/pf-$svc.log 2>&1 &
  else
    echo "$svc not deployed yet (skipped)"
  fi
done
sleep 3
for svc in quorum-coordinator resource-registry volunteer-coordination dispatch situation-analytics; do
  curl -s -o /dev/null -w "$svc -> http://localhost:${PORTS[$svc]}   /healthz: %{http_code}\n" "localhost:${PORTS[$svc]}/healthz"
done
