#!/usr/bin/env bash
# Opens local tunnels (Cloud Shell -> cluster) to the internal services, in the background.
#   scripts/port-forward.sh        start:  coordinator -> localhost:8081
#                                           registry    -> localhost:8082
#                                           volunteers  -> localhost:8083
#   scripts/port-forward.sh stop   stop all tunnels
# Tunnels stop when Cloud Shell disconnects or the pod restarts: just run the script again.
set -uo pipefail
pkill -f "kubectl -n s19 port-forward" 2>/dev/null
[ "${1:-}" = "stop" ] && { echo "tunnels stopped"; exit 0; }
declare -A PORTS=([quorum-coordinator]=8081 [resource-registry]=8082 [volunteer-coordination]=8083)
for svc in "${!PORTS[@]}"; do
  if kubectl -n s19 get svc "$svc" >/dev/null 2>&1; then
    nohup kubectl -n s19 port-forward "svc/$svc" "${PORTS[$svc]}:8080" >/tmp/pf-$svc.log 2>&1 &
    echo "$svc -> http://localhost:${PORTS[$svc]}"
  else
    echo "$svc not deployed yet (skipped)"
  fi
done
sleep 3
for svc in "${!PORTS[@]}"; do
  curl -s -o /dev/null -w "  check $svc /healthz: %{http_code}\n" "localhost:${PORTS[$svc]}/healthz"
done
