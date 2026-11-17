#!/usr/bin/env bash
# Sends a steady stream of incidents (1 every 2 s by default) so Flink's 1-minute windows keep closing.
# Usage: scripts/traffic.sh [count] [seconds-between]     (needs IP exported)
set -uo pipefail
: "${IP:?export IP first}"
N="${1:-90}"; GAP="${2:-2}"
REGIONS=(north south east west central); TYPES=(FLOOD FIRE EARTHQUAKE CYCLONE MEDICAL)
for i in $(seq 1 "$N"); do
  R=${REGIONS[$((RANDOM % 5))]}; T=${TYPES[$((RANDOM % 5))]}
  SEV=HIGH; [ $((RANDOM % 10)) -eq 0 ] && SEV=CRITICAL
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://$IP/api/incidents" -H 'content-type: application/json' \
    -d "{\"reporterId\":\"reporter-$((RANDOM % 15))\",\"type\":\"$T\",\"severity\":\"$SEV\",\"region\":\"$R\",\"description\":\"traffic $i\",\"latitude\":20,\"longitude\":78,\"peopleAffected\":$((RANDOM % 50))}")
  echo "$i/$N $R $SEV -> $code"
  sleep "$GAP"
done
