# Shortcuts for the replication (Dynamo) demo. Load them into your shell with:
#   source scripts/demo-helpers.sh
# Needs: IP (public Intake IP), ADMIN_TOKEN, and scripts/port-forward.sh running (coordinator on :8081).
C=localhost:8081

# fault <replica> <none|down|slow|drop_writes> [delayMs]   - failure injection on one replica
fault() {
  curl -s -X POST "$C/api/quorum/replicas/$1/fault" -H "X-Admin-Token: $ADMIN_TOKEN" \
    -H 'content-type: application/json' -d "{\"mode\":\"$2\",\"delayMs\":${3:-1500}}" | jq -c
}

# heal - all replicas back to normal
heal() { for r in incident-store-0 incident-store-1 incident-store-2; do fault "$r" none; done; }

# show <key> - status stored on EACH replica (raw view, bypasses the quorum)
show() {
  curl -s "$C/api/quorum/debug/$1" |
    jq -c 'map_values(if has("versions") then [.versions[].value.status] else .error end)'
}

# newinc [region] - report a new incident through the PUBLIC Intake URL; prints the full answer
newinc() {
  curl -s -X POST "http://$IP/api/incidents" -H 'content-type: application/json' -d "{
    \"reporterId\":\"demo-$RANDOM\",\"type\":\"FLOOD\",\"severity\":\"HIGH\",\"region\":\"${1:-north}\",
    \"description\":\"phase 2 demo incident\",\"latitude\":28.6,\"longitude\":77.2,\"peopleAffected\":10}"
}

# ae - run one anti-entropy (replica sync) round now instead of waiting for the 30 s timer
ae() { curl -s -X POST "$C/api/quorum/anti-entropy" -H "X-Admin-Token: $ADMIN_TOKEN" | jq -c '{differingBuckets, keysChecked, repaired}'; }

# replicas - health, fault mode and number of keys of each replica
replicas() { curl -s "$C/api/quorum/replicas" | jq -c '.replicas[] | {id, reachable, faultMode, keys}'; }

echo "demo helpers loaded: fault, heal, show, newinc, ae, replicas   (coordinator=$C, IP=${IP:-NOT SET})"
