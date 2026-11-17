#!/usr/bin/env bash
# Register new services in k8s/s19/kustomization.yaml (safe to run twice):
#   - adds  "- <service>.yaml"            to resources:
#   - adds  "- name: s19/<service> ..."   to images: (tag "none" until scripts/release.sh builds it)
# Usage: PROJECT_ID=<id> scripts/add-services.sh resource-registry volunteer-coordination
set -euo pipefail
: "${PROJECT_ID:?export PROJECT_ID first}"
REGION="${REGION:-us-central1}"
cd "$(dirname "$0")/.."
K=k8s/s19/kustomization.yaml
for s in "$@"; do
  [ -f "k8s/s19/$s.yaml" ] || { echo "ERROR: k8s/s19/$s.yaml not found"; exit 1; }
  [ -d "services/$s/app" ] || { echo "ERROR: services/$s/app not found"; exit 1; }
  if ! grep -q "^  - $s.yaml\$" "$K"; then
    sed -i "/^  - hpa.yaml\$/i\\  - $s.yaml" "$K"
    echo "added resource $s.yaml"
  fi
  if ! grep -q "name: s19/$s\$" "$K"; then
    printf '  - name: s19/%s\n    newName: %s-docker.pkg.dev/%s/s19/%s\n    newTag: none\n' \
      "$s" "$REGION" "$PROJECT_ID" "$s" >> "$K"
    echo "added image s19/$s"
  fi
done
echo "----- $K -----"
cat "$K"
