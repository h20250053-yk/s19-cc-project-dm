#!/usr/bin/env bash
# Build -> push -> pin tag in Git -> push to GitHub. Argo CD then deploys the new version.
# Builds every service listed in k8s/s19/kustomization.yaml (lines "- name: s19/<service>").
# Usage: PROJECT_ID=<id> scripts/release.sh            (all services)
#        PROJECT_ID=<id> scripts/release.sh incident-intake   (only one; others keep their tag)
set -euo pipefail
: "${PROJECT_ID:?export PROJECT_ID first}"
REGION="${REGION:-us-central1}"
cd "$(dirname "$0")/.."
KUSTOMIZATION=k8s/s19/kustomization.yaml
REG="${REGION}-docker.pkg.dev/${PROJECT_ID}/s19"
TAG="v$(date +%m%d-%H%M%S)"

if [ $# -gt 0 ]; then
  SERVICES=("$@")
else
  mapfile -t SERVICES < <(grep -oP '^\s*- name: s19/\K[a-z0-9-]+' "$KUSTOMIZATION")
fi
echo ">> services: ${SERVICES[*]}   tag: $TAG"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet >/dev/null
for s in "${SERVICES[@]}"; do
  echo ">> building $s"
  docker build -q -f services/Dockerfile --build-arg SERVICE="$s" -t "$REG/$s:$TAG" .
  docker push -q "$REG/$s:$TAG"
  # update the newTag line that follows "name: s19/<service>"
  sed -i "/name: s19\/${s}\$/,/newTag:/ s#newTag: .*#newTag: ${TAG}#" "$KUSTOMIZATION"
done

git add "$KUSTOMIZATION"
git commit -m "release ${SERVICES[*]} ${TAG}"
git push
echo ">> pushed. Argo CD will deploy ${TAG} within ~1-3 minutes."
