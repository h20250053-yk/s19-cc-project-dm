#!/usr/bin/env bash
# Build -> push -> pin tag in Git -> push to GitHub. Argo CD then deploys the new version.
# Images are built by Cloud Build (inside GCP), not Docker in Cloud Shell. Free tier: 2,500 build-min/month.
# Usage: scripts/release.sh                    (all services in k8s/s19/kustomization.yaml)
#        scripts/release.sh incident-intake    (only that one; others keep their tag)
set -euo pipefail
: "${PROJECT_ID:?export PROJECT_ID first}"
REGION="${REGION:-us-central1}"
cd "$(dirname "$0")/.."
KUSTOMIZATION=k8s/s19/kustomization.yaml
REG="${REGION}-docker.pkg.dev/${PROJECT_ID}/s19"
TAG="v$(date +%m%d-%H%M%S)"
if [ $# -gt 0 ]; then SERVICES=("$@"); else
  mapfile -t SERVICES < <(grep -oP '^\s*- name: s19/\K[a-z0-9-]+' "$KUSTOMIZATION"); fi
echo ">> services: ${SERVICES[*]}   tag: $TAG"
CFG=$(mktemp --suffix=.yaml)
{
  echo "steps:"
  for s in "${SERVICES[@]}"; do
    echo "- name: gcr.io/cloud-builders/docker"
    echo "  id: $s"
    echo "  waitFor: ['-']"
    echo "  args: ['build','-f','services/Dockerfile','--build-arg','SERVICE=$s','-t','$REG/$s:$TAG','.']"
  done
  echo "images:"
  for s in "${SERVICES[@]}"; do echo "- $REG/$s:$TAG"; done
  echo "timeout: 1800s"
  echo "options:"
  echo "  logging: CLOUD_LOGGING_ONLY"
} > "$CFG"
echo ">> building in Cloud Build (3-6 min)..."
gcloud builds submit --project "$PROJECT_ID" --config "$CFG" .
rm -f "$CFG"
for s in "${SERVICES[@]}"; do
  sed -i "/name: s19\/${s}\$/,/newTag:/ s#newTag: .*#newTag: ${TAG}#" "$KUSTOMIZATION"
done
git add "$KUSTOMIZATION" scripts/release.sh
git commit -m "release ${SERVICES[*]} ${TAG}"
git push
echo ">> pushed. Argo CD will deploy ${TAG} within ~1-3 minutes."
