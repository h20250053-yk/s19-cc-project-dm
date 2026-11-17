#!/usr/bin/env bash
# One-time: write your project id and GitHub URL into the GitOps files.
# Usage: scripts/setup-repo.sh <PROJECT_ID> <GITHUB_REPO_HTTPS_URL>
set -euo pipefail
PROJECT_ID="${1:?usage: setup-repo.sh <PROJECT_ID> <REPO_URL>}"
REPO_URL="${2:?usage: setup-repo.sh <PROJECT_ID> <REPO_URL>}"
cd "$(dirname "$0")/.."
sed -i "s#REPLACE_PROJECT_ID#${PROJECT_ID}#g" k8s/s19/kustomization.yaml
sed -i "s#REPLACE_REPO_URL#${REPO_URL}#g" argocd/apps/*.yaml argocd/later/*.yaml
if grep -rn "REPLACE_" k8s argocd; then
  echo "ERROR: placeholders left (see above)"; exit 1
fi
echo "OK - project and repo URL written into k8s/ and argocd/"
