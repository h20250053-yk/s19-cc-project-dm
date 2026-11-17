#!/usr/bin/env bash
# Build the Flink job, upload it, and run it on the Dataproc cluster s19-flink (Flink on YARN).
# Prerequisite: terraform apply -var dataproc_enabled=true  (in infra/cloud)
# Usage: PROJECT_ID=<id> scripts/flink-submit.sh
set -euo pipefail
: "${PROJECT_ID:?export PROJECT_ID first}"
REGION="${REGION:-us-central1}"
CLUSTER=s19-flink
BUCKET="${PROJECT_ID}-s19-build"
cd "$(dirname "$0")/../flink/situation-analytics-flink"

echo ">> building the job (first time downloads dependencies, ~3-5 min)"
mvn -q -DskipTests package
JAR=target/situation-analytics-flink-1.0.0.jar
ls -lh "$JAR"
gcloud storage cp "$JAR" "gs://${BUCKET}/jobs/situation-analytics-flink.jar"

echo ">> stopping any older run of the job"
for id in $(gcloud dataproc jobs list --region "$REGION" --cluster "$CLUSTER" --state-filter=active --format="value(reference.jobId)"); do
  gcloud dataproc jobs kill "$id" --region "$REGION" --quiet
done

echo ">> submitting"
gcloud dataproc jobs submit flink --region "$REGION" --cluster "$CLUSTER" \
  --jar "gs://${BUCKET}/jobs/situation-analytics-flink.jar" --async \
  -- --bootstrap 10.10.0.10:9092 \
     --input-topics incident-events,dispatch-events,consistency-events \
     --output-topic incident-aggregates \
     --window-seconds 60 \
     --checkpoint-dir hdfs:///flink/checkpoints/situation-analytics
echo ">> submitted. Check: gcloud dataproc jobs list --region $REGION --cluster $CLUSTER"
