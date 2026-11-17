# Simple (student-level) identity model:
#  * GKE nodes run as service account s19-gke-nodes. Pods use the node's identity, so every
#    service can use Cloud Storage, Firestore and Pub/Sub without key files.
#    (Production would use Workload Identity per service - deliberately skipped here.)
#  * The Kafka VM runs as s19-kafka-vm, which may only write logs/metrics.
resource "google_service_account" "gke_nodes" {
  account_id   = "s19-gke-nodes"
  display_name = "S19 GKE nodes and pods"
  depends_on   = [google_project_service.apis]
}

resource "google_service_account" "kafka_vm" {
  account_id   = "s19-kafka-vm"
  display_name = "S19 Kafka VM"
  depends_on   = [google_project_service.apis]
}

locals {
  node_roles = [
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer",
    "roles/artifactregistry.reader",
    "roles/datastore.user",    # Firestore (Phase 2)
    "roles/pubsub.publisher",  # Pub/Sub alerts (Phase 3)
  ]
  kafka_roles = ["roles/logging.logWriter", "roles/monitoring.metricWriter"]
}

resource "google_project_iam_member" "nodes" {
  for_each = toset(local.node_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_project_iam_member" "kafka" {
  for_each = toset(local.kafka_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.kafka_vm.email}"
}

# Pods may read/write objects only in the project's raw-data bucket
resource "google_storage_bucket_iam_member" "nodes_raw" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.gke_nodes.email}"
}
