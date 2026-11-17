# ---------------------------------------------------------------- Artifact Registry (container images)
resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "s19"
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

# ---------------------------------------------------------------- Storage product 1: Cloud Storage
# raw/incidents/...  NDJSON batches of every raw incident report (written by Incident Intake)
resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-s19-raw"
  location                    = upper(var.region) # us-central1: inside the Always Free storage regions
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }
  depends_on = [google_project_service.apis]
}

# ---------------------------------------------------------------- Storage product 2: Cloud SQL
# Used in Phase 2 by Resource Registry and Dispatch. Created now because it takes ~10 min.
resource "random_password" "db" {
  length  = 20
  special = false
}

resource "google_sql_database_instance" "pg" {
  name                = "s19-pg"
  region              = var.region
  database_version    = "POSTGRES_15"
  deletion_protection = false

  settings {
    tier              = "db-f1-micro" # cheapest tier (no free tier exists for Cloud SQL)
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_type         = "PD_HDD"
    disk_size         = 10
    disk_autoresize   = false
    activation_policy = var.cloudsql_running ? "ALWAYS" : "NEVER"

    ip_configuration {
      ipv4_enabled    = false # private IP only: reachable from GKE pods, not from the internet
      private_network = google_compute_network.vpc.id
    }
    backup_configuration { enabled = false }
  }
  depends_on = [google_service_networking_connection.sql]
}

resource "google_sql_database" "s19" {
  name     = "s19"
  instance = google_sql_database_instance.pg.name
}

resource "google_sql_user" "app" {
  name     = "s19app"
  instance = google_sql_database_instance.pg.name
  password = random_password.db.result
}

# ---------------------------------------------------------------- Storage product 3: Firestore
# Used in Phase 2-3 (volunteers, alerts, analytics results).
resource "google_firestore_database" "default" {
  name                    = "(default)"
  location_id             = "nam5" # United States multi-region
  type                    = "FIRESTORE_NATIVE"
  delete_protection_state = "DELETE_PROTECTION_DISABLED"
  deletion_policy         = "DELETE"
  depends_on              = [google_project_service.apis]
}

# ---------------------------------------------------------------- Pub/Sub (message queue for alerts, Phase 3)
resource "google_pubsub_topic" "alerts" {
  name                       = "alert-requests"
  message_retention_duration = "86400s"
  depends_on                 = [google_project_service.apis]
}
