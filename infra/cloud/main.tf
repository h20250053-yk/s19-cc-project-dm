# =====================================================================
# S19 Disaster Management - cloud infrastructure (requirement a)
# One Terraform folder. Run from Cloud Shell:
#   terraform init -backend-config="bucket=<PROJECT_ID>-tfstate"
#   terraform apply -var project_id=<PROJECT_ID>
# =====================================================================
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.14" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
  backend "gcs" {
    prefix = "cloud"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

data "google_project" "this" {}

# ---------------------------------------------------------------- APIs
# Phase 3 services (functions, run, eventarc, cloudbuild, dataproc) are enabled now
# so that no "API not enabled" waits happen later.
resource "google_project_service" "apis" {
  for_each = toset([
    "compute.googleapis.com",
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "sqladmin.googleapis.com",
    "servicenetworking.googleapis.com",
    "firestore.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "eventarc.googleapis.com",
    "cloudbuild.googleapis.com",
    "dataproc.googleapis.com",
    "iap.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}
