# Zonal GKE Standard cluster. Its management fee is covered by the GKE free-tier credit
# (one zonal cluster per billing account). Nodes are normal VMs and are billed.
resource "google_container_cluster" "s19" {
  name     = "s19-gke"
  location = var.zone

  network         = google_compute_network.vpc.id
  subnetwork      = google_compute_subnetwork.main.id
  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false

  release_channel { channel = "REGULAR" }

  # Container stdout/stderr of every pod -> Cloud Logging (requirement h, Phase 4)
  logging_config { enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"] }
  # We install our own Prometheus in Phase 4; Google Managed Prometheus would bill per sample
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus { enabled = false }
  }

  depends_on = [google_project_service.apis]
}

# Fixed size, no node autoscaler: HPA scales PODS inside these 2 nodes.
# Keeps CPU usage predictable (free trial allows 8 vCPU in total).
resource "google_container_node_pool" "main" {
  name       = "main"
  cluster    = google_container_cluster.s19.id
  location   = var.zone
  node_count = var.gke_node_count

  management {
    auto_repair  = true
    auto_upgrade = true
  }
  # Upgrade by replacing one node at a time WITHOUT creating an extra node (no quota surprises)
  upgrade_settings {
    max_surge       = 0
    max_unavailable = 1
  }

  node_config {
    machine_type    = "e2-standard-2"
    disk_type       = "pd-standard"
    disk_size_gb    = 30
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = { app = "s19" }
  }
}
