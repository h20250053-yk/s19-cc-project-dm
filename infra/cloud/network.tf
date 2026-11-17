# ---------------------------------------------------------------- VPC + subnet
resource "google_compute_network" "vpc" {
  name                    = "s19-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "main" {
  name                     = "s19-subnet"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = "10.10.0.0/20" # VMs: GKE nodes, Kafka VM, (Phase 3) Dataproc
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/16" # every pod gets a real VPC IP => pods can reach the Kafka VM directly
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

# Allow all traffic between machines and pods inside the VPC (Kafka 9092, Dataproc later)
resource "google_compute_firewall" "internal" {
  name          = "s19-allow-internal"
  network       = google_compute_network.vpc.name
  source_ranges = ["10.10.0.0/20", "10.20.0.0/16"]
  allow { protocol = "tcp" }
  allow { protocol = "udp" }
  allow { protocol = "icmp" }
}

# SSH through Google's IAP proxy only (used to look inside the Kafka VM)
resource "google_compute_firewall" "iap_ssh" {
  name          = "s19-allow-iap-ssh"
  network       = google_compute_network.vpc.name
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# ---------------------------------------------------------------- private IP range for Cloud SQL
resource "google_compute_global_address" "sql_range" {
  name          = "s19-sql-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "sql" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.sql_range.name]
  deletion_policy         = "ABANDON"
}
