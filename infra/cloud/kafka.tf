# Single-broker Apache Kafka on one small VM (Container-Optimized OS + Docker).
# Fixed internal IP 10.10.0.10:9092, reachable from GKE pods and (Phase 3) from Dataproc/Flink.
# It does NOT depend on GKE or Argo CD, so a Kubernetes problem can never break Kafka/Flink.
resource "google_compute_instance" "kafka" {
  name         = "s19-kafka"
  machine_type = "e2-medium"
  zone         = var.zone
  tags         = ["kafka"]

  desired_status            = var.kafka_running ? "RUNNING" : "TERMINATED"
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
      size  = 20
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.main.id
    network_ip = var.kafka_ip
    access_config {} # external IP only so the VM can download the Kafka image from Docker Hub
  }

  service_account {
    email  = google_service_account.kafka_vm.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    startup-script         = templatefile("${path.module}/kafka-startup.sh", { kafka_ip = var.kafka_ip })
    google-logging-enabled = "true"
  }

  depends_on = [google_project_iam_member.kafka]
}
