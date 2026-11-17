output "cluster_name" { value = google_container_cluster.s19.name }
output "zone" { value = var.zone }
output "region" { value = var.region }
output "image_registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}
output "raw_bucket" { value = google_storage_bucket.raw.name }
output "kafka_bootstrap" { value = "${var.kafka_ip}:9092" }
output "alert_topic" { value = google_pubsub_topic.alerts.name }
output "db_private_ip" { value = google_sql_database_instance.pg.private_ip_address }
output "db_name" { value = google_sql_database.s19.name }
output "db_user" { value = google_sql_user.app.name }
output "db_password" {
  value     = random_password.db.result
  sensitive = true
}
