variable "project_id" {
  type        = string
  description = "Your GCP project id"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

# ---------------------------------------------------------------- pause switches
# Pause:  terraform apply -var project_id=$PROJECT_ID -var gke_node_count=0 -var cloudsql_running=false -var kafka_running=false
# Resume: terraform apply -var project_id=$PROJECT_ID
variable "gke_node_count" {
  type        = number
  default     = 2
  description = "Number of GKE nodes (e2-standard-2). 0 = paused (cluster, disks and public IP are kept)"
}

variable "cloudsql_running" {
  type    = bool
  default = true
}

variable "kafka_running" {
  type    = bool
  default = true
}

variable "kafka_ip" {
  type        = string
  default     = "10.10.0.10"
  description = "Fixed internal IP of the Kafka VM (used by GKE pods and later by Dataproc/Flink)"
}

