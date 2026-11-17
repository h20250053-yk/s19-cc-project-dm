# =====================================================================
# S19 - cluster bootstrap (requirements a + d)
# Creates: namespace s19, ConfigMap s19-config, Secret s19-secrets, Argo CD,
#          and the root Argo CD application that tracks the argocd/apps folder in GitHub.
# After this, every Kubernetes workload is deployed by Argo CD from Git (never kubectl apply).
#   terraform init -backend-config="bucket=<PROJECT_ID>-tfstate"
#   terraform apply -var project_id=<PROJECT_ID> -var git_repo_url=https://github.com/<you>/s19-dm.git
# =====================================================================
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google     = { source = "hashicorp/google", version = "~> 6.14" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.35" }
    helm       = { source = "hashicorp/helm", version = "~> 2.17" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
  backend "gcs" {
    prefix = "platform"
  }
}

variable "project_id" { type = string }
variable "git_repo_url" {
  type        = string
  description = "HTTPS URL of your GitHub repo, e.g. https://github.com/you/s19-dm.git"
}

# Read the outputs of infra/cloud (same state bucket, prefix "cloud")
data "terraform_remote_state" "cloud" {
  backend = "gcs"
  config = {
    bucket = "${var.project_id}-tfstate"
    prefix = "cloud"
  }
}

locals {
  c = data.terraform_remote_state.cloud.outputs
}

provider "google" {
  project = var.project_id
  region  = local.c.region
}

data "google_client_config" "me" {}

data "google_container_cluster" "s19" {
  name     = local.c.cluster_name
  location = local.c.zone
}

provider "kubernetes" {
  host                   = "https://${data.google_container_cluster.s19.endpoint}"
  token                  = data.google_client_config.me.access_token
  cluster_ca_certificate = base64decode(data.google_container_cluster.s19.master_auth[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = "https://${data.google_container_cluster.s19.endpoint}"
    token                  = data.google_client_config.me.access_token
    cluster_ca_certificate = base64decode(data.google_container_cluster.s19.master_auth[0].cluster_ca_certificate)
  }
}

# ---------------------------------------------------------------- namespace + configuration for the services
resource "kubernetes_namespace_v1" "s19" {
  metadata { name = "s19" }
}

resource "kubernetes_config_map_v1" "config" {
  metadata {
    name      = "s19-config"
    namespace = kubernetes_namespace_v1.s19.metadata[0].name
  }
  data = {
    GCP_PROJECT_ID  = var.project_id
    RAW_BUCKET      = local.c.raw_bucket
    KAFKA_BOOTSTRAP = local.c.kafka_bootstrap
    ALERT_TOPIC     = local.c.alert_topic
    DB_HOST         = local.c.db_private_ip
    DB_NAME         = local.c.db_name
    DB_USER         = local.c.db_user
  }
}

resource "random_password" "admin_token" {
  length  = 16
  special = false
}

resource "kubernetes_secret_v1" "secrets" {
  metadata {
    name      = "s19-secrets"
    namespace = kubernetes_namespace_v1.s19.metadata[0].name
  }
  data = {
    DB_PASSWORD = local.c.db_password
    ADMIN_TOKEN = random_password.admin_token.result # protects the fault-injection API (Phase 2)
  }
}

# ---------------------------------------------------------------- Argo CD
resource "helm_release" "argocd" {
  name             = "argocd"
  namespace        = "argocd"
  create_namespace = true
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "9.5.15"
  timeout          = 900
  values = [yamlencode({
    configs       = { params = { "server.insecure" = true } }
    dex           = { enabled = false }
    notifications = { enabled = false }
  })]
}

# Root "app of apps": watches argocd/apps/ in your repo. Every YAML file you add there
# (Phase 1: s19-services, Phase 4: monitoring) becomes an Argo CD application automatically.
resource "helm_release" "root_app" {
  name       = "s19-root"
  namespace  = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argocd-apps"
  version    = "2.0.4"
  values = [yamlencode({
    applications = {
      s19-root = {
        namespace = "argocd"
        project   = "default"
        source = {
          repoURL        = var.git_repo_url
          targetRevision = "main"
          path           = "argocd/apps"
        }
        destination = { server = "https://kubernetes.default.svc", namespace = "argocd" }
        syncPolicy  = { automated = { prune = true, selfHeal = true } }
      }
    }
  })]
  depends_on = [helm_release.argocd]
}

output "admin_token" {
  value     = random_password.admin_token.result
  sensitive = true
}
