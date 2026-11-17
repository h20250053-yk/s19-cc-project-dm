# S19 Disaster Management — minimal GCP implementation

CS/SS G527 Cloud Computing · Scenario S19 · Concept: leaderless (Dynamo-style) replication · Cloud: GCP.
Everything is done from **Google Cloud Shell** (browser only). Design document: [docs/DESIGN.md](docs/DESIGN.md).

## Plan: 4 phases, 2 requirements each — full guide: [docs/PLAN.md](docs/PLAN.md)

| Phase | Requirements | Services | Lead |
|---|---|---|---|
| 1 Foundation | (a) IaC, (d) GitOps | Incident Intake, Replicated Incident Store ×3, Quorum Coordinator | Member 1 |
| 2 Data and concept | (f) leaderless replication, (g) 3 storage products | Resource Registry, Volunteer Coordination | Member 2 |
| 3 Events and streaming | (b) serverless + REST/gRPC/queues, (e) Flink | Dispatch, Alert Service, Situation Analytics | Member 3 |
| 4 Scale, observe, submit | (c) GKE + HPA + load test, (h) observability | — | Member 4 |

Demo day: [docs/DEMO.md](docs/DEMO.md) · Team split: [task_divison.txt](task_divison.txt)

## Layout

```
infra/cloud/        Terraform: all cloud resources (phase3.tf.off is renamed to phase3.tf in Phase 3)
infra/platform/     Terraform: Argo CD + cluster config
argocd/apps/        Argo CD applications watched by the root app (s19-services)
argocd/later/       monitoring apps, moved into argocd/apps/ in Phase 4
k8s/s19/            the 7 containerised services + HPAs (deployed by Argo CD)
k8s/monitoring/     ServiceMonitor + Grafana dashboard
services/           Python microservices (one Dockerfile) + alert-function (Cloud Run function)
flink/              Flink job (Dataproc)
proto/              gRPC contract
tests/              replication tests (pytest)
loadtest/           k6 script
scripts/            setup-repo, release, add-services, tunnels, demo-helpers, consistency_test,
                    flink-submit, traffic, load-test, record-hpa
docs/               PLAN.md (the guide), DESIGN.md, DEMO.md, evidence
```

## Everyday commands (Cloud Shell)

```bash
cd ~/s19-dm && source ~/.bashrc && source .venv/bin/activate
cd infra/cloud && terraform apply && cd ~/s19-dm          # resume
scripts/tunnels.sh                                        # internal services on localhost:8081-8085
scripts/release.sh <service>                              # build -> push -> Git -> Argo CD deploys
cd infra/cloud && terraform apply -var gke_node_count=0 -var cloudsql_running=false -var kafka_running=false   # pause
```
Rule: never `kubectl apply` a service. Change files, commit, push, and Argo CD deploys.
