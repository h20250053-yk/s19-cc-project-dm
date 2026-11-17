# Final demonstration script (≈ 25 minutes, 4 presenters)

## T-60 minutes: preparation (one person, Cloud Shell)

```bash
cd ~/s19-dm && source ~/.bashrc && source .venv/bin/activate && git pull --rebase
cd infra/cloud && terraform apply -var dataproc_enabled=true && cd ~/s19-dm     # resume everything + Flink cluster
kubectl -n s19 get pods            # wait until all Running 1/1 (≈ 5 min after resume)
kubectl -n argocd get applications # all Synced / Healthy
scripts/flink-submit.sh            # Flink job RUNNING
scripts/tunnels.sh
source scripts/demo-helpers.sh && heal
scripts/traffic.sh 60 2            # 2 minutes of traffic so dashboards and Flink windows have data
```
Open these in separate Cloud Shell tabs (`+`), each with a background port-forward plus Web Preview:
- **Argo CD:** `kubectl -n argocd port-forward svc/argocd-server 8080:80` → Web Preview on port 8080.
- **Grafana:** `kubectl -n monitoring port-forward svc/kps-grafana 3000:80` → Web Preview, *Change port* to 3000.

Also open in the browser: the GitHub repo, Console → Dataproc → s19-flink → Web Interfaces → YARN ResourceManager (for the Flink UI), Console → Logging → Logs Explorer, and Console → Firestore.

## 0. Overview — 2 min (any member)
Show `docs/DESIGN.md` §2 and §3 (diagrams render on GitHub) and the 8-service table. Then the public URL: `curl -s http://$IP/api/incidents | jq '.items | length'`, and `http://<IP>/docs` in the browser.

## 1. Member 1 — (a) IaC + (d) GitOps · Incident Intake, Resource Registry — 5 min
1. `terraform -chdir=infra/cloud state list | head -30` → everything (VPC, GKE, VM, SQL, Firestore, buckets, Pub/Sub, function, Dataproc, IAM) is in Terraform. Run `terraform -chdir=infra/cloud plan -var dataproc_enabled=true`: it shows **No changes**.
2. GitOps: edit `k8s/s19/hpa.yaml` (incident-intake `maxReplicas: 5` → `6`), then `git commit -am "demo: GitOps" && git push`. In Argo CD click **Refresh**: the app goes OutOfSync → Synced. Then `kubectl -n s19 get hpa`. Point out that nobody ran `kubectl apply`.
3. Intake: `newinc west | jq -c '{incidentId, quorum: .quorum.acks}'`. Registry: `curl -s localhost:8082/api/resources/stats | jq .byRegion.west`.

## 2. Member 2 — (f) Leaderless replication + (g) storage · Replicated Store, Quorum Coordinator — 7 min
1. `curl -s localhost:8081/api/quorum/config | jq` → N=3, W=2, R=2, R+W>N.
2. Scenarios A (replica down + anti-entropy), B (read repair) and C (conflict merge) from `docs/PLAN.md` Step 2.5, using `fault`, `show`, `ae`.
3. Real crash: `kubectl -n s19 delete pod incident-store-1 --wait=false`, then `newinc` three times; writes still succeed.
4. `BASE_URL=http://$IP COORD_URL=http://localhost:8081 python scripts/consistency_test.py` → 14/14.
5. Storage (g): Cloud Storage bucket `raw/` and `attachments/`; Cloud SQL Studio query or `GET /api/resources/stats`; Firestore console collections.

## 3. Member 3 — (b) Serverless + communication + (e) Flink · Alert Service, Situation Analytics — 6 min
1. Report a CRITICAL incident (`docs/PLAN.md` Step 3.6, part 1). After 10 s show AUTO dispatch (`localhost:8084/api/dispatch?status=ACTIVE`) and the incident `DISPATCHED`.
2. Serverless: `curl -s localhost:8085/api/analytics/alerts?limit=3 | jq -c '.items[] | {alertType, incidentId, recipientCount}'`, plus Cloud Run → `alert-service` → Logs.
3. Name each mechanism in the communication matrix (`docs/DESIGN.md` §3.1): REST, gRPC, Kafka, Pub/Sub.
4. Flink: Dataproc job RUNNING; Flink UI job graph and checkpoints; results topic (`kafka-console-consumer … incident-aggregates`, `docs/PLAN.md` Step 3.7.4); `curl -s "localhost:8085/api/analytics/windows?limit=5" | jq -c '.items[] | {region, incidents, uniqueReporters, surge}'`.

## 4. Member 4 — (c) HPA + load test + (h) observability · Dispatch, Volunteer Coordination — 5 min
1. Dispatch and Volunteer: `curl -s localhost:8083/api/volunteers/stats | jq -c .byRegion` before and after a manual dispatch.
2. Load test (short version): tab 1 `scripts/record-hpa.sh`, tab 2 `scripts/load-test.sh 40 3m`. Show `kubectl -n s19 get hpa` and the Grafana *HPA replicas* panel climbing from 1 to several pods. Show the saved `docs/evidence/phase4/` from the full run.
3. Grafana dashboard: RPS, error rate, p95 latency, nodes/pods health, replication panels.
4. Cloud Logging: the queries from `docs/PLAN.md` Step 4.5, covering services, function, Flink job and Kafka VM.

## Afterwards
```bash
scripts/tunnels.sh stop
cd ~/s19-dm/infra/cloud && terraform apply -var gke_node_count=0 -var cloudsql_running=false -var kafka_running=false
```
