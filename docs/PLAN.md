# S19 Disaster Management on GCP — Complete Plan (Phases 1–4, from scratch)

CS/SS G527 Cloud Computing · Scenario **S19 Disaster Management** · Concept **Leaderless (Dynamo-style) replication** · Cloud **Google Cloud Platform**

This single guide takes you from **nothing** (no Google Cloud account, no repo) to a deployed, tested and demonstrated system covering **all 8 microservices** and **all 8 requirements (a)–(h)**. Follow it top to bottom.

- **You need:** a web browser and the file **`s19-dm-start.zip`**. Nothing is installed on Windows. Every command runs in **Google Cloud Shell**, a Linux terminal inside the Google Cloud website that already has `gcloud`, `terraform`, `kubectl`, `docker`, `git`, `python3`, `java`, `maven` and `jq`.
- **Steps are numbered `Step <phase>.<step>`**, e.g. Step 2.5 = Phase 2, step 5. Unless a step says *browser*, run its commands in **Cloud Shell**.

---

## The plan at a glance

| Phase | Requirements | Services built | Lead | Time | Done when |
|---|---|---|---|---|---|
| **1 Foundation** | (a) IaC · (d) GitOps | Incident Intake (public URL), Replicated Incident Store ×3, Quorum Coordinator | Member 1 | 3–4 h | Public URL stores an incident with `acks: 2`; Argo CD Synced |
| **2 Data and concept** | (f) leaderless replication · (g) 3 storage products | Resource Registry (Cloud SQL), Volunteer Coordination (Firestore) | Member 2 | 2–3 h | Replication scenarios A–E shown; 14/14 consistency checks; data in all 3 storage products |
| **3 Events and streaming** | (b) serverless + REST/gRPC/queues · (e) Flink | Dispatch, Alert Service (Cloud Run function), Situation Analytics (Flink on Dataproc + API) | Member 3 | 3–4 h | CRITICAL incident → Kafka → auto-dispatch → Pub/Sub → function; Flink 1-minute results on the results topic |
| **4 Scale, observe, submit** | (c) HPA + load test · (h) observability | — | Member 4 | 3 h + docs | Grafana dashboard live; k6 shows HPA scale-out; logs from every component; documents submitted |

Each member owns **2 requirements and 2 services** (the assignment's rule). The full split is in `task_divison.txt`:

| Member | Requirements | Services |
|---|---|---|
| 1 | (a), (d) | Incident Intake, Resource Registry |
| 2 | (f), (g) | Replicated Incident Store, Quorum Coordinator |
| 3 | (b), (e) | Alert Service, Situation Analytics |
| 4 | (c), (h) | Dispatch, Volunteer Coordination |

The phases are the **order things are connected and tested in the cloud**. Members can read and change their services' code at any time.

## Architecture in one picture

```
Browser / k6 ──HTTP──► [LoadBalancer] ─► Incident Intake ──REST──► Quorum Coordinator ══gRPC══► incident-store-0/1/2 (3 disks)
                                              │  │                        │
                       Kafka VM 10.10.0.10 ◄──┘  └──► Pub/Sub ──► Alert Service (Cloud Run function) ──► Firestore
                          ▲   │                          ▲
              Dispatch ───┘   ├──► Flink job on Dataproc ──► results topic ──► Situation Analytics API ──► Firestore
   (Kafka consumer, REST to   │
    Registry/Volunteer/Coord) └── (incident-events, dispatch-events, consistency-events)
   Resource Registry, Dispatch ──► Cloud SQL       Volunteer Coordination ──► Firestore      Intake ──► Cloud Storage
   GKE cluster (2 nodes): the 7 container services + Argo CD + Prometheus/Grafana     Logs of everything ──► Cloud Logging
```
Full diagrams and rationale: `docs/DESIGN.md` (the design-document deliverable).

## How the repository is staged

Everything is in the repo from the start. Parts that belong to later phases are **switched off** until their phase:

| Part | Switched off by | Switched on in |
|---|---|---|
| Resource Registry, Volunteer Coordination | not listed in `k8s/s19/kustomization.yaml` | Step 2.2 (`scripts/add-services.sh`) |
| Dispatch, Situation Analytics API | not listed in `k8s/s19/kustomization.yaml` | Step 3.4 |
| Alert function, Dataproc, build bucket (Terraform) | file named `infra/cloud/phase3.tf.off` (Terraform reads only `*.tf`) | Step 3.1 (`git mv … phase3.tf`) |
| Prometheus + Grafana (Argo CD apps) | files in `argocd/later/` (Argo CD watches only `argocd/apps/`) | Step 4.1 (`git mv … argocd/apps/`) |

```
s19-dm/
├── infra/cloud/        Terraform: VPC, GKE, Kafka VM, Artifact Registry, Cloud Storage, Cloud SQL, Firestore,
│                       Pub/Sub, IAM  (+ phase3.tf.off: Alert function, Dataproc)
├── infra/platform/     Terraform: namespace, config, secrets, Argo CD + root app
├── argocd/apps/        Argo CD applications (watched)      argocd/later/  monitoring apps (Phase 4)
├── k8s/s19/            Kubernetes manifests of the 7 container services + HPAs (deployed by Argo CD)
├── k8s/monitoring/     ServiceMonitor + Grafana dashboard
├── services/           Python microservices (one shared Dockerfile), alert-function/ (serverless)
├── flink/              Flink job (Java, Maven)
├── proto/              gRPC contract           tests/   replication tests (pytest)
├── loadtest/           k6 script
├── scripts/            setup-repo, release, add-services, tunnels, demo-helpers, consistency_test,
│                       flink-submit, traffic, load-test, record-hpa, gen_proto
└── docs/               PLAN.md (this guide), DESIGN.md, DEMO.md, evidence/
```

## Every working session: start and end

**Start** (Cloud Shell; after a pause, wait ~5 min until all pods are Running):
```bash
cd ~/s19-dm && source ~/.bashrc && source .venv/bin/activate && git pull --rebase
cd infra/cloud && terraform apply && cd ~/s19-dm                 # resume (only needed if you paused)
kubectl -n s19 get pods
```
**End**, i.e. pause. The cluster, disks, data and public IP are kept; only the costly running parts stop:
```bash
cd ~/s19-dm/infra/cloud && terraform apply -var gke_node_count=0 -var cloudsql_running=false -var kafka_running=false && cd ~/s19-dm
```
Only **one person runs `terraform`** at a time (normally the Phase 1 lead). Teammates use `kubectl`, `gcloud` and Git.

## Money

| Mode | Approx. cost |
|---|---|
| Running (2 GKE nodes, Kafka VM, Cloud SQL, load balancer, disks) | about $0.21/h (about $5/day) |
| + Dataproc (Phase 3 Flink work and demo day only) | + about $0.09/h |
| Paused | about $0.04/h (about $1/day) |

Cloud Storage, Firestore, Pub/Sub, Cloud Run functions and Cloud Logging stay inside their free quotas at this scale. Everything comes out of the **$300 free-trial credit**; expect roughly $25–40 for the project if you pause when not working. Never click "Upgrade / Activate full account".

## What has been tested, and what you are running for the first time

| Already run and passed | First real run is yours |
|---|---|
| 8/8 replication tests with 3 real gRPC replicas | All Terraform (Phases 1 and 3), Docker builds, Argo CD |
| 14/14 consistency checks + all Phase 2 scenarios A–D (outputs in the guide are real) | Volunteer Coordination (no Firestore emulator was available) |
| Resource Registry on real PostgreSQL (incl. 8 parallel reservations, no double-booking) | Kafka on the VM, Pub/Sub → function, Dataproc |
| Dispatch end to end (Postgres + Registry + Coordinator + replicas + Intake) | **The Flink job: never compiled** (Maven Central was blocked), so it's compiled early in Step 1.15 and again in Step 3.2 |
| Alert function logic (a crash bug was found and fixed) | Prometheus/Grafana chart, dashboard queries, k6 run |

Every step ends with an **Expected** result and a table of likely errors. **When a step fails:** send the step number and the last ~20 lines of output.

---

# PHASE 1 — Foundation: Infrastructure as Code (a) + GitOps (d)

**Lead:** Member 1 · **Services:** Incident Intake (public URL), Replicated Incident Store ×3, Quorum Coordinator · **Time:** 3–4 h (about half is waiting)

### What Phase 1 delivers

At the end of this phase:
1. **One Terraform folder (`infra/cloud`)** creates the cloud infrastructure for the whole project (only the Alert function and Dataproc are added in Phase 3, from the same folder):
   - network (VPC, subnet, firewall rules);
   - GKE cluster with 2 nodes;
   - Artifact Registry for container images;
   - Kafka VM;
   - Cloud Storage bucket, Cloud SQL, Firestore and the Pub/Sub topic;
   - service accounts and their permissions.
2. **A second small Terraform folder (`infra/platform`)** installs Argo CD and the cluster configuration.
3. **Argo CD** reads your GitHub repo and deploys the 3 services and 2 HPAs. You never run `kubectl apply`.
4. **A public URL** stores an incident on the 3 replicas with a quorum write (W=2).

**Phase 1 is done when** a `curl` to `http://<PUBLIC-IP>/api/incidents` returns `"acks": 2` or `3`, and Argo CD shows **Synced / Healthy**.

> Honest status: the three Python services and their replication logic are the same code that passed 8/8 tests and a 14/14 local consistency run. The Terraform, the Docker builds and the Argo CD setup are new and have **not** been run against Google Cloud yet. Each step below has a check, so if something fails you know exactly where.

### Step 1.1 — Google Cloud account and Cloud Shell (10 min, browser)

1. Go to **https://console.cloud.google.com** and sign in with a Google account.
2. Click **Activate / Start free trial**: $300 credit for 90 days. Follow the verification steps. **Never click "Upgrade" / "Activate full account".** On the trial you are not charged real money.
3. Click the **`>_` (Activate Cloud Shell)** icon at the top right. A terminal opens at the bottom of the page. Click **Open in new window** (square-arrow icon) for more room.

**Cloud Shell tips (read once):**
- **Files:** your home folder (`~`) is kept between sessions. Running programs are not; they stop when the session ends.
- **Disconnects:** the session ends after some idle time. If it disconnects during a long command, reopen it and re-run that command. Every command in this guide is safe to re-run.
- **Usage limit:** Cloud Shell has a weekly usage quota (about 50 hours). Close the tab when you're not working.
- **Editor:** the **Open Editor** button gives you a VS Code-like editor for the files.
- **Upload:** use **⋮ (More) → Upload**.
- **Web Preview:** the eye icon. It opens a web page served from Cloud Shell (used for the Argo CD UI).
- **Pasting:** `Ctrl+V` works in the terminal. If the browser asks to allow clipboard access, allow it.

### Step 1.2 — Create the project and link billing (5 min, Cloud Shell)

Choose a **project ID**: globally unique, 6–30 characters, lowercase letters, digits and hyphens, starting with a letter. Example: `s19-dm-` plus 6 random digits.

```bash
export PROJECT_ID=s19-dm-$RANDOM$RANDOM          # or type your own id
echo "export PROJECT_ID=$PROJECT_ID" >> ~/.bashrc
echo "export REGION=us-central1 ZONE=us-central1-a" >> ~/.bashrc
source ~/.bashrc && echo $PROJECT_ID

gcloud projects create $PROJECT_ID --name="S19 Disaster Mgmt"
gcloud config set project $PROJECT_ID
gcloud billing accounts list
```
**Expected:** the last command prints a table with an `ACCOUNT_ID` like `01ABCD-23EF45-67GH89` and `OPEN True`. Link it:
```bash
gcloud billing projects link $PROJECT_ID --billing-account=<ACCOUNT_ID>
```
**Expected:** `billingEnabled: true`.

| Problem | Fix |
|---|---|
| `The project ID you specified is already in use` | Choose another ID, then fix the line in `~/.bashrc` (`nano ~/.bashrc`) |
| `Cloud billing quota exceeded` | Too many projects on the trial account; unlink an old one in Console → Billing → Account management |
| Cloud Shell asks "Authorize Cloud Shell" | Click **Authorize** |

### Step 1.3 — Give teammates access (2 min)

Each member must demo their own part, so each needs access to the project:
```bash
gcloud projects add-iam-policy-binding $PROJECT_ID --member=user:teammate1@gmail.com --role=roles/editor
```
Repeat for each teammate. Teammates open their own Cloud Shell and run `gcloud config set project <PROJECT_ID>`.

**Rule:** only **one person runs `terraform`** (the Phase 1 lead). Others use `kubectl`, `gcloud` and Git.

### Step 1.4 — GitHub repository (5 min, browser + Cloud Shell)

1. On **github.com → New repository**: name **`s19-dm`**, **Public**, and **don't** tick README, .gitignore or license, so the repo is empty. Public is simplest because Argo CD can read it without a password.
2. Create a token so Cloud Shell can push: GitHub → your photo → **Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token (classic)**. Tick **`repo`**, set an expiry after your demo date, then **Generate** and **copy the token** (it starts with `ghp_`).
3. In Cloud Shell:
```bash
git config --global user.name  "Your Name"
git config --global user.email "you@example.com"
git config --global credential.helper store      # remembers the token after the first push
git config --global init.defaultBranch main
echo 'export REPO_URL=https://github.com/<your-github-user>/s19-dm.git' >> ~/.bashrc   # edit <your-github-user>
source ~/.bashrc && echo $REPO_URL
```

### Step 1.5 — Upload the code and push it (5 min)

1. Cloud Shell → **⋮ → Upload** → select `s19-dm-start.zip`.
2. Then:
```bash
cd ~ && unzip -q s19-dm-start.zip && cd ~/s19-dm
chmod +x scripts/*.sh
scripts/setup-repo.sh $PROJECT_ID $REPO_URL
```
**Expected:** `OK - project and repo URL written into k8s/ and argocd/`.

```bash
git init
git add .
git commit -m "S19 Disaster Management: initial code"
git remote add origin $REPO_URL
git push -u origin main
```
When asked, **Username** = your GitHub username and **Password** = the **token** from Step 1.4, not your GitHub password. **Expected:** the files appear on GitHub.

| Problem | Fix |
|---|---|
| `Authentication failed` | You pasted your GitHub password; use the `ghp_…` token |
| `rejected … fetch first` | The repo wasn't empty: `git pull --rebase origin main`, then push again |
| `setup-repo.sh: Permission denied` | `chmod +x scripts/*.sh` |

### Step 1.6 — Run the replication tests (3 min)

This checks that the replication code works in Cloud Shell before anything is deployed.
```bash
cd ~/s19-dm
python3 -m venv .venv && source .venv/bin/activate
pip install -q -r services/requirements.txt pytest
bash scripts/gen_proto.sh && pytest -q tests/
```
**Expected:** `8 passed`. These tests start 3 real replica servers and check:
- quorum writes and reads;
- one replica down, then recovery;
- read repair;
- conflicting writes and their merge;
- quorum loss;
- a slow replica.

If `pip install` fails on one package, send me the last 20 lines.

### Step 1.7 — Create all cloud infrastructure with Terraform (≈ 25 min)

#### 1.7.1 State bucket
Terraform keeps a record ("state") of what it created. We store that record in a bucket, which is the only thing created by a plain command instead of Terraform:
```bash
gcloud services enable storage.googleapis.com
gcloud storage buckets create gs://$PROJECT_ID-tfstate --location=us-central1 --uniform-bucket-level-access
```

#### 1.7.2 Variables
```bash
cd ~/s19-dm/infra/cloud
echo "project_id = \"$PROJECT_ID\"" > terraform.tfvars
cat terraform.tfvars
```

#### 1.7.3 Init, plan, apply
```bash
terraform init -backend-config="bucket=$PROJECT_ID-tfstate"
terraform plan
terraform apply
```
- **`init`** downloads the Google provider. Expected: `Terraform has been successfully initialized!`
- **`plan`** lists what will be created and ends with `Plan: <N> to add, 0 to change, 0 to destroy.` You should see `google_container_cluster.s19`, `google_container_node_pool.main`, `google_compute_instance.kafka`, `google_sql_database_instance.pg`, `google_firestore_database.default`, `google_storage_bucket.raw`, `google_pubsub_topic.alerts` and `google_artifact_registry_repository.images`.
- **`apply`** asks `Enter a value:`. Type **`yes`**. It takes **15–25 minutes**; Cloud SQL and GKE are the slow ones. Keep the tab open.
- **Expected:** `Apply complete! Resources: <N> added, 0 changed, 0 destroyed.` followed by outputs:
```
cluster_name    = "s19-gke"
image_registry  = "us-central1-docker.pkg.dev/<PROJECT_ID>/s19"
kafka_bootstrap = "10.10.0.10:9092"
raw_bucket      = "<PROJECT_ID>-s19-raw"
db_private_ip   = "10.x.x.x"
...
```

**If apply fails, read the error and re-run `terraform apply`.** It only creates what is still missing.

| Error contains | What it means / fix |
|---|---|
| `SERVICE_DISABLED`, `has not been used in project … before or it is disabled` | APIs were enabled seconds ago; wait 2 minutes, run `terraform apply` again |
| `Quota 'CPUS' exceeded` or `IN_USE_ADDRESSES` | Free-trial limit. Check nothing else is running: `gcloud compute instances list`. Phase 1 uses 6 vCPU (2 nodes × 2 + Kafka 2) |
| `servicenetworking` / `Error waiting for Create Service Networking Connection` | Private network for Cloud SQL not ready yet; re-run apply |
| `Error 409 … database already exists` (Firestore) | `terraform import google_firestore_database.default "projects/$PROJECT_ID/databases/(default)"`, then apply |
| `Error acquiring the state lock` | An earlier run was cut off (e.g. Cloud Shell disconnected). Run `terraform force-unlock <LOCK ID shown in the message>`, then apply |
| `googleapi: Error 403 … billing` | Billing not linked; redo Step 1.2 |

#### 1.7.4 Check in the Console (optional, 2 min)
Console → **Kubernetes Engine** (`s19-gke`, 2 nodes), **Compute Engine → VM instances** (`s19-kafka` plus 2 GKE nodes), **SQL** (`s19-pg`), **Firestore**, **Cloud Storage** (`…-s19-raw`, `…-tfstate`), **Pub/Sub** (`alert-requests`), **Artifact Registry** (`s19`).

### Step 1.8 — Check Kafka is running (3 min)

The Kafka VM starts Kafka by itself with `infra/cloud/kafka-startup.sh`. Wait about 2 minutes after the apply, then:
```bash
gcloud compute ssh s19-kafka --zone $ZONE --tunnel-through-iap --command "sudo docker ps"
```
The first time, it asks to create an SSH key: press **Enter** twice (empty passphrase).
**Expected:** one line with `apache/kafka:3.8.0` and `Up … `.

```bash
gcloud compute ssh s19-kafka --zone $ZONE --tunnel-through-iap \
  --command "sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.10.0.10:9092 --list"
```
**Expected:** no error. The list may be empty; topics are created automatically when the first event is sent in Step 1.12.

| Problem | Fix |
|---|---|
| `docker ps` shows nothing | Look at the startup log: `gcloud compute ssh s19-kafka --zone $ZONE --tunnel-through-iap --command "sudo journalctl -u google-startup-scripts -n 50 --no-pager"`. Fix, then `gcloud compute instances reset s19-kafka --zone $ZONE` (reboots and re-runs the script) |
| `failed to connect to backend` / IAP error | Wait 1–2 min after apply (firewall + IAP API), retry |

### Step 1.9 — Connect kubectl to the cluster (1 min)
```bash
gcloud container clusters get-credentials s19-gke --zone $ZONE --project $PROJECT_ID
kubectl get nodes
```
**Expected:** 2 nodes `Ready`.

### Step 1.10 — Build and push the three images (≈ 10 min)

```bash
cd ~/s19-dm
scripts/release.sh
```
**What it does** (`scripts/release.sh`):
1. Reads the three services listed in `k8s/s19/kustomization.yaml`.
2. Builds each image from `services/Dockerfile` (the gRPC code is generated inside the build).
3. Pushes them to Artifact Registry with a tag like `v1117-153012`.
4. Writes that tag into `k8s/s19/kustomization.yaml`.
5. Runs `git commit` and `git push`.

**Expected:** `>> building …` three times, then `>> pushed. Argo CD will deploy …`.
**Check:**
```bash
gcloud artifacts docker images list $REGION-docker.pkg.dev/$PROJECT_ID/s19 --format="value(package)" | sort -u
grep newTag k8s/s19/kustomization.yaml       # 3 lines with the same v… tag, none says "none"
```

| Problem | Fix |
|---|---|
| `denied: Permission "artifactregistry.repositories.uploadArtifacts"` | Run `gcloud auth configure-docker $REGION-docker.pkg.dev`, then re-run |
| `name unknown: Repository "s19" not found` | Step 1.7 did not finish; check `terraform output image_registry` |
| `git push` rejected | `git pull --rebase` then `git push` |
| Build error mentioning a Python package | Send me the error lines |

### Step 1.11 — Install Argo CD and hand deployment over to Git (≈ 10 min)

```bash
cd ~/s19-dm/infra/platform
cat > terraform.tfvars <<EOF
project_id   = "$PROJECT_ID"
git_repo_url = "$REPO_URL"
EOF
terraform init -backend-config="bucket=$PROJECT_ID-tfstate"
terraform apply
```
Type `yes`. **Takes about 5 minutes.**
**Creates** (`infra/platform/main.tf`):
- namespace `s19`;
- ConfigMap `s19-config` (project, bucket, Kafka address, database IP);
- Secret `s19-secrets` (database password, admin token);
- Argo CD;
- the root application `s19-root`, which watches `argocd/apps/` in your repo.

**Expected:** `Apply complete! Resources: 6 added`.

Watch Argo CD deploy the services:
```bash
kubectl -n argocd get applications
kubectl -n s19 get pods -w          # Ctrl+C to stop watching
```
**Expected within ~3–5 min:** applications `s19-root` and `s19-services` with `Synced` / `Healthy`. In namespace `s19`, all `Running` and `1/1`:
- `incident-store-0`, `incident-store-1`, `incident-store-2`;
- one `quorum-coordinator-…`;
- one `incident-intake-…`.

**Open the Argo CD web UI:**
```bash
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d; echo
kubectl -n argocd port-forward svc/argocd-server 8080:80
```
Then Cloud Shell **Web Preview (eye icon) → Preview on port 8080**. Log in as `admin` with the password printed above. Press `Ctrl+C` in the terminal to stop the port-forward.

| Problem | Fix |
|---|---|
| Only `s19-root` exists, no `s19-services` | `kubectl -n argocd describe application s19-root`. Usually the repo URL is wrong or the repo is private. Check `git_repo_url` in `terraform.tfvars` and that the repo is Public. Then click **Refresh** in the UI |
| Pods `ImagePullBackOff` | The tag in `kustomization.yaml` is `none` or the project id is wrong. Re-run Step 1.10 |
| Pods `CrashLoopBackOff` | `kubectl -n s19 logs <pod-name>` and send me the last lines |
| Pods `Pending` | `kubectl -n s19 describe pod <name>`. If it says Insufficient cpu, check `kubectl get nodes` shows 2 Ready |
| `incident-store-*` Pending with `unbound PersistentVolumeClaims` | Wait 1 min; disks are being created |
| helm `timed out waiting for the condition` | Re-run `terraform apply` in `infra/platform` |

### Step 1.12 — Test the public URL (10 min)

```bash
kubectl -n s19 get svc incident-intake       # EXTERNAL-IP appears after 1-2 min
export IP=$(kubectl -n s19 get svc incident-intake -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "export IP=$IP" >> ~/.bashrc; echo $IP
```
**Report an incident** (quorum write to 3 replicas over gRPC):
```bash
curl -s -X POST http://$IP/api/incidents -H 'content-type: application/json' -d '{
  "reporterId":"citizen-42","type":"FLOOD","severity":"HIGH","region":"north",
  "description":"River breached embankment near the bridge",
  "latitude":28.61,"longitude":77.21,"peopleAffected":120}' | jq
```
**Expected:** JSON with `"incidentId": "inc-…"`, `"status": "REPORTED"` and `"quorum": {"acks": 2 or 3, "w": 2, "n": 3, "replicas": {"incident-store-0": "ok", …}}`.

```bash
export ID=inc-xxxxxxxxxxxx          # paste your incidentId
curl -s http://$IP/api/incidents/$ID | jq '{found, value: .value.status, responded, context}'   # quorum read R=2
curl -s http://$IP/api/incidents | jq '.items | length'
```
Open `http://<IP>/docs` in the browser to see the interactive API page of the public service.

**See the incident on each replica** (the coordinator is internal, so use port-forward):
```bash
kubectl -n s19 port-forward svc/quorum-coordinator 8081:8080 >/dev/null 2>&1 &
sleep 2
curl -s localhost:8081/api/quorum/config | jq
curl -s localhost:8081/api/quorum/debug/$ID | jq 'map_values(.versions[0].value.status)'
curl -s localhost:8081/api/quorum/replicas | jq
kill %1
```
**Expected:** `n=3, w=2, r=2, rPlusWGreaterThanN: true`; all 3 replicas show `"REPORTED"`.

**Check the event reached Kafka** (Intake → Kafka):
```bash
gcloud compute ssh s19-kafka --zone $ZONE --tunnel-through-iap --command \
 "sudo docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 10.10.0.10:9092 --topic incident-events --from-beginning --max-messages 1 --timeout-ms 15000"
```
**Expected:** one JSON line with `"eventType": "INCIDENT_REPORTED"`.

**Check the raw report reached Cloud Storage** (written in batches every ~30 s):
```bash
sleep 40; gcloud storage ls -r gs://$PROJECT_ID-s19-raw/raw/ | tail -3
```
**Expected:** a `….ndjson` file.

| Problem | Fix |
|---|---|
| `EXTERNAL-IP <pending>` for > 5 min | `kubectl -n s19 describe svc incident-intake` (Events) |
| `503 quorum coordinator unreachable` | `kubectl -n s19 get pods`; coordinator not ready. `kubectl -n s19 logs deploy/quorum-coordinator` |
| `acks: 0` / 503 `write quorum not reached` | Replicas not ready: `kubectl -n s19 logs incident-store-0` |
| No Kafka message | `kubectl -n s19 logs deploy/incident-intake | grep -i kafka`. Make sure the Kafka VM is running (Step 1.8) and the ConfigMap has `KAFKA_BOOTSTRAP: 10.10.0.10:9092` (`kubectl -n s19 get cm s19-config -o yaml`) |
| No GCS file | `kubectl -n s19 logs deploy/incident-intake | grep -i gcs` (permission errors mean the IAM apply was incomplete; re-run Step 1.7 apply, then restart the pod by deleting it: `kubectl -n s19 delete pod -l app=incident-intake`) |

### Step 1.13 — Check the HPAs (2 min)
```bash
kubectl -n s19 get hpa
```
**Expected:** two rows (`incident-intake`, `quorum-coordinator`) with TARGETS like `cpu: 2%/60%`, MINPODS 1, MAXPODS 4. If it shows `<unknown>`, wait 1–2 minutes. The load test that makes them scale comes in Phase 4.

### Step 1.14 — Prove GitOps (requirement d) (5 min)

Change something **in Git only** and let Argo CD apply it:
```bash
cd ~/s19-dm
sed -i '0,/maxReplicas: 4/s//maxReplicas: 5/' k8s/s19/hpa.yaml      # incident-intake max 4 -> 5
git commit -am "GitOps test: intake maxReplicas 5" && git push
kubectl -n s19 get hpa incident-intake -w                          # within ~3 min MAXPODS becomes 5 (Ctrl+C)
```
In the Argo CD UI you see `s19-services` go **OutOfSync → Syncing → Synced** with your commit message. **Take a screenshot.** That's your evidence for (d).

Argo CD checks Git about every 3 minutes. Click **Refresh** in the UI to make it check immediately.

### Step 1.15 — Early risk check: compile the Flink job (optional, 10 min)

Flink runs in Phase 3, but compiling it now finds build problems early:
```bash
cd ~/s19-dm/flink/situation-analytics-flink
mvn -q -DskipTests package && ls -lh target/*.jar
```
**Expected:** `situation-analytics-flink-1.0.0.jar`. If Maven prints `[ERROR]` lines, send me the first ones and I'll fix the code before Phase 3. If `mvn` or `java` is missing, run `java -version; mvn -v` and send me the output.

### Step 1.16 — End of session: pause to save money

Pausing keeps the cluster, the disks (replica data) and the public IP. Only the expensive running parts stop. Resuming takes ~5 minutes.
```bash
cd ~/s19-dm/infra/cloud
# PAUSE
terraform apply -var gke_node_count=0 -var cloudsql_running=false -var kafka_running=false
# RESUME (next session)
terraform apply
```
After resuming, wait ~5 min until `kubectl -n s19 get pods` shows everything Running again. Pods restart on the new nodes; the replica data on the disks is kept.

---

### Phase 1 exit checklist

| ✔ | Check | Command / where |
|---|---|---|
| ☐ | 8 replication tests pass | Step 1.6 |
| ☐ | All infrastructure created by Terraform | `terraform state list` in `infra/cloud` (screenshot) |
| ☐ | Kafka VM running | Step 1.8 |
| ☐ | Argo CD apps Synced / Healthy | Argo CD UI (screenshot) |
| ☐ | Public URL stores an incident with W=2 | Step 1.12 `curl` output (save it) |
| ☐ | Incident visible on all 3 replicas | Step 1.12 `debug` output |
| ☐ | Event in Kafka, raw file in Cloud Storage | Step 1.12 |
| ☐ | 2 HPAs present | `kubectl -n s19 get hpa` |
| ☐ | GitOps change applied by Argo CD, not kubectl | Step 1.14 (screenshot) |
| ☐ | (optional) Flink jar compiles | Step 1.15 |

Save outputs and screenshots in a folder `docs/evidence/phase1/`, then `git add docs && git commit -m "phase 1 evidence" && git push`.

### Cost of Phase 1 (approximate, from the $300 trial credit)

| Item | While running | While paused |
|---|---|---|
| GKE management fee | covered by GKE free-tier credit | same |
| 2 × e2-standard-2 nodes | ≈ $0.13/h | $0 |
| Kafka VM e2-medium | ≈ $0.03/h | $0 (disk only) |
| Cloud SQL db-f1-micro | ≈ $0.015/h | $0 (10 GB disk only) |
| Public load balancer (Intake) | ≈ $0.025/h | ≈ $0.025/h |
| Disks (nodes, replicas, Kafka, SQL) | ≈ $0.01/h | ≈ $0.01/h |
| Firestore, Cloud Storage, Pub/Sub, Artifact Registry | free-tier quotas | free |
| **Total** | **≈ $0.21/h (≈ $5/day)** | **≈ $0.04/h (≈ $1/day)** |

---

# PHASE 2 — Data and concept: Leaderless replication (f) + Three storage products (g)

**Lead:** Member 2 · **Services:** Resource Registry (Cloud SQL), Volunteer Coordination (Firestore) · **Time:** 2–3 h

### What Phase 2 delivers

1. **(g) Three distinct storage products, each with a clear job:**

   | Product | Holds | Written by | Why this product |
   |---|---|---|---|
   | **Cloud Storage** | raw incident reports (NDJSON batches) + uploaded photos/documents | Incident Intake | cheap, append-only archive for raw files |
   | **Cloud SQL (PostgreSQL)** | vehicles, supply kits, teams + reservations | Resource Registry | relational data; transactions with row locks so two dispatchers never take the same vehicle |
   | **Firestore** | volunteers, availability, assignments | Volunteer Coordination | semi-structured, frequently changing documents; transactional assignment |

2. **(f) A full demonstration of leaderless replication (N=3, W=2, R=2):**
   - A. writes keep working with a replica **down**, and the replica is **repaired after recovery**;
   - B. **read repair** fixes a replica that silently lost a write;
   - C. **concurrent writes** become siblings (vector clocks) and are **merged**;
   - D. **quorum loss**: W=2 is refused, W=1 is accepted, R=2 fails, and everything converges afterwards;
   - E. a **real crash** (pod deleted): writes continue and the replica comes back with its disk;
   - plus the **automated 14-check consistency test**, saved as evidence.

**Phase 2 is done when** both new pods are Running, each storage product shows data, and the consistency test prints `14/14 checks passed` against your cloud deployment.

**What has been tested, and what hasn't:**
- **Tested:** all the commands in Section 5 (scenarios A–D) and the 14-check test were run before delivery against 3 real replicas + coordinator + intake. The outputs shown below come from that run. Resource Registry was also tested on a real PostgreSQL: tables and seed data created once, list, reserve, optimistic-lock conflict, release, and **8 parallel reservations with no vehicle booked twice**.
- **Not tested:** Volunteer Coordination could **not** be tested before delivery (no Firestore emulator was available), so Step 2.4 is its first real run. If it fails, send me its logs.

### Files this phase uses

```
services/resource-registry/app/main.py            REST API on Cloud SQL
services/resource-registry/app/migrations/001_schema.sql   tables (created automatically at start-up)
services/resource-registry/app/migrations/002_seed.sql     105 demo resources (5 regions)
services/volunteer-coordination/app/main.py       REST API on Firestore
k8s/s19/resource-registry.yaml                    Deployment + Service (Argo CD deploys it)
k8s/s19/volunteer-coordination.yaml               Deployment + Service
scripts/add-services.sh       registers new services in k8s/s19/kustomization.yaml (reused in Phase 3)
scripts/port-forward.sh       tunnels from Cloud Shell to the internal services (:8081 :8082 :8083)
scripts/demo-helpers.sh       short commands for the replication demo: fault, heal, show, newinc, ae, replicas
scripts/consistency_test.py   automated 14-check replication test
```
No Terraform changes: Cloud SQL, Firestore and the bucket were already created in Phase 1. All these files have been in your repo since Step 1.5.

---

### Step 2.0 — Start the session (2 min, Cloud Shell)

```bash
cd ~/s19-dm && source ~/.bashrc && source .venv/bin/activate
git pull --rebase                      # get any changes teammates pushed
```
If you **paused** at the end of Phase 1, resume and wait about 5 min:
```bash
cd ~/s19-dm/infra/cloud && terraform apply && cd ~/s19-dm
kubectl -n s19 get pods                # wait until everything is Running 1/1
```
Check the public IP is still set: `echo $IP`. If it's empty, run:
```bash
export IP=$(kubectl -n s19 get svc incident-intake -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
```

### Step 2.1 — Check the Phase 2 files are present (1 min)
They came with the repo, but Argo CD doesn't deploy them yet because they aren't listed in `k8s/s19/kustomization.yaml`:
```bash
cd ~/s19-dm && ls services/resource-registry/app services/volunteer-coordination/app k8s/s19/
```
**Expected:** `main.py` (+ `migrations/`) in both service folders, and `resource-registry.yaml` and `volunteer-coordination.yaml` in `k8s/s19/`.

### Step 2.2 — Deploy the two new services through GitOps (≈ 10 min)

```bash
cd ~/s19-dm
scripts/add-services.sh resource-registry volunteer-coordination
git add services scripts k8s docs
scripts/release.sh resource-registry volunteer-coordination
```
What happens:
1. **`add-services.sh`** adds both services to `k8s/s19/kustomization.yaml` (resources + images). It prints the updated file, where you should see 5 resources and 5 images.
2. **`git add`** stages the new code and manifests.
3. **`release.sh`** builds and pushes only these 2 images, writes their tags into `kustomization.yaml`, and makes **one commit** with everything. Then it runs `git push`.
4. **Argo CD** sees the commit and deploys the two new Deployments within about 1–3 minutes. Click **Refresh** in the Argo CD UI to make it check immediately.

Watch them start:
```bash
kubectl -n s19 get pods -w            # Ctrl+C when both are Running 1/1
kubectl -n s19 logs deploy/resource-registry | grep -i "ready\|migration"
kubectl -n s19 logs deploy/volunteer-coordination | tail -5
```
**Expected:** two new pods, `resource-registry-…` and `volunteer-coordination-…`, both `Running 1/1`. The registry log contains `"message": "registry ready"` and `"migrationsApplied": ["001_schema.sql", "002_seed.sql"]` the first time. After a restart that list is `[]`, because migrations run only once.

| Problem | Fix |
|---|---|
| `ImagePullBackOff` on a new pod | Its tag is still `none`: `grep -A2 "s19/resource-registry" k8s/s19/kustomization.yaml`. Re-run `scripts/release.sh <service>` |
| Registry `CrashLoopBackOff`, log shows `connection timeout` / `PoolTimeout` | Cloud SQL is stopped or unreachable. Check `gcloud sql instances describe s19-pg --format="value(state)"` → must be `RUNNABLE`. If it says `STOPPED`, run `terraform apply` in `infra/cloud` (resume) |
| Registry log `password authentication failed` | The secret doesn't match: re-run `terraform apply` in `infra/platform`, then `kubectl -n s19 delete pod -l app=resource-registry` (the pod is recreated with the new secret) |
| Volunteer pod crashes, or later calls return 500 with `403 Missing or insufficient permissions` | Node service account lacks Firestore rights: re-run `terraform apply` in `infra/cloud` (it grants `roles/datastore.user`), wait 2 min, then `kubectl -n s19 delete pod -l app=volunteer-coordination` |
| Volunteer 500 with `The query requires an index` | Send me the error; it contains a link that creates the index |
| `git push` rejected | `git pull --rebase && git push` |

### Step 2.3 — Open tunnels and load the admin token (1 min)

Resource Registry, Volunteer Coordination and the Quorum Coordinator are **internal** services (no public IP). From Cloud Shell you reach them through `kubectl port-forward` tunnels:
```bash
scripts/port-forward.sh
export ADMIN_TOKEN=$(terraform -chdir=infra/platform output -raw admin_token)
echo "export ADMIN_TOKEN=$ADMIN_TOKEN" >> ~/.bashrc
```
**Expected:**
```
quorum-coordinator -> http://localhost:8081
resource-registry -> http://localhost:8082
volunteer-coordination -> http://localhost:8083
  check quorum-coordinator /healthz: 200
  check resource-registry /healthz: 200
  check volunteer-coordination /healthz: 200
```
Tunnels stop if Cloud Shell disconnects or a pod restarts. If a command later says `Connection refused`, run `scripts/port-forward.sh` again.

---

### Step 2.4 — Requirement (g): the three storage products (≈ 30 min)

#### 2.4.1 Cloud Storage — raw reports and attachments (Incident Intake)
```bash
R=$(curl -s -X POST http://$IP/api/incidents -H 'content-type: application/json' -d '{
 "reporterId":"citizen-7","type":"FIRE","severity":"HIGH","region":"west","description":"Warehouse fire near the port",
 "latitude":19.07,"longitude":72.88,"peopleAffected":15}'); echo "$R" | jq -c '{incidentId, acks: .quorum.acks}'
export ID=$(echo "$R" | jq -r .incidentId)

echo "photo of the fire (demo file)" > /tmp/fire-photo.txt
curl -s -F file=@/tmp/fire-photo.txt http://$IP/api/incidents/$ID/attachments | jq
sleep 35
gcloud storage ls -r gs://$PROJECT_ID-s19-raw/ | tail -5
```
**Expected:** the upload returns `"uri": "gs://<PROJECT_ID>-s19-raw/attachments/<ID>/…-fire-photo.txt"`. The listing shows both `attachments/…` and `raw/incidents/YYYY/MM/DD/HH/batch-….ndjson`. Raw reports are written in batches every ~30 s to keep storage operations low.
**Look at it:** Console → **Cloud Storage → Buckets → `<PROJECT_ID>-s19-raw`**. Take a screenshot for the evidence.
**Read one raw batch:** `gcloud storage cat $(gcloud storage ls -r "gs://$PROJECT_ID-s19-raw/raw/**" | tail -1) | head -2`

#### 2.4.2 Cloud SQL — Resource Registry (localhost:8082)
```bash
curl -s localhost:8082/api/resources/stats | jq '.byRegion.north'
curl -s "localhost:8082/api/resources?region=north&kind=VEHICLE" | jq -c '.items[] | {name, status, capacity}' | head -5
```
**Expected:** per-kind counts for `north` (VEHICLE, SUPPLY, TEAM, all `AVAILABLE`), then vehicles like `{"name":"ambulance-north-1","status":"AVAILABLE","capacity":4}`.

Register a new resource:
```bash
curl -s -X POST localhost:8082/api/resources -H 'content-type: application/json' \
  -d '{"kind":"VEHICLE","subtype":"drone","name":"drone-north-1","region":"north","capacity":1}' | jq -c '{id, name, status, version}'
```

**Atomic reservation** (Dispatch will use this in Phase 3). It locks rows inside a transaction with `FOR UPDATE SKIP LOCKED`:
```bash
curl -s -X POST localhost:8082/api/resources/reserve -H 'content-type: application/json' \
  -d "{\"incidentId\":\"$ID\",\"region\":\"west\",\"kind\":\"VEHICLE\",\"quantity\":2}" | jq -c '{reserved: [.reserved[].name], shortfall}'
curl -s localhost:8082/api/resources/stats | jq '{west: .byRegion.west.VEHICLE, activeReservations}'
```
**Expected:** 2 vehicles reserved; `west.VEHICLE` shows `RESERVED: 2`; `activeReservations: 2`.

**Optimistic locking** (relational integrity). An update with an old version number is refused:
```bash
RES=$(curl -s "localhost:8082/api/resources?region=west&status=RESERVED" | jq -c '.items[0] | {id, version}')
RID=$(echo $RES | jq -r .id); VER=$(echo $RES | jq -r .version)
curl -s -X PATCH localhost:8082/api/resources/$RID/status -H 'content-type: application/json' -d "{\"status\":\"DEPLOYED\",\"version\":$VER}" | jq -c '{name, status, version}'
curl -s -o /dev/null -w "same update with the OLD version -> HTTP %{http_code}\n" -X PATCH localhost:8082/api/resources/$RID/status \
  -H 'content-type: application/json' -d "{\"status\":\"AVAILABLE\",\"version\":$VER}"
curl -s -X POST localhost:8082/api/resources/release -H 'content-type: application/json' -d "{\"incidentId\":\"$ID\"}" | jq -c
```
**Expected:** the first PATCH returns `DEPLOYED` with version +1; the second returns **`HTTP 409`**; release returns `"released": 2`.

**Look at the tables (optional):** Console → **SQL → s19-pg → Cloud SQL Studio**. Database `s19`, user `s19app`, password from `terraform -chdir=infra/cloud output -raw db_password`. Then run `SELECT region, kind, status, count(*) FROM resources GROUP BY 1,2,3 ORDER BY 1,2;`. If the Studio can't connect, the API output above is enough evidence.

#### 2.4.3 Firestore — Volunteer Coordination (localhost:8083)
```bash
curl -s -X POST "localhost:8083/api/volunteers/_seed?perRegion=6" | jq          # {"seeded": 30}
curl -s -X POST localhost:8083/api/volunteers -H 'content-type: application/json' \
  -d '{"name":"Asha Verma","phone":"+919812345678","region":"north","skills":["first-aid","driving"]}' | jq -c '{id, name, region, available}'
curl -s "localhost:8083/api/volunteers?region=north&available=true" | jq -c '.items[] | {id, name, phone, skills}' | head -3
```
**Expected:** 30 seeded volunteers; your new volunteer returned with an id `vol-…`; the list shows **masked** phone numbers like `+91*****78`.

**Transactional assignment** (Dispatch will call this in Phase 3):
```bash
curl -s -X POST localhost:8083/api/volunteers/assign -H 'content-type: application/json' \
  -d "{\"incidentId\":\"$ID\",\"region\":\"west\",\"count\":3,\"skills\":[\"first-aid\"]}" | jq -c '{assigned: [.assigned[].name], shortfall}'
curl -s localhost:8083/api/volunteers/stats | jq -c '.byRegion.west'
curl -s -X POST localhost:8083/api/volunteers/release -H 'content-type: application/json' -d "{\"incidentId\":\"$ID\"}" | jq -c
curl -s localhost:8083/api/volunteers/stats | jq -c '.byRegion.west'
```
**Expected:** 3 west volunteers assigned. `west` changes from `{"total":6,"available":6,"assigned":0}` to `available 3 / assigned 3`, and back after release.
**Look at it:** Console → **Firestore → Data**: collections `volunteers` and `volunteer_assignments`. Take a screenshot.

---

### Step 2.5 — Requirement (f): leaderless replication demo (≈ 40 min)

Load the shortcuts:
```bash
cd ~/s19-dm && source scripts/demo-helpers.sh
```
**Expected:** `demo helpers loaded: fault, heal, show, newinc, ae, replicas (coordinator=localhost:8081, IP=<your IP>)`.

| Shortcut | What it does |
|---|---|
| `fault <replica> down\|slow\|drop_writes\|none` | failure injection on one replica (needs `ADMIN_TOKEN`) |
| `heal` | all replicas back to normal |
| `show <id>` | the status stored on **each** replica, bypassing the quorum |
| `newinc [region]` | report an incident through the public URL |
| `ae` | run one anti-entropy (replica sync) round now; it also runs every 30 s on its own |
| `replicas` | health, fault mode and key count of each replica |

> The anti-entropy timer repairs replicas every 30 s in the background. Run each scenario's commands one after another without long pauses, or the timer may repair a replica before you show it is stale. That is correct behaviour; just repeat the scenario.

#### A — Replica failure and recovery
```bash
fault incident-store-2 down
R=$(newinc); echo "$R" | jq -c .quorum; export A=$(echo "$R" | jq -r .incidentId)
show $A
fault incident-store-2 none
show $A
ae
show $A
```
**Expected** (from the pre-delivery run):
```
{"acks":2,"w":2,"n":3,"replicas":{"incident-store-0":"ok","incident-store-1":"ok","incident-store-2":"error: UNAVAILABLE: incident-store-2 is down (fault injection)"}}
{"incident-store-0":["REPORTED"],"incident-store-1":["REPORTED"],"incident-store-2":"UNAVAILABLE: incident-store-2 is down (fault injection)"}
{"incident-store-0":["REPORTED"],"incident-store-1":["REPORTED"],"incident-store-2":[]}          <- recovered but missing the write
{"differingBuckets":1,"keysChecked":1,"repaired":{"incident-store-2":1}}                          <- anti-entropy found and fixed it
{"incident-store-0":["REPORTED"],"incident-store-1":["REPORTED"],"incident-store-2":["REPORTED"]}
```
**Explain:** W=2 needs only 2 of 3 acknowledgements, so the write succeeds with no leader to fail over. The recovered replica is behind until anti-entropy compares hashes of 256 key buckets and copies only what differs.

#### B — Read repair
```bash
fault incident-store-0 drop_writes
curl -s -X PATCH http://$IP/api/incidents/$A -H 'content-type: application/json' \
  -d '{"status":"VERIFIED","note":"confirmed by drone","author":"field-officer"}' | jq -c '.quorum.acks'
fault incident-store-0 none
show $A
curl -s "localhost:8081/api/quorum/kv/$A?r=3" | jq -c '{status: .value.status, staleReplicasInQuorum}'
sleep 2; show $A
```
**Expected:**
```
2
{"incident-store-0":["REPORTED"],"incident-store-1":["VERIFIED"],"incident-store-2":["VERIFIED"]}   <- replica 0 lost the update
{"status":"VERIFIED","staleReplicasInQuorum":["incident-store-0"]}                                   <- read returns newest, detects stale
{"incident-store-0":["VERIFIED"],"incident-store-1":["VERIFIED"],"incident-store-2":["VERIFIED"]}   <- read repair fixed it
```
**Explain:** the read compares the versions it got. The newest wins thanks to the vector clock, and the stale replica is overwritten in the background.

#### C — Concurrent writes, siblings and merge
Two writers update the same incident from the same old version, without seeing each other:
```bash
CUR=$(curl -s localhost:8081/api/quorum/kv/$A); CTX=$(echo "$CUR" | jq -c .context)
V1=$(echo "$CUR" | jq -c '.value | .status="DISPATCHED"')
V2=$(echo "$CUR" | jq -c '.value | .severity="CRITICAL"')
curl -s -X PUT localhost:8081/api/quorum/kv/$A -H 'content-type: application/json' -d "{\"value\":$V1,\"context\":$CTX,\"clientId\":\"dispatch\"}" | jq -c .context
curl -s -X PUT localhost:8081/api/quorum/kv/$A -H 'content-type: application/json' -d "{\"value\":$V2,\"context\":$CTX,\"clientId\":\"field-officer\"}" | jq -c .context
curl -s localhost:8081/api/quorum/kv/$A | jq -c '{conflict, resolvedBy, siblingWriters: [.siblings[].writer], status: .value.status, severity: .value.severity}'
sleep 2; curl -s "localhost:8081/api/quorum/kv/$A?r=3" | jq -c '{conflict, mergedFromSiblings: .value.mergedFromSiblings}'
```
**Expected:**
```
{"intake":1,"field-officer":1,"dispatch":1}       <- vector clock of write 1
{"intake":1,"field-officer":2}                    <- vector clock of write 2: neither contains the other => concurrent
{"conflict":true,"resolvedBy":"semantic-merge","siblingWriters":["dispatch","field-officer"],"status":"DISPATCHED","severity":"CRITICAL"}
{"conflict":false,"mergedFromSiblings":2}         <- merged version written back; siblings are gone
```
**Explain:** both versions are kept as siblings, and none is lost to "last write wins". The merge rules are safe for a disaster: the most advanced status, the highest severity, and the union of notes and assignments. The exact counter values may differ from these; what matters is that neither clock is ≥ the other in every entry.

#### D — Quorum loss and tunable consistency
```bash
fault incident-store-1 down; fault incident-store-2 down
newinc | jq -c '{error, acks: .quorum.acks}'
curl -s -X PUT "localhost:8081/api/quorum/kv/test-w1?w=1" -H 'content-type: application/json' \
  -d '{"value":{"region":"east","status":"REPORTED"},"clientId":"intake"}' | jq -c '{acks, w}'
curl -s -o /dev/null -w "R=2 read -> HTTP %{http_code}\n" localhost:8081/api/quorum/kv/test-w1
heal >/dev/null; ae; show test-w1; replicas
```
**Expected:**
```
{"error":"incident not durably stored","acks":1}          <- W=2 impossible with 1 replica: refused (consistency)
{"acks":1,"w":1}                                          <- W=1 accepted (availability)
R=2 read -> HTTP 503
{"differingBuckets":2,"keysChecked":2,"repaired":{"incident-store-1":2,"incident-store-2":2}}
{"incident-store-0":["REPORTED"],"incident-store-1":["REPORTED"],"incident-store-2":["REPORTED"]}
...three lines, all reachable, faultMode none, the same "keys" number
```
**Talking point:** the refused W=2 write still reached the one live replica, and anti-entropy later copied it everywhere. Dynamo-style systems don't roll back partial writes; that is why the client got an error and should retry.

#### E — Real crash (Kubernetes deletes a replica pod)
```bash
kubectl -n s19 delete pod incident-store-2 --wait=false
for i in 1 2 3 4 5; do newinc | jq -c '.quorum.acks'; sleep 1; done
kubectl -n s19 get pods -l app=incident-store       # incident-store-2 restarting / Running again
sleep 40; replicas
```
**Expected:**
- All 5 writes succeed with `2` or `3` acks.
- `incident-store-2` comes back with the **same name and the same disk** (StatefulSet + PersistentVolume).
- Within ~30–60 s the anti-entropy timer brings it back in line: `replicas` shows the same `keys` on all three.

If the coordinator tunnel dropped (`Connection refused`), run `scripts/port-forward.sh` again.

#### F — Automated test suite (evidence)
```bash
mkdir -p docs/evidence/phase2
BASE_URL=http://$IP COORD_URL=http://localhost:8081 python scripts/consistency_test.py | tee docs/evidence/phase2/consistency-test.txt
```
**Expected:** 14 `[PASS]` lines ending with `14/14 checks passed`. The same suite passed 14/14 before delivery. If one check fails because the 30-second anti-entropy ran in the middle of it, run the suite again. A check that keeps failing needs fixing; send me its line.

#### G — See it in the logs (1 min)
Console → **Logging → Logs Explorer**, paste and **Run query**:
```
resource.type="k8s_container" resource.labels.namespace_name="s19" (jsonPayload.message="read repair" OR jsonPayload.message="anti-entropy repaired keys" OR jsonPayload.message="fault injection changed")
```
You see the repairs and fault changes from the scenarios above. Screenshot it for the evidence.

---

### Step 2.6 — Save evidence and end the session

```bash
heal
cd ~/s19-dm && git add docs/evidence && git commit -m "phase 2 evidence" && git pull --rebase && git push
scripts/port-forward.sh stop
```
Take screenshots of: the Cloud Storage bucket, Firestore data, the Cloud SQL Studio query (or the API output), the Argo CD app with the 5 services, and the Logs Explorer query.

Pause if you're stopping for the day:
```bash
cd ~/s19-dm/infra/cloud && terraform apply -var gke_node_count=0 -var cloudsql_running=false -var kafka_running=false
```

### Phase 2 exit checklist

| ✔ | Check | Evidence |
|---|---|---|
| ☐ | Resource Registry + Volunteer Coordination Running, deployed by Argo CD | `kubectl -n s19 get pods`, Argo CD screenshot |
| ☐ | Cloud Storage: raw batches + attachment | `gcloud storage ls` output / console screenshot |
| ☐ | Cloud SQL: list, reserve, 409 on stale version, release | Step 2.4.2 outputs |
| ☐ | Firestore: seed, register, assign/release in a transaction | Step 2.4.3 outputs + console screenshot |
| ☐ | Scenarios A–E shown | outputs (copy into `docs/evidence/phase2/demo.txt`) |
| ☐ | Consistency test 14/14 against the cloud | `docs/evidence/phase2/consistency-test.txt` |
| ☐ | Repairs visible in Cloud Logging | screenshot |

### Troubleshooting (Phase 2)

| Symptom | Fix |
|---|---|
| `curl: (7) Failed to connect to localhost port 8081/8082/8083` | Tunnels gone: `scripts/port-forward.sh` |
| `{"detail":"missing or wrong X-Admin-Token"}` | `export ADMIN_TOKEN=$(terraform -chdir=infra/platform output -raw admin_token)`, then `source scripts/demo-helpers.sh` again |
| `show` prints `null` for every replica | Wrong id: `echo $A`. Use the `incidentId` from `newinc` |
| Scenario B shows replica 0 already `VERIFIED` | Anti-entropy ran in between (every 30 s). Repeat B a bit faster |
| A scenario leaves a replica `down` | `heal` |
| Several coordinator pods (HPA) | Fine: replicas and fault modes are shared; stats differ per pod |
| Volunteer API returns 500 | `kubectl -n s19 logs deploy/volunteer-coordination --tail=30` and send me the lines |
| Registry API returns 500 | `kubectl -n s19 logs deploy/resource-registry --tail=30` |

### Cost of Phase 2

The same as Phase 1: about **$0.21/h running** and about **$0.04/h paused**. The new pods fit on the existing 2 nodes. Firestore use here (tens of documents) stays well inside its free daily quota.

---

# PHASE 3 — Events and streaming: Serverless + REST/gRPC/queues (b) + Flink (e)

**Lead:** Member 3 · **Services:** Dispatch, Alert Service (serverless), Situation Analytics (Flink + API) · **Time:** 3–4 h

### What Phase 3 delivers

1. **(b) Serverless:** the **Alert Service** is a Cloud Run function, created by Terraform. Each message on the Pub/Sub topic `alert-requests` triggers it asynchronously. It works out who to notify and stores a delivery record in Firestore.
2. **(b) All three communication styles, each used for real:**

   | Mechanism | Where it is used |
   |---|---|
   | **REST** | public API (Intake); Intake → Coordinator; Dispatch → Coordinator / Registry / Volunteer; Analytics → Coordinator / Registry / Volunteer |
   | **gRPC** | Coordinator → the 3 replicas (Phase 1) |
   | **Message queue: Kafka** | Intake → `incident-events` → Dispatch (auto-dispatch) and Flink; Dispatch → `dispatch-events` → Flink; Coordinator → `consistency-events` → Flink; Flink → `incident-aggregates` → Analytics API |
   | **Message queue: Pub/Sub** | Intake and Dispatch → `alert-requests` → Alert function |

3. **(e) Flink on a managed cluster:** a single-node **Dataproc** cluster with Flink, created by Terraform only while you need it. The job runs like this:
   - It **consumes** the three Kafka topics.
   - It builds a **1-minute tumbling window per region**, counting incidents, critical incidents, **unique reporters**, dispatches and consistency events. The counts and the reporter set are Flink-managed state, checkpointed every 30 s.
   - It flags **surges** using state kept across windows.
   - It **publishes** the results to the separate topic **`incident-aggregates`**.

**Phase 3 is done when:**
- one CRITICAL incident flows automatically: Kafka → Dispatch → resources + volunteers reserved → incident `DISPATCHED` → Pub/Sub → function → alert stored;
- `GET /api/analytics/windows` shows 1-minute results produced by Flink.

**What has been tested, and what hasn't:**
- **Tested:** Dispatch was tested end to end with real PostgreSQL, Registry, Coordinator, replicas and Intake (Volunteer Coordination replaced by a small stand-in):
  - manual dispatch reserved 3 resources and 5 volunteers, and the incident became DISPATCHED;
  - a duplicate dispatch was refused (409) and an unknown incident returned 404;
  - completing it marked the incident RESOLVED and released the resources.
- **Tested:** the Alert function with a fake Firestore: CRITICAL alert, dispatch alert, a duplicate message ignored, a malformed message dropped. This test **found and fixed a real bug**: every alert would have crashed after sending its notifications and been retried forever.
- **Not tested:**
  - the Analytics API's Kafka/Firestore path: only its storage logic was tested;
  - Kafka consumption, Pub/Sub, Eventarc, the function deploy, Dataproc;
  - the **Flink job, which has never been compiled**: Maven Central was blocked where it was written. That's why Step 3.2 compiles it first.

### Files this phase uses

```
services/dispatch/app/main.py + migrations/001_schema.sql   Dispatch (Cloud SQL table dispatch_assignments)
services/situation-analytics/app/main.py                    Analytics API (Kafka consumer -> Firestore)
services/alert-function/main.py + requirements.txt          Alert Service (Cloud Run function)
infra/cloud/phase3.tf.off      Terraform (renamed to phase3.tf in Step 3.1): function + its 3 service accounts, build bucket, Firestore index,
                               Dataproc Flink cluster (switch: dataproc_enabled)
k8s/s19/dispatch.yaml, k8s/s19/situation-analytics.yaml
scripts/tunnels.sh             tunnels to all 5 internal services (replaces port-forward.sh)
scripts/flink-submit.sh        build jar -> upload -> gcloud dataproc jobs submit flink
scripts/traffic.sh             steady stream of incidents so Flink windows keep closing
```
The Flink job source is in `flink/situation-analytics-flink/`; you may already have compiled it in Step 1.15.

---

### Step 3.0 — Start the session (2 min)
```bash
cd ~/s19-dm && source ~/.bashrc && source .venv/bin/activate && git pull --rebase
kubectl -n s19 get pods            # resume first if paused: cd infra/cloud && terraform apply && cd ~/s19-dm
echo $IP $ADMIN_TOKEN              # both must print values (see Step 1.12 / Step 2.3)
```

### Step 3.1 — Switch on the Phase 3 Terraform file (1 min)
Until now the function and Dataproc definitions were in `infra/cloud/phase3.tf.off`, which Terraform ignores because it doesn't end in `.tf`. Rename it:
```bash
cd ~/s19-dm && git mv infra/cloud/phase3.tf.off infra/cloud/phase3.tf
ls infra/cloud/*.tf
```
**Expected:** `phase3.tf` listed with the other `.tf` files. It's committed together with Step 3.4's release.

### Step 3.2 — Compile the Flink job FIRST (≈ 5–10 min)

This is the riskiest piece, so it goes first:
```bash
cd ~/s19-dm/flink/situation-analytics-flink
java -version 2>&1 | head -1; mvn -v | head -1
mvn -q -DskipTests package && ls -lh target/*.jar
cd ~/s19-dm
```
**Expected:** `target/situation-analytics-flink-1.0.0.jar`, tens of MB.

| If Maven prints… | Fix |
|---|---|
| `Could not find artifact org.apache.flink:flink-connector-kafka:jar:3.0.2-1.17` | In `pom.xml` set `<kafka.connector.version>3.0.1-1.17</kafka.connector.version>` and build again |
| `[ERROR] … SituationAnalyticsJob.java:[line,col] …` | Copy the **first 10 `[ERROR]` lines** and send them to me |
| `mvn: command not found` | Cloud Shell normally has it; send me the output of `ls /usr/share/maven* ; which java` |

### Step 3.3 — Terraform: the serverless function, the index and the build bucket (≈ 10 min)
```bash
cd ~/s19-dm/infra/cloud
terraform init -backend-config="bucket=$PROJECT_ID-tfstate"     # again: installs the "archive" provider used by phase3.tf
terraform plan          # check the plan: Dataproc must NOT be in it yet
terraform apply
```
**Plan should show about 19 to add:** the bucket `…-s19-build`, 4 service accounts with their roles, `google_cloudfunctions2_function.alert`, `google_firestore_index.windows_by_region`, and the source zip object.
**Takes:** 3–8 minutes; the function is built by Cloud Build.
**Check:**
```bash
gcloud functions describe alert-service --region $REGION --format="value(state)"      # ACTIVE
cd ~/s19-dm
```

**Test the function on its own** by publishing one message straight to Pub/Sub:
```bash
gcloud pubsub topics publish alert-requests \
  --message='{"alertType":"TEST","incidentId":"inc-manual-test","region":"north","severity":"CRITICAL","message":"manual test"}'
sleep 20
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="alert-service" AND jsonPayload.message="alert processed"' --limit 3 --format="value(jsonPayload)"
```
**Expected:** a line containing `'incidentId': 'inc-manual-test'` and `'deliveries': …`. Notifications go only to volunteers who are **available** in `north` right now, plus one region broadcast. In Console → **Firestore → Data → `alerts`** there's a document with this alert.

| Problem | Fix |
|---|---|
| Apply error `Build failed … permission` / `iam.serviceAccounts.actAs` | IAM changes take a few minutes to spread: wait 3 min and `terraform apply` again |
| `Error creating function … Eventarc … service agent` | First use of Eventarc in a new project; wait 3–5 min and apply again |
| `Error 409 … index already exists` | `terraform import google_firestore_index.windows_by_region <the index name in the message>` |
| No "alert processed" log | `gcloud logging read 'resource.labels.service_name="alert-service"' --limit 20 --format="value(textPayload,jsonPayload.message)"` and send me the error lines |

### Step 3.4 — Deploy Dispatch and the Analytics API through GitOps (≈ 10 min)
```bash
cd ~/s19-dm
scripts/add-services.sh dispatch situation-analytics
git add services infra scripts k8s docs flink
scripts/release.sh dispatch situation-analytics
kubectl -n s19 get pods -w          # Ctrl+C when dispatch-… and situation-analytics-… are Running 1/1
kubectl -n s19 logs deploy/dispatch | grep -i "subscribed\|migration\|error" | head
```
**Expected:** both new pods `Running 1/1`. Dispatch's log contains `kafka consumer subscribed` with topic `incident-events`. The first start also creates its table (`dispatch_schema_migrations`). Argo CD now shows **7 Deployments / StatefulSets**.

Note: `release.sh` builds only these two images. Its commit also includes the Terraform and Flink files you added, which is fine: they're part of the repo too.

### Step 3.5 — Open the tunnels (1 min)
```bash
scripts/tunnels.sh
```
**Expected:** five lines, `quorum-coordinator … 8081`, `resource-registry … 8082`, `volunteer-coordination … 8083`, `dispatch … 8084`, `situation-analytics … 8085`, each with `/healthz: 200`.

---

### Step 3.6 — Requirement (b): one CRITICAL incident through every mechanism (≈ 15 min)

Make sure volunteers exist in the region (Phase 2 seeded 6 per region): `curl -s localhost:8083/api/volunteers/stats | jq -c .byRegion.south`

**1. Report it** (REST → Intake; gRPC quorum write; Kafka `incident-events`; Pub/Sub `alert-requests`):
```bash
R=$(curl -s -X POST http://$IP/api/incidents -H 'content-type: application/json' -d '{
 "reporterId":"citizen-99","type":"CYCLONE","severity":"CRITICAL","region":"south",
 "description":"Cyclone landfall, roofs blown off in coastal village","latitude":13.08,"longitude":80.27,"peopleAffected":300}')
echo "$R" | jq -c '{incidentId, acks: .quorum.acks, alertPublished}'
export C3=$(echo "$R" | jq -r .incidentId)
```
**Expected:** `acks` 2 or 3, `"alertPublished": true`.

**2. Wait ~10 s.** Dispatch received the Kafka event and dispatched by itself (REST → Registry + Volunteer + Coordinator):
```bash
sleep 10
curl -s "localhost:8084/api/dispatch?status=ACTIVE" | jq -c '.items[0] | {incident_id, mode, status, resources: (.resource_ids|length), volunteers: (.volunteer_ids|length)}'
curl -s http://$IP/api/incidents/$C3 | jq -c '.value | {status, assignedResources: (.assignedResources|length), assignedVolunteers: (.assignedVolunteers|length), notes: [.notes[].text]}'
```
**Expected:** a dispatch with `"incident_id": "<C3>"`, `"mode": "AUTO"`, 3 resources and up to 5 volunteers, and the incident now `"status": "DISPATCHED"` with an AUTO dispatch note.

**3. The serverless function ran twice:** once for the CRITICAL incident (from Intake) and once for the dispatch notice (from Dispatch):
```bash
sleep 15
curl -s "localhost:8085/api/analytics/alerts?limit=5" | jq -c '.items[] | {alertType, incidentId, recipientCount, channels}'
```
**Expected:** a `CRITICAL_INCIDENT` and a `DISPATCH_ASSIGNED` alert for `C3`.

**4. The Kafka topics now exist with real traffic** (you see the topic list, then one dispatch event):
```bash
gcloud compute ssh s19-kafka --zone $ZONE --tunnel-through-iap --command \
 "sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.10.0.10:9092 --list; \
  sudo docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 10.10.0.10:9092 --topic dispatch-events --from-beginning --max-messages 1 --timeout-ms 15000"
```
**Expected:** topics `consistency-events`, `dispatch-events`, `incident-events` (and later `incident-aggregates`), then a `DISPATCH_CREATED` JSON line.

**5. Finish the dispatch** (releases resources and volunteers; incident becomes RESOLVED):
```bash
DID=$(curl -s "localhost:8084/api/dispatch?status=ACTIVE" | jq -r --arg i "$C3" '.items[] | select(.incident_id==$i) | .id')
curl -s -X POST localhost:8084/api/dispatch/$DID/complete | jq -c '{status, completed_at}'
curl -s http://$IP/api/incidents/$C3 | jq -r .value.status            # RESOLVED
```

**Manual dispatch** of a non-critical incident (REST):
```bash
H=$(curl -s -X POST http://$IP/api/incidents -H 'content-type: application/json' -d '{"reporterId":"c-5","type":"FLOOD","severity":"HIGH","region":"east","description":"Street flooding","latitude":22.5,"longitude":88.3,"peopleAffected":40}' | jq -r .incidentId)
curl -s -X POST localhost:8084/api/dispatch -H 'content-type: application/json' -d "{\"incidentId\":\"$H\"}" | jq -c '.dispatch | {mode, status, resources: (.resource_ids|length), volunteers: (.volunteer_ids|length)}'
curl -s -o /dev/null -w "dispatch again -> HTTP %{http_code} (409 = refused, one active dispatch per incident)\n" -X POST localhost:8084/api/dispatch -H 'content-type: application/json' -d "{\"incidentId\":\"$H\"}"
```

| Problem | Fix |
|---|---|
| No AUTO dispatch after 30 s | `kubectl -n s19 logs deploy/dispatch --tail=30`. `curl -s localhost:8084/api/dispatch/_meta/consumer` shows `processed` and `lastError`. Kafka must be running (Step 1.8) |
| Dispatch `409 no resources or volunteers available` | That region ran out: complete old dispatches (`GET /api/dispatch?status=ACTIVE` → `/complete`) or use another region |
| `alertPublished: false` | Intake log: `kubectl -n s19 logs deploy/incident-intake | grep -i pubsub`. The node account needs `roles/pubsub.publisher` (Phase 1 Terraform) |
| Alerts list empty | Step 3.3 test first. Then `gcloud logging read 'resource.labels.service_name="alert-service"' --limit 20` |
| Analytics API 500 on `/alerts` | `kubectl -n s19 logs deploy/situation-analytics --tail=30` |

---

### Step 3.7 — Requirement (e): Flink on Dataproc (≈ 40 min)

#### 3.7.1 Create the Dataproc cluster (≈ 5 min)
```bash
cd ~/s19-dm/infra/cloud
terraform apply -var dataproc_enabled=true
cd ~/s19-dm
gcloud dataproc clusters list --region $REGION          # s19-flink  RUNNING
```
With Dataproc on, the project uses **8 vCPU** (GKE 4 + Kafka VM 2 + Dataproc 2), exactly the free-trial limit.

| Problem | Fix |
|---|---|
| `Insufficient 'CPUS' quota` / `exceeded quota` | Temporarily run GKE on one node: `terraform apply -var dataproc_enabled=true -var gke_node_count=1`. Most pods still fit; switch back with `terraform apply -var dataproc_enabled=true` after the Flink demo |
| `… does not have permission … dataproc.worker` / staging bucket errors | IAM still spreading: wait 3 min and apply again |
| Cluster creation fails on the image | Send me the message; the `image_version` in `phase3.tf` may need a newer `2.2` label |

#### 3.7.2 Check the Flink version on the cluster matches the job (1 min)
```bash
gcloud compute ssh s19-flink-m --zone $ZONE --tunnel-through-iap --command "flink --version"
```
**Expected:** `Version: 1.17.x`. The job is built for 1.17 (`flink.version` in `pom.xml`). If it prints a different version, e.g. 1.18, send it to me; the pom then needs that version and the matching Kafka connector before submitting.

#### 3.7.3 Submit the job (≈ 5 min)
```bash
scripts/flink-submit.sh
gcloud dataproc jobs list --region $REGION --cluster s19-flink --format="table(reference.jobId,status.state)"
```
**Expected:** the build, an upload to `gs://<PROJECT_ID>-s19-build/jobs/`, then a job in state **`RUNNING`** after about 1 minute.

#### 3.7.4 Feed it traffic and read the results (≈ 5 min)
Event-time windows close only when **newer** events arrive, so keep traffic flowing for 3+ minutes:
```bash
scripts/traffic.sh 120 2          # 120 incidents, one every 2 s (~4 min); 10 % are CRITICAL -> auto-dispatch + alerts
```
In a **second Cloud Shell tab** (`+`) while it runs, after ~2 minutes:
```bash
cd ~/s19-dm && source ~/.bashrc
gcloud compute ssh s19-kafka --zone $ZONE --tunnel-through-iap --command \
 "sudo docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 10.10.0.10:9092 --topic incident-aggregates --from-beginning --max-messages 3 --timeout-ms 20000"
```
**Expected:** JSON lines like
`{"region":"north","windowStart":…,"windowEnd":…,"incidents":6,"critical":1,"uniqueReporters":5,"dispatches":1,…,"surge":false,…}`.
That's Flink's output on the separate **results topic**.

The Analytics API stores these windows in Firestore:
```bash
scripts/tunnels.sh >/dev/null
curl -s "localhost:8085/api/analytics/windows?limit=5" | jq -c '.items[] | {region, windowStart, incidents, critical, uniqueReporters, dispatches, surge}'
curl -s "localhost:8085/api/analytics/windows?region=north&limit=3" | jq -c '.items[] | {windowStart, incidents, uniqueReporters}'
curl -s localhost:8085/api/analytics/summary | jq '{surgingRegions, regions: (.latestWindowByRegion|keys), pipeline}'
```
**Expected:** window documents per region with counts, and `pipeline.consumerProcessed > 0`.

**Show the job in the Flink UI:** Console → **Dataproc** (it may also appear as **Managed Service for Apache Spark**) → **Clusters → s19-flink → Web Interfaces → YARN ResourceManager**. Click the running application's **ApplicationMaster** link to open the Flink UI. The job graph shows the stages: Kafka source → parse → watermarks → tumbling window → surge detector → Kafka sink. Open the **Checkpoints** tab too. Take screenshots.

**Flink logs in Cloud Logging** (needed for requirement h):
```bash
gcloud logging read 'resource.type="cloud_dataproc_cluster" AND resource.labels.cluster_name="s19-flink"' --limit 5 --format="value(logName)"
```

| Problem | Fix |
|---|---|
| Job `ERROR` within a minute | `gcloud dataproc jobs describe <JOB_ID> --region $REGION` and the driver output link; send me the first exception |
| Job RUNNING but no messages on `incident-aggregates` | Windows need newer events: keep `traffic.sh` running 3+ min. Check the job reads Kafka: Flink UI → source → *Records Received* |
| `TimeoutException … 10.10.0.10:9092` in the job log | Kafka VM stopped? `gcloud compute instances list` → `s19-kafka RUNNING` |
| `windows` API empty but topic has data | `kubectl -n s19 logs deploy/situation-analytics | grep -i "kafka\|error"` |
| `region=` query error `requires an index` | The index from Step 3.3 is still building (a few minutes): `gcloud firestore indexes composite list` |

#### 3.7.5 Turn Dataproc off when you are done (saves $0.09/h)
```bash
cd ~/s19-dm/infra/cloud && terraform apply && cd ~/s19-dm       # dataproc_enabled defaults to false -> cluster deleted
```
Turn it on again for the Phase 4 demo with `terraform apply -var dataproc_enabled=true` + `scripts/flink-submit.sh`. Even if you forget, the cluster deletes itself after 2 hours without a running job.

---

### Step 3.8 — Evidence and end of session
```bash
mkdir -p docs/evidence/phase3
# paste the outputs of Steps 3.3, 3.6 and 3.7 into docs/evidence/phase3/outputs.txt (Cloud Shell Editor), then:
git add docs/evidence && git commit -m "phase 3 evidence" && git pull --rebase && git push
scripts/tunnels.sh stop
cd infra/cloud && terraform apply -var gke_node_count=0 -var cloudsql_running=false -var kafka_running=false   # pause (also removes Dataproc)
```
Screenshots: the function in Console (Cloud Run → `alert-service`), Firestore `alerts` and `analytics_windows`, the Dataproc job RUNNING, the Flink UI job graph and checkpoints, the Argo CD app with 7 workloads.

### Phase 3 exit checklist

| ✔ | Check | Evidence |
|---|---|---|
| ☐ | Flink jar compiles | Step 3.2 |
| ☐ | Alert function ACTIVE and triggered by Pub/Sub | Step 3.3 test + log line |
| ☐ | Dispatch + Analytics API deployed by Argo CD | `kubectl -n s19 get pods` |
| ☐ | CRITICAL incident → AUTO dispatch → DISPATCHED → alerts | Step 3.6 outputs |
| ☐ | REST, gRPC, Kafka, Pub/Sub each shown | Step 3.6 (+ Phase 1/2 gRPC debug) |
| ☐ | Dataproc Flink job RUNNING, consuming Kafka | Step 3.7.3 + Flink UI screenshot |
| ☐ | 1-minute window results on `incident-aggregates` | Step 3.7.4 consumer output |
| ☐ | Results visible via `/api/analytics/windows` | Step 3.7.4 |
| ☐ | Flink logs in Cloud Logging | Step 3.7.4 |

### Cost of Phase 3
- **Running, Dataproc off:** about **$0.21/h**, the same as before. The function scales to zero and its invocations stay inside the free tier.
- **Dataproc on:** **+ about $0.09/h**.
- **Cloud Build:** a few build-minutes per function change.

---

# PHASE 4 — Scale, observe, submit: HPA + load test (c) + Observability (h)

**Lead:** Member 4 · **Services:** none new · **Time:** about 3 h + documents and rehearsal

### What Phase 4 delivers

1. **(h) Metrics:** Prometheus + Grafana (kube-prometheus-stack), **deployed by Argo CD from Git**, plus a ServiceMonitor that scrapes all S19 services. The **"S19 Disaster Management" dashboard** shows:
   - RPS, error rate and latency;
   - Kubernetes cluster health (nodes, pods, restarts, CPU) and HPA replicas;
   - the replication layer.
2. **(h) Centralised logging:** one Cloud Logging query per component covers every GKE service, the Alert function, the **Flink analytics job** and the Kafka VM.
3. **(c) Load test:** a k6 run with **sustained** traffic. Recorded evidence shows the HPAs of `incident-intake` and `quorum-coordinator` scaling out and back in.
4. **Deliverables:** `docs/DESIGN.md` (design document), `task_divison.txt`, the evidence folder, the demo script (`docs/DEMO.md`) and the final cleanup.

**Phase 4 is done when:**
- the Grafana dashboard shows live data;
- the load test shows HPA replicas going above 1;
- the log queries return entries for every component;
- the documents are submitted.

**What has been tested, and what hasn't:** every new YAML/JSON file parses. The monitoring chart, the dashboard queries and the k6 run have **not** been run against a cluster. The steps below check each one.

### Files this phase uses

```
argocd/later/monitoring.yaml         (moved to argocd/apps/ in Step 4.1) Argo CD app: kube-prometheus-stack 87.19.2 (values inline)
argocd/later/monitoring-extras.yaml  (moved to argocd/apps/ in Step 4.1) Argo CD app: k8s/monitoring
k8s/monitoring/servicemonitor.yaml   scrape /metrics of the S19 services
k8s/monitoring/s19-dashboard.json    the Grafana dashboard (12 panels)
k8s/monitoring/kustomization.yaml
loadtest/hpa-load.js                 k6 script
scripts/load-test.sh                 runs k6 in Docker, saves output to docs/evidence/phase4/
scripts/record-hpa.sh                records HPA/pods/CPU every 15 s to docs/evidence/phase4/hpa-scaling.log
docs/DESIGN.md                       design document (deliverable)
docs/DEMO.md                         demonstration script
task_divison.txt                     team split (fill in IDs and names)
```

---

### Step 4.0 — Start the session (5 min)
```bash
cd ~/s19-dm && source ~/.bashrc && source .venv/bin/activate && git pull --rebase
cd infra/cloud && terraform apply && cd ~/s19-dm          # resume if paused (Dataproc stays off for now)
kubectl -n s19 get pods                                   # all Running 1/1
```

### Step 4.1 — Switch on the monitoring apps (2 min)
The two monitoring applications are in `argocd/later/`, which Argo CD doesn't watch. `setup-repo.sh` already filled in your repo URL in Step 1.5. Move them into the watched folder; pushing that commit **is** the deployment (GitOps):
```bash
cd ~/s19-dm && git pull --rebase
git mv argocd/later/monitoring.yaml argocd/later/monitoring-extras.yaml argocd/apps/
grep repoURL argocd/apps/monitoring-extras.yaml          # must show YOUR GitHub URL
git commit -m "Phase 4: enable Prometheus + Grafana" && git push
```

### Step 4.2 — Argo CD deploys Prometheus + Grafana (≈ 10 min)
The root app watches `argocd/apps/`, so the two new files become two new applications automatically:
```bash
kubectl -n argocd get applications -w            # Ctrl+C when monitoring and monitoring-extras are Synced/Healthy
kubectl -n monitoring get pods
```
**Expected pods in `monitoring`, all Running:**
- `kps-grafana-…` (3/3);
- `kps-kube-prometheus-stack-operator-…`;
- `kps-kube-state-metrics-…`;
- `kps-prometheus-node-exporter-…`, one per node;
- `prometheus-kps-kube-prometheus-stack-prometheus-0` (2/2).

Names can differ slightly between chart versions; `kubectl -n monitoring get svc` shows the real ones.

| Problem | Fix |
|---|---|
| `monitoring` error `metadata.annotations: Too long` | `ServerSideApply=true` must be in `argocd/apps/monitoring.yaml` (it is); in the Argo CD UI click **Sync** once more |
| `monitoring-extras` error `could not find … ServiceMonitor` | The CRDs from `monitoring` weren't installed yet; the app retries every 30 s or so. Wait 5 min or click **Sync** |
| `monitoring-extras` `repository not found` | `REPLACE_REPO_URL` left in the file: run `scripts/setup-repo.sh $PROJECT_ID $REPO_URL`, then commit and push |
| Pods `Pending` with `Insufficient cpu/memory` | GKE must have 2 nodes: `kubectl get nodes`. If you set `gke_node_count=1` for Flink in Phase 3, apply again without it |
| Chart version not found | Send me the error; `targetRevision` in `monitoring.yaml` may need a newer version number |

### Step 4.3 — Grafana and Prometheus (10 min)

**Grafana** (keep this running in its own Cloud Shell tab):
```bash
kubectl -n monitoring get secret kps-grafana -o jsonpath='{.data.admin-password}' | base64 -d; echo
kubectl -n monitoring port-forward svc/kps-grafana 3000:80
```
**Web Preview** (eye icon) → **Change port** → `3000` → log in as `admin` with the printed password → **Dashboards** → **S19 Disaster Management**.

The panels need traffic. In another tab: `cd ~/s19-dm && source ~/.bashrc && scripts/traffic.sh 60 1`.
**Expected within 1–2 min:** *Total requests / s* above 0, the RPS/latency lines for `incident-intake` and `quorum-coordinator`, *Incident replicas up* = 3, and *HPA replicas* showing both HPAs at 1.

**Prometheus targets** (check everything is scraped):
```bash
kubectl -n monitoring get svc | grep prometheus          # find the "...-prometheus" service, port 9090
kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090
```
Web Preview → port `9090` → **Status → Targets**: `serviceMonitor/monitoring/s19-services/0` lists the 6 REST services and `…/1` the 3 replicas, all **UP**.

| Problem | Fix |
|---|---|
| Dashboard not in the list | `kubectl -n monitoring get cm s19-dashboard --show-labels` (needs `grafana_dashboard=1`). Then `kubectl -n monitoring logs deploy/kps-grafana -c grafana-sc-dashboard --tail=20` |
| Panels say **No data** | Check Targets are UP; generate traffic; set the time range to *Last 15 minutes* |
| `s19-services` targets missing | `kubectl -n s19 get svc --show-labels`: each service needs label `app=<name>` and a port named `http` (replicas: `metrics`) |
| A panel shows `datasource not found` | Panel menu → Edit → choose the **Prometheus** data source → Save |

### Step 4.4 — Requirement (c): load test and HPA scale-out (≈ 20 min)

The HPAs (`k8s/s19/hpa.yaml`) add pods when average CPU is above **60 %** of the pods' request (100m), between **1 and 5** pods for Intake (you raised it in Phase 1) and 1 and 4 for the Coordinator.

1. **Tab A**, the recorder (leave it running):
   ```bash
   cd ~/s19-dm && scripts/record-hpa.sh
   ```
2. **Tab B**, the load (about 9 minutes: 2 min ramp-up, **6 min sustained at 40 virtual users**, 1 min ramp-down):
   ```bash
   cd ~/s19-dm && source ~/.bashrc && scripts/load-test.sh 40 6m
   ```
3. Watch Grafana: the *HPA replicas* and *CPU used by pod* panels, plus RPS and p95 latency.
4. When k6 has finished, wait ~5 minutes until the HPAs scale back to 1, then stop Tab A with `Ctrl+C`.

**What to look for:** in `docs/evidence/phase4/hpa-scaling.log`, TARGETS rise above `60%` and REPLICAS for `incident-intake` and/or `quorum-coordinator` go from 1 to 2, 3 or more, then back to 1. The k6 summary in `docs/evidence/phase4/k6-output.txt` shows `http_reqs`, `http_req_duration p(95)` and `http_req_failed`. **Record the numbers you actually get.**

| Problem | Fix |
|---|---|
| TARGETS `<unknown>/60%` | Metrics come from GKE's metrics-server; wait 1–2 min after pods start |
| Replicas stay at 1 | Not enough load: `scripts/load-test.sh 80 6m`. If still nothing, lower `averageUtilization` to 40 in `k8s/s19/hpa.yaml`, then commit + push (GitOps) and run again |
| New pods stay `Pending` | The 2 nodes are full: that's the cluster's limit (no node autoscaling, by design). Mention it in the demo; the pods that did start show the scale-out |
| Many errors (`http_req_failed` > 5 %) | Lower the load (`scripts/load-test.sh 25 6m`); check Grafana's error panel and `kubectl -n s19 logs deploy/incident-intake --tail=20` |
| `docker: permission denied` | Cloud Shell's Docker works without sudo; if not, restart Cloud Shell (⋮ → Restart) |

During the load test about 1 % of incidents are CRITICAL, so Dispatch auto-dispatches until a region runs out of resources. Afterwards you may see `409 no resources` in its log; that's expected. Free them with `/api/dispatch/{id}/complete` if you need them for the demo.

Save 2–3 Grafana screenshots (RPS, latency, HPA replicas) into `docs/evidence/phase4/`. In Cloud Shell use the **Upload** feature, or keep them on your laptop for the report.

### Step 4.5 — Requirement (h): centralised logging (10 min)

Console → **Logging → Logs Explorer**. Paste each query, click **Run query**, and take a screenshot. You can also use the `gcloud` line under each.

**1. All S19 microservices on GKE** (JSON logs, one line per event):
```
resource.type="k8s_container" resource.labels.namespace_name="s19"
```
`gcloud logging read 'resource.type="k8s_container" AND resource.labels.namespace_name="s19"' --limit 5 --format="value(resource.labels.container_name,jsonPayload.service,jsonPayload.message)"`

**2. One service and errors only:**
```
resource.type="k8s_container" resource.labels.namespace_name="s19" jsonPayload.service="dispatch" severity>=WARNING
```

**3. The serverless Alert Service:**
```
resource.type="cloud_run_revision" resource.labels.service_name="alert-service"
```

**4. The Flink analytics job (Dataproc).** Needs Dataproc on and the job running (Step 3.7), or use logs from your Phase 3 run within the retention period:
```
resource.type="cloud_dataproc_cluster" resource.labels.cluster_name="s19-flink"
```

**5. The Kafka broker VM:**
```
resource.type="gce_instance" logName:"gcplogs-docker-driver"
```

**6. Everything together, in one view:**
```
(resource.type="k8s_container" resource.labels.namespace_name="s19") OR resource.type="cloud_run_revision" OR resource.type="cloud_dataproc_cluster" OR (resource.type="gce_instance" logName:"gcplogs-docker-driver")
```
Click **Save** on query 6 and name it **S19 all services**. That saved query is the "centralised logging" view to show in the demo.

| Problem | Fix |
|---|---|
| Query 5 shows nothing | The Kafka VM writes logs through Docker's `gcplogs` driver; if it was started before the VM's roles were ready, reboot it: `gcloud compute instances reset s19-kafka --zone $ZONE`, then wait 2 min |
| Query 4 shows nothing | Dataproc is off; turn it on and submit the job (Step 3.7.1–7.3) |

### Step 4.6 — Deliverables (≈ 1–2 h of writing)

1. **Design document, `docs/DESIGN.md`**, covering what the assignment asks:
   - system overview;
   - cloud deployment architecture;
   - microservice diagrams and responsibilities;
   - interconnection mechanisms;
   - the rationale for the design choices.

   Read it, adjust anything you changed, and add your screenshots. **For a PDF:** open the file on **GitHub**, which draws the Mermaid diagrams, and use the browser's *Print → Save as PDF*.
2. **`task_divison.txt`:** replace `<ID NUMBER n>` and `<NAME n>` with real values. The split matches the phases: Member 1 = (a)(d), Member 2 = (f)(g), Member 3 = (b)(e), Member 4 = (c)(h).
3. **Evidence:** `docs/evidence/phase1..4/` with outputs and screenshots.
4. Commit everything:
   ```bash
   git add -A && git commit -m "Final deliverables" && git pull --rebase && git push
   ```
5. **Submit on Nalanda** (one per group): the design document PDF, the **GitHub repository link** (code, Terraform, Kubernetes manifests, Argo CD configuration) and `task_divison.txt`.

### Step 4.7 — Rehearse the demo
Follow `docs/DEMO.md` once end to end with all four members. It contains the T-60 preparation commands, one section per member (each shows their 2 requirements and 2 services), and the pause command for afterwards.

### Final requirement checklist

| Req | Shown by | Phase | ✔ |
|---|---|---|---|
| ≥ 8 microservices, one on a public URL | 8 services; `http://<IP>/api/incidents` | 1–3 | ☐ |
| (a) All infrastructure via IaC | `infra/cloud`, `infra/platform`; `terraform state list`, `plan` = No changes | 1, 3 | ☐ |
| (b) Serverless async function | Alert Service (Pub/Sub → Cloud Run function) | 3 | ☐ |
| (b) REST, gRPC and message queue each used | communication matrix `DESIGN.md` §3.1 | 1–3 | ☐ |
| (c) Managed K8s for the microservices | GKE `s19-gke` | 1 | ☐ |
| (c) HPAs on ≥ 2 critical services | `k8s/s19/hpa.yaml` (Intake, Coordinator) | 1 | ☐ |
| (c) Load test shows HPA scale-out | `docs/evidence/phase4/` k6 output + HPA log + screenshot | 4 | ☐ |
| (d) GitOps controller tracks Git, no kubectl apply | Argo CD app-of-apps; GitOps change demo | 1 | ☐ |
| (e) Flink on a managed cluster | Dataproc `s19-flink` | 3 | ☐ |
| (e) Consumes Kafka, stateful time window, results to a separate topic | `incident-events…` → 1-min window + unique reporters → `incident-aggregates` | 3 | ☐ |
| (f) Concept implemented and demonstrated | Phase 2 scenarios A–E + 14/14 consistency test | 2 | ☐ |
| (g) ≥ 3 storage products with clear purposes | Cloud Storage, Cloud SQL, Firestore | 2 | ☐ |
| (h) Prometheus + Grafana dashboard (RPS, errors, latency, cluster health) | "S19 Disaster Management" dashboard | 4 | ☐ |
| (h) Centralised logging incl. the analytics job | Cloud Logging saved query "S19 all services" | 4 | ☐ |
| Deliverables | `DESIGN.md` (PDF), GitHub link, `task_divison.txt` | 4 | ☐ |

### Step 4.8 — After grading: delete everything

Kubernetes created the public load balancer and the replica disks, not Terraform. So remove the Kubernetes side **first**, while the cluster still exists:
```bash
cd ~/s19-dm
terraform -chdir=infra/platform destroy        # deletes namespace s19 (load balancer + replica disks), Argo CD
sleep 120
gcloud compute forwarding-rules list           # should be empty
gcloud compute disks list --filter="-users:*"  # unattached disks: should be empty (delete any pvc-... with: gcloud compute disks delete NAME --zone $ZONE)
terraform -chdir=infra/cloud destroy           # everything else (type yes)
```
Then delete the project. This stops **all** billing for it and also removes the state bucket:
```bash
gcloud projects delete $PROJECT_ID
```
Finally, check **Billing → Reports** in the Console for the credit used.

| Problem | Fix |
|---|---|
| `destroy` of `infra/platform` hangs on the namespace | Wait 5 min (the load balancer is being removed). If still stuck: `kubectl -n s19 get all`, then re-run destroy |
| `infra/cloud` destroy error on the Cloud SQL private connection | Re-run `terraform destroy`. If it keeps failing, just delete the project; that removes it all |

### Cost of Phase 4
- **About $0.21/h running.** Prometheus and Grafana run on the existing nodes, and nothing new is billed.
- **The load test:** a few cents of network.
- **Demo day with Dataproc:** + about $0.09/h.
