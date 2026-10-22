# Kubernetes operating guide and team handoff

This supersedes the earlier Compose/personal-consumer setup for the current group VM. Do not run an old `docker compose up` alongside Kubernetes. The database is still the original Docker database; application workers and monitoring have moved to Kubernetes.

## 1. Connect from Nick's Mac

```bash
ssh -o IdentitiesOnly=yes \
  -i "$HOME/Documents/Cloud and edge coputing/group17/group17_rsa" \
  ubuntu@63.33.242.251
cd ~/group17-final-work
```

This working tree is on `codex/final-performance`. The older `~/group17-temperature-observability` checkout was retained. The running Pods use built images, not a bind mount of either directory.

## 2. Terminal A — deployment and resources

```bash
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl -n temperature get deployments,pods,services,hpa,pdb
sudo k3s kubectl -n temperature top pods
sudo k3s kubectl top nodes
```

Expected minimum: two Ready nodes; two ready API Pods, two consumers, two notifiers, one gateway, one Prometheus and one node-exporter. More API Pods may be created by HPA. Nick's load node is tainted intentionally. It does not automatically run application Pods.

For a live view:

```bash
watch -n 2 'sudo k3s kubectl -n temperature get pods; sudo k3s kubectl -n temperature top pods'
```

`Ctrl+C` exits watch. Kubernetes and the applications keep running.

## 3. Terminal B — events and notifications

```bash
sudo k3s kubectl -n temperature logs -f -l app=consumer \
  --all-containers=true --max-log-requests=8 --prefix --tail=20
```

In a second tab:

```bash
sudo k3s kubectl -n temperature logs -f -l app=notifier \
  --all-containers=true --max-log-requests=8 --prefix --tail=20
```

Consumers should report `topic=experiment commit_mode=sync`. The course controls when experiments arrive. Silence while the course producer is idle is normal. `notification_sent` means the real Notifications service accepted the request. The numeric `response` is its reported latency. Per-reading INFO logs were removed to avoid wasting CPU and disk under load; use SQL or the dashboard to inspect temperatures.

`Ctrl+C` disconnects log streaming; it does not stop the Pods.

## 4. Terminal C — verify the API, stored results and monitoring

```bash
curl -fsS http://127.0.0.1:3003/ready
curl -fsS http://127.0.0.1:3003/health
curl -fsS http://127.0.0.1:3008/-/ready
python3 scripts/verify_runtime.py
```

Inspect recent real measurements without printing any password:

```bash
docker exec group17-temperature-observability-database-1 \
  psql -U temperature -d temperature -c \
  "SELECT experiment_id,measurement_id,measured_at,temperature,out_of_range
   FROM measurements WHERE during_experiment
   ORDER BY measured_at DESC LIMIT 10;"
```

For a chosen experiment, set its actual ID and time range before querying:

```bash
experiment_id='PASTE_AN_ACTUAL_EXPERIMENT_ID'
start_time=0
end_time=$(date +%s)
curl -fsS -G http://127.0.0.1:3003/temperature \
  --data-urlencode "experiment-id=$experiment_id" \
  --data-urlencode "start-time=$start_time" \
  --data-urlencode "end-time=$end_time"
curl -fsS -G http://127.0.0.1:3003/temperature/out-of-range \
  --data-urlencode "experiment-id=$experiment_id"
```

`/dashboard?experiment-id=...` is our temperature chart. The course Grafana is `https://grafana.cec.dlandau.nl/`; log in with your course credentials. Its notification/REST panels describe the external evaluator's observations. Prometheus on port 3008 remains available for the course's optional infrastructure panels. Choose a recent time range to separate new results from errors caused during earlier development/migration. Do not interpret a historical red total as a new failure without checking its time window.

The runtime report separates `active_pending` from `quarantined_unsent`. The latter should remain 3045 unless separately reviewed and explicitly changed. Do not mark them sent or release their retries into the real service.

## 5. Repeat an isolated Kafka test

The prepared group VM has the temporary benchmark DSN in `/tmp/group17-final-db.env`, mode 600, and the Docker network name in `/tmp/group17-trial-network`. Do not print or commit the DSN. These are test prerequisites, not part of the Kubernetes deployment.

```bash
python3 scripts/benchmark_kafka.py \
  --count 100 --candidate-only --consumers 2 \
  --candidate-image final-v4 --commit-mode sync
```

This starts the official producer on **group17**, two isolated test consumers, and a mock notifier. It stores test data in a new schema in **final_bench**. The primary consumers stay on **experiment**. Never change the production runtime ConfigMap to `group17` to run this test.

For a strict same-stream reference comparison, omit `--candidate-only` and use `--count 40`. It requires the retained `baseline-final` consumer image. Both variants share VM/DB resources, so this is a paired comparison, not independent maximum-capacity measurement.

The benchmark stops its own containers afterward, retains schemas and JSON evidence, and does not send development hashes to the real Notifications API. Test schemas consume storage; review them separately before any deletion.

## 6. Rebuild/update the current release

Run builds on the group node where the images are installed:

```bash
bash deploy/kubernetes/build-images.sh
python3 deploy/kubernetes/render.py --active > deploy/kubernetes/manifests.json
sudo k3s kubectl apply --dry-run=server -f deploy/kubernetes/manifests.json
sudo k3s kubectl apply -f deploy/kubernetes/manifests.json
sudo k3s kubectl -n temperature rollout status deployment/api
sudo k3s kubectl -n temperature rollout status deployment/consumer
sudo k3s kubectl -n temperature rollout status deployment/notifier
python3 scripts/verify_runtime.py
```

Use a **new image tag** and update `render.py` for a future code change. `IfNotPresent` with an unchanged tag does not roll Pods or distribute a rebuilt image. A gateway ConfigMap change also needs `rollout restart deployment/gateway` after applying; its `subPath` mount is not updated live.

`prepare_shadow.py`, `activate_api.py`, `activate_monitoring.py` and `activate_workers.py` record the one-time migration, not everyday startup commands. The worker handover script requires Kubernetes workers at zero and the original Docker container names. Do not rerun it on this active deployment.

K3s/systemd and Docker restart policies maintain services after reboot. Routine SSH logout does not stop them.

## 7. Ibrahim and Yorick — what to do now

First stop the legacy personal consumer and its database tunnel **on your own personal VM**, not on the group VM:

```bash
cd ~/group17-temperature-observability
docker compose --env-file .env.personal \
  -f deploy/personal-consumer.compose.yaml stop consumer
sudo systemctl disable --now group17-db-tunnel.service
```

If the Compose command fails because its old `.env.personal` is incomplete, inspect `docker ps` and stop the exact old container directly:

```bash
docker stop group17-personal-consumer-consumer-1
```

The group database deliberately rejects the old shared administrator's TCP access. Do not ask for that administrator password or remove the rejection rule to revive an obsolete consumer.

Get the new code using the GitHub SSH key already configured on your VM:

```bash
git fetch origin
git switch --track origin/codex/final-performance
```

If the branch already exists locally, `git switch codex/final-performance`, then `git pull --ff-only`. Preserve any local edits before switching. Your existing Git deploy key is for repository access; it is not the Kubernetes node-join token or VM login key.

You can immediately participate as an HTTP test client without a DB password or Kafka credentials:

```bash
curl -fsS http://63.33.242.251:3003/health
python3 scripts/load_api.py --help
```

Coordinate total HTTP request rates across teammates. Three clients each sending 250 req/s produce 750 req/s total, which has not been validated. Share the exact experiment ID, time range, rate and test duration so results are comparable.

### Optional: register the two remaining Kubernetes nodes

This has **not** been executed for Ibrahim or Yorick. Only Nick and the group VM are currently registered. It requires their VM access or their execution of the following steps; a Git key cannot do it.

1. Keep TCP 6443 and 10250 plus UDP 8472 reachable **only among the cluster VMs**. Do not expose overlay/control-plane ports publicly. Verify private routing to `10.0.1.183`.
2. Nick obtains the K3s join token from the group VM and transfers it privately into a mode-600 file on the joining VM. This token grants node admission; do not put it in Git, chat, screenshots or shell command arguments.
3. With Docker active, each teammate chooses a distinct node name and runs:

```bash
node_name=ibrahim-client2  # Yorick uses yorick-client3
sudo install -d -m 700 /etc/rancher/k3s
# Securely place the supplied token at /etc/rancher/k3s/join-token first.
sudo chmod 600 /etc/rancher/k3s/join-token
curl -fsSL https://get.k3s.io -o /tmp/install-k3s.sh
sudo env INSTALL_K3S_VERSION='v1.36.5+k3s1' \
  K3S_URL='https://10.0.1.183:6443' \
  K3S_TOKEN_FILE='/etc/rancher/k3s/join-token' \
  sh /tmp/install-k3s.sh agent --docker --node-name "$node_name" \
  --node-taint role=load:NoSchedule
```

4. On the group VM, `sudo k3s kubectl get nodes -o wide` must show the new distinct names as Ready. They start tainted for load generation, just like Nick. Adding a node does not multiply PostgreSQL throughput.
5. Before scheduling applications there, build/import **the exact release images** onto that Docker node, verify pod-to-database connectivity and resource capacity, then remove the load taint deliberately and repeat the combined correctness/latency profile. Do not remove the taint first: these local image tags are not hosted in a public registry.
6. Keep the same production `KAFKA_GROUP_ID` for shared consumers. Different group IDs each receive the stream and do not provide cooperative work sharing. Do not increase consumer count merely to use all machines; the measured two-consumer configuration performed better.

For this course-sized workload, using personal VMs as independent load generators is already useful. A four-node application deployment and its performance have not been claimed or measured.

## 8. Recovery

Inspect `get pods`, `describe pod`, `logs --previous` and `rollout history` before making a change. To stop a failed new rollout, restore the previously verified image tag and corresponding configuration; keep the limited DB roles. The initial Docker containers remain stopped as migration rollback material.

A full return to the old Docker application is not a routine `docker start`: Kubernetes owns NodePorts 3003/3008, and old containers use the retired administrator DB login. It requires a coordinated worker stop, removal of those NodePort Services, and reconfiguration of the old application with restricted DB users. Restoring the old pg_hba file would also revive legacy tunnel access; do not do that as an incidental troubleshooting step.

Do not delete the PostgreSQL container/volume, Prometheus volume, Kubernetes Secrets or database schemas as a performance fix.
