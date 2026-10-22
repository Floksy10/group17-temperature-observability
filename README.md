# Group 17 Temperature Observability

Avro Kafka events → durable sensor aggregation → historical temperature REST API and researcher notifications.

**Current group deployment:** K3s/Kubernetes, API `http://63.33.242.251:3003`, Prometheus `http://63.33.242.251:3008`. PostgreSQL remains on its existing Docker volume. Course consumers read `experiment` and notify the real service using the group's token. Development tests read `group17`, use an isolated database and a local mock.

- [Measured performance, selected settings and limitations](docs/final-performance.md)
- [Terminal commands, operation and English team handoff](docs/final-kubernetes-runbook.md)
- [Russian explanation](docs/final-explanation-ru.md)
- [Evidence](docs/final-evidence/)

Earlier Compose and personal-consumer guides are retained for history. They do not describe the current Kubernetes deployment. Do not start the old Compose workers alongside the current workers.

## Interfaces

- `GET /temperature?experiment-id=...&start-time=...&end-time=...`
- `GET /temperature/out-of-range?experiment-id=...`
- `/health` checks DB access; `/ready` is the cheap application readiness probe.
- `/dashboard?experiment-id=...` plots stored temperatures; `/costs` shows infrastructure observations and a compute-cost estimate.
- `/metrics` exports application metrics.

## Implementation

`services/api` contains FastAPI/ASGI history serving and bounded caches. `services/consumer` implements transactional batches, sensor aggregation and a durable notification outbox. `services/notifier` delivers retries independently. `deploy/kubernetes/render.py` generates the live manifests without embedding credentials. `scripts` and `tests` contain repeatable correctness and load checks.

Kafka credentials and the notification token are mounted as separate Kubernetes Secrets. The API, ingest and notifier use separate DB roles. Never commit `.env`, private keys, token files or database DSNs.

The official [course assignment](https://github.com/EC-labs/cec-assignment) defines the required interfaces. A successful mock test does not prove official-service delivery; see the separate real-service observation in the results.
