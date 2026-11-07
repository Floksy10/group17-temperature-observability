# Group 17 Temperature Observability

Avro Kafka events → durable sensor aggregation → historical temperature REST API and researcher notifications.

**Deployment verified on 7 November 2026.** The group VM runs Docker Compose, with API `http://63.33.242.251:3003` and Prometheus `http://63.33.242.251:3008`. Its two course consumers read `experiment` in the same consumer group, with eight of the topic's sixteen partitions assigned to each. PostgreSQL and two notification workers also run on the group VM. K3s is stopped and masked on the group and Nick VMs.

- [Submission code review and source/runtime differences](docs/code-review-2026-11-07.md)
- [Historical Kubernetes API placement and Grafana latency analysis](docs/grafana-latency-followup.md)
- [Historical Kubernetes performance measurements](docs/final-performance.md)
- [Historical Kubernetes operation and team handoff](docs/final-kubernetes-runbook.md)
- [Russian explanation](docs/final-explanation-ru.md)
- [Evidence](docs/final-evidence/)

The live Compose deployment uses the protected files `/etc/group17-compose-20261022/runtime.env` and `/etc/group17-compose-20261022/compose.yaml` on the group VM, together with the older checkout at `/home/ubuntu/group17-temperature-observability`. The checked-in `deploy/compose.yaml` is the development template. Its default topic, replica counts and database login differ from the live overrides.

The live API image `group17/temperature-api:baseline-final` contains the API source from `0a81e25`. This checkout contains a newer API with bounded caching, filtered queries and `/ready`. The consumer and notifier logic matches the deployed `final-v4` and `final-v3` images. The proposed three-VM API replicas were tested with synthetic databases. Connecting them to the shared database and routing live traffic to them remains a separate deployment step.

## Interfaces

- `GET /temperature?experiment-id=...&start-time=...&end-time=...`
- `GET /temperature/out-of-range?experiment-id=...`
- `/health` checks DB access. The newer source provides `/ready`; the currently deployed older API returns 404 for that path.
- `/dashboard?experiment-id=...` plots stored temperatures; `/costs` shows infrastructure observations and a compute-cost estimate.
- `/metrics` exports application metrics.

## Implementation

`services/api` contains FastAPI/ASGI history serving and bounded caches. `services/consumer` implements transactional batches, sensor aggregation and a durable notification outbox. `services/notifier` delivers retries independently. `deploy/kubernetes/render.py` generates manifests for the historical Kubernetes configuration. `scripts` and `tests` contain repeatable correctness and load checks.

The live Compose deployment mounts Kafka credentials and the notification token from files on the group VM. The API, ingest and notifier use separate DB roles. Development producers and tests use `group17`, isolated data and a mock notification service. Never commit `.env`, private keys, token files or database DSNs.

The official [course assignment](https://github.com/EC-labs/cec-assignment) defines the required interfaces. A successful mock test does not prove official-service delivery; see the separate real-service observation in the results.
