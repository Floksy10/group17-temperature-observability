# Final performance upgrade — measured results

The current group VM runs the API, Kafka processing, notification delivery and monitoring in K3s. PostgreSQL stays on its existing Docker volume. The course-facing API remains on **63.33.242.251:3003** and Prometheus on **:3008**. The course topic remains `experiment`; its existing consumer group was retained during cutover.

## Selected configuration

| Component | Selected setting | Reason |
| --- | --- | --- |
| API | Two Pods, one Uvicorn/uvloop worker each | Measured better cached request latency than the candidate Docker setup; independent rolling updates |
| API connection pools | Maximum four DB connections per Pod | Limits DB contention and connection memory |
| History cache | Estimated 32 MiB per worker, at most 1024 entries; cache only completed histories of at most 4096 points | Avoids retaining unbounded histories; active data stays fresh |
| Live or oversized queries | Indexed SQL filters by experiment/time/out-of-range | Read the requested window instead of loading a complete history into Python |
| Kafka | Two consumers, maximum 64 messages per transaction, 20 ms collection timeout | Best measured latency/resource balance in the tested workload |
| Kafka commits | Synchronous, explicit per-partition offsets after successful DB commit | Async variants performed worse; failed DB work is never acknowledged |
| Sensor processing | Merge consecutive readings for the same measurement within each experiment | Fewer SQL round trips; lifecycle order is preserved |
| SQL writes | `unnest` insertion + aggregate CTE; pipeline independent final writes | Fewer calls and network waits, with the same durable state and outbox |
| Notifications | Two Pods, separate restricted DB role, readiness after DB connection, graceful shutdown | Durable retries without stopping all ready delivery workers during an update |
| HTTP gateway | Nginx keepalive, endpoint DNS refresh and GET retries | Handles backend changes without exposing stale Pod connections to clients |
| API probes | `/ready` checks initialized/open pool without borrowing a connection; `/health` keeps the DB check | Prevents pool saturation from taking every API Pod out of service |
| Scaling | API HPA min 2/max 4, target 75% CPU, PDB minAvailable 1 | Automatic API scaling configured; an actual HPA scale-up was not demonstrated |

The 32 MiB cache budget is an estimate of retained cache objects, **not** a hard 32 MiB process RSS ceiling. Kubernetes memory limits are separate.

## Measurements

Results were collected on the existing group VM, with the HTTP client on Nick's VM. Repeated comparisons are separate from the final combined run. These are measured profiles, not universal capacity limits.

| Scenario | Previous/reference implementation | Selected implementation |
| --- | --- | --- |
| Active histories, 5000 points each, query a 20-point window, target 100 req/s, two repetitions | 13.38–14.20 req/s; p95 3.63–4.02 s | 99.88–100.00 req/s; p95 94–111 ms; all bodies correct |
| DB-only ingestion, 8800 events, two repetitions | 198–216 events/s, one transaction per event | Pipeline batch64: 547–662 events/s; compacted validation run: 701 events/s |
| Same real Kafka stream, 40 experiments × 4 sensors | Two baseline consumers: notification p95 14.964 s, max 15.130 s | Two compacting consumers: p95 0.778 s, max 1.577 s; exact history/outbox equality |
| 100 experiments × 4 sensors, sampled once/second | Four candidate consumers: max 2.087 s | Two candidate consumers: max 1.547 s; all 2000 experiment measurements present |
| Final combined load, 100 experiments plus 250 HTTP req/s | — | **12,500/12,500 valid HTTP responses**, zero errors; HTTP p95 334 ms; mock notification p95 0.662 s/max 0.819 s |
| Final API update with overlapping traffic | — | **3750/3750 valid responses**, zero errors; p95 147 ms |
| Official service, steady sample from 22:15 UTC | — | 68 accepted notifications; largest returned latency **0.533 s**; later post-notifier-update runtime sample: 36 responses, max **0.904 s** |

Kafka development tests use `group17`, unique consumer groups, separate schemas in `final_bench`, and a local mock notification receiver. Their notification latency does **not** prove external-service latency. Official service results are a separate observation at the course's live load, not a controlled 100-experiment stress test.

The active-history improvement is mainly avoiding unnecessary database/Python work. Kubernetes supplies scheduling, lifecycle management, resource limits and API scaling; installing Kubernetes alone does not make the application faster.

The final combined HTTP profile queries preserved completed histories while Kafka writes separate test schemas on the same PostgreSQL server. Test consumers run as isolated Docker containers using the same selected consumer image. The public API is the deployed Kubernetes API. The final controlled Kafka load was not generated through Kubernetes Jobs.

## Rejected results and fixes

- Four consumers were slower than two in the selected 100-experiment workload. More workers can compete for the same database and CPU.
- Async commits without sensor compaction reached 13–18 s maximum notification latency. They were rejected; synchronous commits remain selected.
- A pipeline-only variant without compaction exceeded ten seconds with two consumers. Batching alone was insufficient.
- The first combined deployment test returned 3500 HTTP 502 errors out of 10,000 requests. DB-dependent readiness and Nginx passive upstream disabling amplified saturation. The final code uses `/ready` and `max_fails=0`; its repeated combined test had zero errors. Both result sets are retained.
- An early notifier update delayed one official notification to 13.326 s. Notifier readiness now waits for an established DB connection before Kubernetes retires the previous ready Pod. Its regression tests cover both connection success and failure.
- Raw `sent_at - measurement.timestamp` for course events was about 18 seconds larger than the latency returned by the official service. Group VM chrony was synchronized to AWS Time Sync. The cause of the external timestamp difference is unproven; official returned latency is reported separately, and raw values are retained. No timestamps were rewritten.

## Correctness and isolation

**30 unit/contract tests passed:** API 18, batching 6, experiment model 3, notifier readiness 2, cost calculation 1. Real database tests additionally verify complete sensor averaging, notification transitions, replay idempotency and rollback. The shared-stream Kafka comparison checks exact hashes of histories and all five notification payload fields.

The first handover exposed legacy personal consumers still importing development-topic messages into the course database. A full payload comparison identified **3045** matching unsent test notifications. With explicit user approval, only these IDs were set to `next_attempt_at='infinity'`; `sent_at` remains NULL. They are retained for audit, not counted as successfully delivered. New test events use an isolated database.

Old TCP connections under the administrator role `temperature` are rejected for database `temperature`. Local Unix-socket `psql` administration remains available. API, consumer and notifier use separate DB users. API credentials allow SELECT only. Kafka auth files and the notification token are separate Kubernetes Secrets mounted only into the services that need them. No Secrets are in the Git manifests.

## Resource and availability limits

A recorded application snapshot had API Pods about 54–60 MiB each, consumers about 30–32 MiB each, notifiers about 29 MiB each, Nginx 4 MiB and Prometheus about 97 MiB. These are observations, not peak guarantees. Reducing consumer/notifier counts saves application resources, but K3s adds roughly 575 MiB of control-plane overhead on the group VM. **A lower AWS bill has not been demonstrated.** EC2 instances have not been resized or removed.

Two nodes are currently registered and Ready: `group17` and `nick-client4`. Nick is tainted for load generation. Ibrahim and Yorick are not registered Kubernetes nodes; older SSH tunnels and Git deploy keys never constituted Kubernetes membership. Their next steps are in the team runbook.

PostgreSQL, the K3s server and the HTTP gateway still depend on the group VM. This is not a highly available multi-server cluster. No database replication, long soak test, node-loss failover, forced Kafka rebalance failure test, or HPA scale-up has been demonstrated. Growing historical storage remains an operational concern; no assessed data was deleted.

## Evidence and source

- [HTTP summaries and compressed per-request traces](final-evidence/http/)
- [Same-stream comparison](final-evidence/kafka-25eb56c7.json)
- [Final combined Kafka profile](final-evidence/kafka-1388303b.json)
- [Official service sample](final-evidence/official-service-latency.json)
- [Approved quarantine audit](final-evidence/notification-audit.json)
- [Runtime state](final-evidence/runtime.json)
- [Operating and teammate instructions](final-kubernetes-runbook.md)

Implementation is in `services/api/app.py`, `services/consumer/batching.py`, `services/consumer/storage_pipeline.py`, `services/notifier/notifier.py` and `deploy/kubernetes/render.py`.

Technical references: [Kubernetes probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/), [Psycopg pipeline mode](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html), [K3s setup](https://docs.k3s.io/quick-start), [Nginx upstream configuration](https://nginx.org/en/docs/http/ngx_http_upstream_module.html).
