# Can Group 17 start another course HTTP run?

Checked on 9 October 2026 using the actual client2 credentials and the authenticated course Grafana API.

## Finding

The repository supplies a runnable HTTP generator, but the credentials available to Group 17 cannot currently read its input topic. There is no documented student control for starting or restarting the central generator.

- The course [`Makefile`](../../cec-assignment/Makefile) has a `run_http` target. It starts the `http-load-generator` container after copying an environment configuration.
- [`CONFIGURATIONS.md`](../../cec-assignment/CONFIGURATIONS.md) identifies the Kafka brokers, document topic, consumer group, workload settings, authentication directory and target host file it needs.
- The generator's [`Consume`](../../cec-assignment/http-load-generator/src/consume.rs) reads Avro experiment documents from Kafka. Its HTTP server exposes only `GET /metrics`; it has no start-run endpoint.
- A read-only permission check on client2 successfully read one event from `experiment` (16 partitions). The same credentials received `TOPIC_AUTHORIZATION_FAILED`, Kafka error 29, for `experiment-document`.
- The check disabled automatic commits, automatic offset storage and automatic topic creation, and explicitly assigned a partition rather than joining the production consumer group. It did not publish events or change production offsets.
- The course Prometheus API returned `up=1` for `job="http-load-generator"`, `instance="http-load-generator:3002"`. That confirms the central metrics endpoint was reachable, not that a new workload was active.
- The [demo instructions](../../cec-assignment/DEMO-INSTRUCTIONS.md) explicitly grant read-only access to the live `experiment` topic. They provide no mechanism to trigger the central workload.

Evidence: [permission check](latency-evidence/2026-10-09/runner-access-check.json), [central metrics target](latency-evidence/2026-10-09/course-runner-target.json).

## Available paths

Subsequent [independent stress testing](stress-test-2026-10-09.md) confirmed that we can test the API directly ourselves. Historical central metrics also revealed official traffic overlapping those tests, although we did not trigger it. Empty instantaneous metric queries alone do not prove the course workload was absent throughout a longer test.

1. Ask course staff to start a fresh central HTTP run. This tests the official network path and updates the existing course histogram.
2. To run the supplied generator ourselves, course staff would need to grant access to the document topic and provide its actual workload settings. Running it on client2 would produce a separate metrics endpoint; the existing central Grafana dashboard would require its Prometheus configuration to include that endpoint before it could display those measurements.
3. Continue isolated HTTP tests on client2 using the completed, real experiment histories already exported from the group database. These require no document-topic access. They do not update the official histogram and cannot prove the official-path 5 ms target.

The earlier Docker demo images on client2 belong to the container tutorials. No course HTTP generator was running there when checked.

## Current 5 ms candidate evidence

The prepared prototype pre-encodes immutable completed histories, serves cached responses directly through ASGI, and uses uvloop/httptools. Production application code has not been replaced with this prototype.

Six additional isolated tests each sent 1,880 measured requests at 94 requests/second with up to 50 concurrent requests. All 11,280 responses were checked against real database histories; there were zero response errors. Stage tracing was disabled during these tests.

| Prototype and client validation | Mean to response headers | Fraction at or below 5 ms |
| --- | ---: | ---: |
| Encoded responses, asyncio/h11, validation deferred | 22.76 ms | 3.46% |
| Encoded responses, uvloop/httptools, validation deferred | 14.92 ms | 12.98% |
| Direct cached ASGI, uvloop/httptools, validation deferred | 10.81 ms | 22.98% |
| Direct cached ASGI, ordinary validation, repeat 1 | 21.67 ms | 2.29% |
| Direct cached ASGI, ordinary validation, repeat 2 | 21.51 ms | 4.36% |
| Direct cached ASGI, ordinary validation, 650 histories | 21.26 ms | 2.13% |

Deferred validation means checking all response bodies after the timed workload completes, rather than parsing and checking each body on the Python client's event loop during the workload. The timing difference indicates that the client affects the measurement. It does not establish an exclusive server processing time or prove the course target has been met. The official generator uses Rust, Tokio and reqwest, so its fresh measurements or an independent equivalent client are needed before drawing a 5 ms conclusion.

These tests show improvement, but **none demonstrates most responses at or below 5 ms**. Do not present the deferred-validation result as the official course measurement. Exact configurations, timestamps and thresholds are saved in [target5-summaries.json](latency-evidence/2026-10-09/target5-summaries.json).

The diagnostic API container was removed after testing. The production API container identity, creation time and image remained unchanged, and its health endpoint returned `{"status":"ok"}`.
