# API optimization — 9 October 2026

## Implementation

- Completed experiment histories are encoded to JSON when loaded, rather than revalidated and re-encoded for every query. Temperature windows retain inclusive boundaries and original point ordering/values.
- A direct ASGI middleware path serves ordinary, valid cached GET requests. Duplicate, unusual or invalid parameters and other methods delegate to FastAPI, preserving its existing validation and error responses.
- Cache misses use asynchronous endpoints that offload blocking PostgreSQL work to a thread. Active histories and unknown experiments remain uncached.
- Request metrics use the ASGI response-start event rather than the old streaming middleware bridge. A cache hit/miss counter was added.
- Compose enables uvloop/httptools and Prometheus multiprocess aggregation. Metric files live in a 16 MiB `/tmp` tmpfs and are reset once before worker startup. Only supported counters/histograms are aggregated; machine resource metrics continue through node-exporter.
- The existing FastAPI, Starlette, Pydantic, AnyIO and psycopg-pool versions were verified on production and pinned. The change adds uvloop 0.23.0 and httptools 0.8.0.

Selected configuration: **two API workers**, **1,024 completed histories cached per worker**. The VM has two CPU cores. Database pool limits remain 15 connections per API worker; the selected worker count reduces the potential API pool total from 60 to 30.

## Verified contracts

The local suite passed 16 tests. The exact container image passed 13 API/cache/cost tests, including real-data HTTP contracts. Covered cases include exact numeric values, order, inclusive and single-instant boundaries, empty windows, missing/invalid/reversed parameters, duplicate parameters, infinity/whitespace delegation, unsupported methods, unknown IDs, active-history non-caching and counting responses once.

## Final implementation benchmarks

All six cases used the ordinary application image, with production metric aggregation enabled. The Python async client ran from client2 against the isolated API on the group VM's private port 13003. Each case sent 100 HTTP requests/second in one-second batches, maximum 50 concurrent requests, for 20 seconds. Response bodies were checked against the actual saved experiment histories after timed traffic completed.

| Workers | Histories | Cache / worker | Mean to headers | p95 | At or below 5 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4, repeat 1 | 100 | 512 | 10.16 ms | 19.07 ms | 16.50% |
| 2, repeat 1 | 100 | 512 | 9.14 ms | 16.68 ms | 15.80% |
| 2, repeat 2 | 100 | 512 | 9.31 ms | 17.69 ms | 14.75% |
| 4, repeat 2 | 100 | 512 | 13.70 ms | 32.93 ms | 18.20% |
| 2 | 650 | 512 | 41.49 ms | 175.27 ms | 6.85% |
| 2 | 650 | 1,024 | 12.34 ms | 22.81 ms | 12.40% |

All **12,000 measured responses were correct, with zero errors**. The 100-history cases used 1,600 excluded warm-up requests; the 650-history cases used 10,000. Aggregate request counters and cache counters matched the exact warm-up-plus-measured total for every case, confirming that metric scrapes collected all workers. Cache metrics in the saved text files include warm-up and must not be presented as measured-only hit rates.

Two workers had lower mean and p95 latency in the observed repeats; four had a slightly larger fraction at or below 5 ms. Two workers were selected for the better observed latency distribution and lower memory/connection budget. These are short sequential tests on shared VMs, not confidence intervals or proof that two workers are universally optimal.

After the larger-cache warm test, the two-worker candidate used 233.1 MiB of
container memory. The additional resource monitor stopped when one database
inspection exceeded its four-second timeout, so its samples are incomplete.
That inspection timeout is not counted as an API response failure, but it
prevents treating these samples as continuous notification monitoring.

The initial recent-course HTTP query had no series. Course ingestion resumed during later tests; a subsequent actual database snapshot contained ten active experiments and zero unsent notifications. The full 100-concurrent-experiment case was not reproduced in this implementation test.

Evidence: [summaries and aggregate metrics](latency-evidence/2026-10-09/implemented-api/summaries.json). The earlier [stress test](stress-test-2026-10-09.md) provides baseline, resource and independent host-packet measurements.

## Notification requirement

The rubric specifies **no more than 100 concurrently running experiments**, each with a one-second sampling interval, and notifications within ten seconds. It does not limit HTTP queries to 100/second. Multiple sensors can generate multiple Kafka events per experiment cycle.

Recent actual outbox records were inspected during preparation. For the latest 500 sent records, outbox creation to successful send averaged 0.111 seconds, p95 0.234 seconds, maximum 0.721 seconds. This interval uses one database clock and excludes Kafka/consumer delay.

A source-measurement-to-send calculation returned negative values for all 500 records (mean -6.880 seconds, maximum -0.580 seconds). Therefore those source timestamps cannot be used as a valid end-to-end notification latency check without resolving the clock/timestamp relationship. No claim of meeting the full ten-second requirement at 100 concurrent experiments is made from these samples.

## Deployment and remaining target

**Deployed at 20:40:31 CEST on 9 October 2026** from source commit
`ebb3247b542be519f47b1d82f0186afd08634e39`. Only the API service was recreated.
The consumers, database and notifiers retained their running containers.
The previous image and a private environment backup are retained on the group
VM for rollback.

The live image is
`sha256:ec7e6f3b4f47802122bd9a941c8912184e07e5373da7487983eda1fda76d4a07`.
The deployed application hash matched the checked-in source. Live checks
confirmed exact real-history responses, unchanged 422 errors, healthy database
access and six consistent aggregate metric snapshots across fresh connections.

A subsequent **public API burst test from client2** sent 100 HTTP requests/second
for 30 seconds, maximum 50 concurrent requests, using the same real 100-history
subset and 1,600 excluded warm-up requests:

- 3,000 measured responses, all correct, zero errors; achieved 99.98 requests/second.
- Mean response-header time **9.36 ms**, median **8.78 ms**, p95 **17.05 ms**,
  maximum **39.35 ms**.
- **15.13%** at or below 5 ms, **62.33%** at or below 10 ms,
  **99.43%** at or below 25 ms; all at or below 50 ms.
- Final snapshots: zero pending readings, zero unsent notifications and
  `/health` returned `{"status":"ok"}`.

No fresh official HTTP histogram samples were available in the final two-minute
query. The public test above is our own measurement and does not populate the
course generator's histogram. Deployment and verification evidence are saved
beside the benchmark evidence, including
[the live test summary](latency-evidence/2026-10-09/implemented-api/deployed-public-burst100.summary.json).

The **5 ms burst target remains unmet** in the implementation benchmarks. The measured improvement does not justify promising that most official course requests will meet it. Earlier evenly spaced requests demonstrated that the machine can meet 5 ms for most requests under that different pattern.
