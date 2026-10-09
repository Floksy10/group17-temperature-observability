# API latency diagnosis — 9 October 2026

This report records the original implementation before the subsequent
[API optimization](api-optimization.md). Its production-state statements and
rankings describe that earlier diagnosis; the implementation report records
the newer code and worker/cache decisions.

Follow-up: the [independent machine stress test](stress-test-2026-10-09.md) includes higher rates, CPU/memory monitoring, host packet timing and a newer cached-response prototype. It demonstrates most responses within 5 ms for evenly spaced requests, but not for bursts, and documents concurrent course traffic.

## Conclusions

There are two measured sources of delay:

1. **With cached histories, bursts spend most of their time waiting between framework stages.** Looking up the cached history and selecting the requested measurements take hundredths of a millisecond. Dispatching the synchronous endpoint, dispatching response validation, resuming tasks and passing through the request middleware take tens of milliseconds under a burst.
2. **With uncached histories, acquiring a database connection and completing database round trips become substantial.** The fast cached path alone does not fix this workload.

The strongest tested candidate combines asynchronous cached endpoints with direct ASGI request-metrics middleware. Two matched comparisons with an independent async client reduced mean time to response headers from **86.4/88.6 ms to 47.8/46.5 ms**, and p95 from **156.6/206.6 ms to 94.4/101.0 ms**. This is approximately a **45–48% reduction in mean response time** in this controlled workload. It is not a measured improvement in the official course run.

The earlier increase from two to four workers is not established as a general improvement. In the original implementation, two workers performed at least as well in the controlled comparisons. Four workers helped the combined prototype, which has a different request-processing path. Worker count must be evaluated with the implementation it will actually serve.

**Production remains on its original application code and four workers.** The temporary diagnostic container has been removed. Its creation time and container ID were unchanged, and `/health` returned `{"status":"ok"}` after cleanup.

## What was measured

- Group VM: `63.33.242.251`, private address `10.0.1.183`; verified instance type `t3a.medium`, two vCPUs and about 3.756 GiB usable RAM. No Docker CPU or memory quota was configured.
- Client: the actual client2 VM, `52.16.239.8`, private address `10.0.1.102`.
- Production image: `sha256:e8219043523a92f6cee2e03de447e5996690790d5d64e0509dd231831e2d9452`. Live `/app/app.py` matched the checkout, SHA-256 `44b0f508828fdc36eda5c187d4c9b51e44b371ec236fc36124e54c7248f836be`.
- Runtime: FastAPI 0.115.12, Starlette 0.46.2, Uvicorn 0.34.0, Pydantic 2.13.5, psycopg 3.2.6 and psycopg_pool 3.3.3. Neither `uvloop` nor `httptools` was installed.
- An isolated API copy used the same image and PostgreSQL database. Its port 13003 was bound to localhost and the group VM's private address. Only this diagnostic container was restarted between variants.
- Input: an export of **650 actual terminated experiments**, with actual timestamps, temperatures and out-of-range flags. Histories contained 80–380 measurements, median 160. The 100-experiment subset contained 19,579 points; the full subset contained 127,347. No invented experiment data was used.
- Both required endpoints were exercised, with random inclusive time windows for `/temperature`. Every measured response was compared exactly with the exported histories.
- Main workload: 94 requests per second in one-second batches, up to 50 concurrent requests, persistent HTTP connections, ten measured seconds. Warm-up was excluded. The course repository's requestor also processes batches and limits in-flight requests; the exact deployed course-generator version and settings were not available.
- Independent drivers: a Python thread client and a Python async I/O client. The latter checked that the result did not depend on waking fifty client threads.
- Evidence retained: **19,740 measured responses across 21 cases, all valid, zero errors**. These counts exclude warm-up and exclude the early cases whose timing stages overlapped. The preserved warm-up summaries also show zero errors.

The production API, consumers and notifiers stayed running. The database was shared with them; this was not an otherwise idle, dedicated benchmark machine. The course HTTP effective-rate query had no current series at the beginning and final check, but other ingestion could continue. The completed-experiment count increased from 3,251 to 3,256 between inspection and cleanup.

## How long each step took

All values below are **milliseconds of wall time**, including any scheduling delay while the operation was in progress. They are not measurements of exclusive CPU time.

The burst column is `aio-baseline4-burst94-r1`: 940 responses, 99.26% cache hits. The steady column is `corrected4-steady94`: the original implementation and the same 94 requests/second, spread evenly rather than batched; 99.57% cache hits. These are individual runs, not an average across all repeats.

| Processing step | Burst mean | Burst p95 | Steady mean |
|---|---:|---:|---:|
| HTTP request parsed → ASGI task begins | 6.774 | 14.107 | Not instrumented in this earlier run |
| Framework/middleware before endpoint dispatch, excluding the next two rows | 10.170 | 21.141 | 0.547 |
| Request parameter validation | 0.027 | 0.032 | 0.019 |
| Wait to enter endpoint thread | 14.773 | 32.023 | 0.305 |
| Cache lookup | 0.007 | 0.010 | 0.009 |
| Acquire database connection, averaged across hits and misses | <0.001 | 0 | <0.001 |
| Experiment-status SQL and result retrieval, averaged across hits and misses | 0.051 | 0 | 0.003 |
| Measurement SQL and result retrieval, averaged across hits and misses | 0.078 | 0 | 0.005 |
| Release connection / transaction completion, averaged across hits and misses | 0.075 | 0 | 0.002 |
| Assemble history and remaining loader overhead | 0.011 | 0.012 | 0.010 |
| Insert completed history in cache, averaged across hits and misses | <0.001 | 0 | <0.001 |
| Select requested time range or out-of-range list; remaining endpoint work | 0.013 | 0.019 | 0.013 |
| Wait to enter response-validation thread | 9.138 | 24.421 | 0.180 |
| Response validation itself | 0.244 | 0.479 | 0.076 |
| Pydantic model serialization | 0.392 | 2.203 | 0.203 |
| Serialization dispatch/resumption and other residual overhead | 12.602 | 31.266 | 0.168 |
| Render JSON bytes | 1.489 | 5.185 | 0.525 |
| Framework/middleware after endpoint, excluding serialization/rendering above | 19.879 | 40.748 | 0.407 |
| **ASGI entry → response headers offered to Uvicorn** | **68.951** | **141.804** | **2.475** |
| Remaining transport/socket/client delay, after subtracting parsed-request queue and ASGI time | 10.701 | 22.960 | Not separately instrumented |
| **Client request start → response headers** | **86.426** | **156.625** | **4.493** |
| Read response body, measured separately | 10.215 | 40.178 | See evidence summary |

The ASGI subtotal includes the application rows, not the protocol queue or remaining transport row. The means approximately add up; **p95 values cannot be added**, because the 95th-percentile request can differ for each stage. Very low database averages and zero p95 values in this table reflect the high cache-hit rate. They do not imply that an individual cache miss is free.

About 66.6 ms of the 69.0 ms ASGI mean in this burst was endpoint/response dispatch and framework overhead. Cache selection and actual response validation were much smaller. The same requested rate produced 4.5 ms mean when requests were evenly spaced. Average requests per second alone therefore does not describe this workload's latency.

The installed [FastAPI 0.115.12 routing source](https://github.com/fastapi/fastapi/blob/0.115.12/fastapi/routing.py) confirms two thread-pool transitions for these synchronous, response-model endpoints: endpoint execution and response validation. [Starlette documents synchronous endpoint offloading](https://starlette.dev/threadpool/), using AnyIO with a default 40-token limit. We did not measure token saturation directly; the recorded dispatch waits include scheduling and thread handoff, so increasing the token limit is not justified by these timings alone.

### When the cache is cold

`aio-async-asgi4-cold650` started a fresh diagnostic API with no application warm-up. It queried the same 650 real histories. Only 14.15% of its 940 requests hit the cache. Mean response time was **162.5 ms**, p95 **362.1 ms**.

Among its **807 cache misses**, mean wall times were:

| Miss-path step | Mean ms |
|---|---:|
| Wait to enter the database thread | 21.27 |
| Acquire a pooled connection | 46.30 |
| Experiment-status SQL and result retrieval | 35.57 |
| Measurement SQL and result retrieval | 36.62 |
| Return connection / transaction completion | 18.68 |
| Other endpoint work and resumption | 3.24 |

By comparison, the four misses in the evenly spaced baseline run averaged about **0.036 ms to acquire a connection, 0.675 ms for status SQL, 1.233 ms for measurement SQL, and 0.547 ms to release it**. That small sample is a low-contention reference, not a distribution estimate.

This points to concurrent connection use, connection creation and scheduling/round-trip contention. It does not establish that PostgreSQL spends 36 ms executing the measurement query. The required history index already exists. Adding another identical index is not supported by the evidence.

**Shared-database limit:** PostgreSQL's configured `max_connections` was 100. At the final larger-cache inspection, the diagnostic API held 48 idle connections and the live API held 38; total client backends including inspection were 96. The final diagnostic container logged zero connection-slot errors, but this snapshot cannot prove earlier cold tests never approached the limit. After diagnostic removal, client backends dropped to 44. Cold results must be interpreted with this shared connection budget in mind. Four production workers already allow up to 60 API connections; more pools or workers should not be added without budgeting the other services.

## Which changes actually helped

All rows used the same real data, both endpoints, 94 requests/second in batches and 940 measured responses. Warm-up and driver differences are stated explicitly. Every measured response was correct.

| Implementation | Workers | Driver | Mean response-header time ms | p95 ms |
|---|---:|---|---:|---:|
| Live production, unchanged | 4 | Threads | 83.3 | 166.5 |
| Original implementation, corrected trace | 4 | Threads | 84.4 | 181.7 |
| Original implementation, repeat | 4 | Threads | 135.1 | 350.2 |
| Original implementation | 2 | Threads | 85.4 | 156.7 |
| Direct `JSONResponse`, retaining sync endpoint | 4 | Threads | 85.3 | 181.3 |
| Direct ASGI metrics wrapper, retaining sync endpoint | 4 | Threads | 94.8 | 194.5 |
| Async cached endpoint, retaining original metrics middleware | 4 | Threads, two runs | 68.0 / 63.7 | 124.7 / 138.8 |
| Async cached endpoint + direct ASGI metrics | 4 | Threads, two runs | 68.2 / 51.4 | 199.0 / 94.4 |
| **Original implementation** | **4** | **Async, two runs** | **86.4 / 88.6** | **156.6 / 206.6** |
| **Async cached endpoint + direct ASGI metrics** | **4** | **Async, two runs** | **47.8 / 46.5** | **94.4 / 101.0** |
| Original implementation | 2 | Async | 77.0 | 126.2 |
| Async cached endpoint + direct ASGI metrics | 2 | Async | 78.5 | 141.3 |

These are short sequential runs on shared machines, not confidence intervals or randomized trials. Persistent connections distributed unevenly across workers. The large variation is real and is preserved in the table. The combination improved both async-client repeats; neither a direct JSON response nor replacing middleware alone demonstrated a benefit in its individual trial.

The combination's first async trial spent only 3.05 ms inside ASGI, but end-to-end response-header time was 47.76 ms. Parsed-request dispatch took 15.61 ms and the remaining socket/transport/client component took 29.10 ms. **Reducing handler time does not remove waiting before the handler begins.** Further optimization should measure the entire request, not just the handler timer.

### Cache capacity, separately tested

Both cases below used the combined prototype, four workers, async client, 650 real histories and 10,000 excluded warm-up requests. Capacity was the only intended configuration difference; the cases ran sequentially.

| Cache capacity per worker | Hit rate | Mean ms | p95 ms |
|---|---:|---:|---:|
| 512 | 77.98% | 61.73 | 154.08 |
| 1,024 | 97.98% | 49.39 | 90.63 |

Mean improved about 20% and p95 about 41%. The larger-cache replica used 336.5 MiB after this test; this is total container memory, not a measured memory delta. A bigger cache is useful when the queried working set exceeds capacity. It does not prevent the first cold lookup or make caching active experiments safe. The current cache correctly stores only terminated histories.

## What the course Grafana data says

### Live follow-up at approximately 16:58 UTC / 18:58 Amsterdam

After the user's observation that the 0.25 bucket remained largest, a new
authenticated query checked the actual dashboard data source and its histogram.
Over the latest five minutes, mean RTT from the histogram sum/count was
**103.1 ms**. The 100–250 ms bucket contained **48.69%** of responses and was
the largest individual bucket. **51.28%** were at or below 100 ms, spread across
the smaller buckets; approximately 0.03% exceeded 250 ms. The latest hour's
mean was **107.4 ms**, with 48.90% in the 100–250 ms bucket.

The largest individual bucket is not necessarily a majority, and its upper
boundary is not the average. A 103 ms average is consistent with this measured
distribution. This is a current course measurement, separate from both the
morning's 164–177 ms samples and the controlled 86.4 ms benchmark. No live
improvement from the proposed async/middleware prototype has been established.
The query evidence is preserved in `histogram-followup-1658.json`.

### Morning measurements and histogram interpretation

The supplied histogram buckets include 0.1, 0.25 and 0.5 seconds. In the inspected histogram presentation, the bar labelled **0.25 represents requests above 0.1 and at or below 0.25 seconds**. It is not proof that requests take exactly 250 ms or that an explicit 250 ms timer exists.

Historical course Prometheus queries, authenticated through Grafana, showed:

| UTC on 9 October | Workers at that time | Target / effective requests per second | Mean RTT ms | Estimated p95 ms |
|---|---:|---:|---:|---:|
| 07:36:45 | 2 | 85 / 85 | 150.2 | 288.4 |
| 07:37:15 | 2 | 85 / 85 | 175.1 | 444.3 |
| 08:26:45 | 4 | 94 / 94 | 163.8 | 347.1 |
| 08:27:15 | 4 | 94 / 94 | 177.1 | 414.2 |
| 08:29:15 | 4 | 42 / 42 | 78.4 | 209.7 |

These samples use one-minute rate windows. Histogram quantiles are interpolated estimates. The worker cutover was verified from the live container's creation time, 08:02:55 UTC. Workload rate, queried histories, ingestion and timing differed, so these historical runs cannot establish a causal two-versus-four-worker result.

The course requestor source times the request through receipt of response headers, including its spawned request task; body decoding and validation happen afterwards. Our response-header metric is the closest comparison, but it starts after our client's own concurrency-slot wait. The central sender's scheduling delay and exact network route were not directly traced.

**An exact stage reconstruction of the earlier official run is unavailable:** production had no per-request stage traces or cache-hit metric then. The controlled tests establish mechanisms and test candidate fixes; they do not prove the earlier run had the same cache-hit rate or identical stage proportions.

### API's own Prometheus metrics need repair

The production API does not configure multiprocess aggregation. Fresh `/metrics` connections returned `/temperature` counters alternating among 73,652, 78,332, 84,449 and 85,646 from different workers. A single scrape therefore cannot represent the whole service, and switching processes can look like counter resets.

Any earlier service-wide average or percentile inferred from these local API counters is unreliable. The independent course RTT histogram is unaffected by this collection bug. [Prometheus Python's documented multiprocess mode](https://prometheus.github.io/client_python/multiprocess/) requires shared storage configured before import, a `MultiProcessCollector` registry, and lifecycle cleanup. Node metrics should continue to come from node-exporter rather than unsupported process collectors.

## Ranked options

Ranking considers measured benefit, correctness, implementation scope and whether the relevant workload was actually tested.

| Rank | Option | Evidence and expected use | Effort / risks |
|---|---|---|---|
| **Prerequisite** | Fix multiprocess metrics; add cache-hit/miss, pool wait and event-loop lag measurements | Required for trustworthy future comparisons. No latency reduction claimed. | Moderate; configure shared metric storage and process cleanup correctly. |
| **1** | **Async cached endpoints + direct ASGI request-metrics middleware** | Strongest repeated cached-workload result: 86–89 → 46–48 ms mean with the async client. | Moderate. Keep blocking DB calls off the event loop; preserve validation, response ordering and inclusive boundaries. Recheck notification latency during a real ingestion run. |
| **2** | **Size and warm the terminated-history cache for the measured working set** | For 650 histories, 512 → 1,024 capacity improved hits 78% → 98%, mean 62 → 49 ms and p95 154 → 91 ms. | Small configuration change, conditional benefit. Budget memory; measure actual course cache pressure. Use only completed immutable histories. |
| **3** | **Improve the miss path: connection budgeting/prewarming, duplicate-miss suppression, fewer DB round trips** | Miss-path waits are measured; these specific remedies have not been benchmarked. Relevant to new/uncached histories, not the already warm 100-experiment case. | Moderate. Measure pool statistics first. Larger pools can exhaust PostgreSQL connections. Single-flight loading needs bounded cleanup and must not make live data stale. |
| **4** | **Test `uvloop` and `httptools` in an isolated image** | Both are absent. Uvicorn documents faster alternatives; protocol/event-loop waiting remains after the leading change. No measured benefit here yet. | Small-to-moderate dependency change. Repeat full correctness and burst tests; do not substitute vendor performance claims for results. |
| **5** | **Upgrade the pinned FastAPI/Starlette stack** | FastAPI 0.130.0 introduced direct Pydantic JSON-byte serialization, which our 0.115.12 pipeline lacks. Benefit to this API is unmeasured. | Broader compatibility scope. Verify exact float values, error status codes, lifespan and middleware behavior. |
| **6** | **Change VM capacity or distribute API replicas** | Potentially useful for sustained contention; no matched larger-VM trial or CPU-credit measurement was available. | Higher scope/cost. Measure CPU credits, steal time and sustained utilization before resizing; shared DB limits still apply. |

For rank 1, [FastAPI's async guidance](https://fastapi.tiangolo.com/async/) supports keeping blocking I/O off the event loop; the prototype directly reads the in-memory cache and offloads only a miss. [Starlette's pure ASGI middleware documentation](https://starlette.dev/middleware/#pure-asgi-middleware) explains the direct interface and the limitations of `BaseHTTPMiddleware`. The performance ranking comes from our comparisons, not a blanket claim that ASGI middleware is always faster.

For rank 3, [psycopg documents pool statistics](https://www.psycopg.org/psycopg3/docs/advanced/pool.html#pool-statistics), including `requests_waiting`, `requests_queued` and `requests_wait_ms`. Collect these per worker before changing pool sizes. A genuinely async database pool is another candidate, but it was not tested and would be a wider change than the cached-path prototype.

For rank 4, [Uvicorn's settings documentation](https://uvicorn.dev/settings/) describes `uvloop` and `httptools` as higher-performance implementations. Actual benefit remains to be measured with the installed application and both endpoints.

For rank 5, the [FastAPI release notes for 0.130.0](https://fastapi.tiangolo.com/release-notes/#01300) describe the new serialization path. The project's advertised JSON speedup is not a measured end-to-end speedup for this service.

For rank 6, the verified `t3a.medium` is a burstable instance. [AWS documents CPU-credit behavior](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/burstable-credits-baseline-concepts.html): depletion limits bursting in Standard mode, whereas Unlimited mode uses surplus credits. Its current credit mode and balance were not measured, so throttling is **not an established cause**. Check [CloudWatch CPU-credit metrics](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/burstable-performance-instances-monitoring-cpu-credits.html) before attributing delay to this.

### Options not supported as the next fix

- **More workers by default:** four did not consistently beat two in the original code on this two-vCPU VM. Re-evaluate after the processing-path change; do not increase beyond four without memory and connection budgets.
- **Direct JSONResponse alone:** 85.3 ms mean versus an 84.4 ms traced baseline is no demonstrated gain. It also bypasses FastAPI's automatic response-model processing, as described in the [direct-response documentation](https://fastapi.tiangolo.com/advanced/response-directly/).
- **Middleware replacement alone:** its individual trial was slower. The measured combination is what supports rank 1.
- **More database indexes:** the required history index is already present and low-contention measurements are fast.
- **Make all functions async without replacing/offloading blocking DB access:** this would block the event loop on a miss.
- **Smooth the examiner's load or alter the histogram:** useful as a diagnostic comparison, but neither improves the service under the actual supplied load.

## Evidence and next implementation gate

The preserved evidence directory is [`latency-evidence/2026-10-09`](latency-evidence/2026-10-09/):

- `summaries.json`: full configurations, timings and correctness counts for all retained cases.
- `request-traces.jsonl.gz`: individual measured requests, with case names and per-stage timings.
- `fixtures.json.gz`: the actual measurement histories used for validation, without researcher details or credentials.
- `manifest.json`: counts, raw-trace hashes, excluded preliminary cases, fixture hash and cleanup checks.
- `course-*.json`: the course queries and historical results.
- `live-runtime.json`: the inspected image, command, limits and actual database history-size distribution.

Diagnostic code is in `scripts/profile_api_entry.py` and `scripts/profile_api_client.py`. The entry script is a pinned-version profiling adapter, not a production entry point. It accepts `PROFILE_VARIANT=baseline`, `direct_json`, `baseline_asgi`, `async_cached` or `async_cached_asgi`. The client supports thread or async scheduling, batch/steady/serial patterns and fixture-based correctness checking. Earlier overlapping timing cases were excluded; corrected stage sums were checked against ASGI time.

To repeat a case, use the existing API image on an isolated private port, mount the profiling entry script, provide the real database connection securely, and select the variant. Decompress the preserved fixtures and run, for example:

```bash
python3 profile_api_client.py \
  --base-url http://10.0.1.183:13003 --fixtures fixtures.json \
  --experiments 100 --client-mode async --pattern burst \
  --rate 94 --seconds 10 --warmup 1600 --output comparison.jsonl
```

Before deploying rank 1, implement it in ordinary application code, verify both endpoint contracts and error cases, then run a longer matched test with concurrent real ingestion. Compare course validation errors, effective versus target HTTP rate, notification latency, cache hit rate, pool waits, whole-request p95, memory and worker distribution. The assignment explicitly evaluates data consistency, keeping up with HTTP load and notifications within ten seconds; there is no stated requirement in the supplied README that every HTTP request must complete in a particular histogram bucket.

No production code change, new commit or push was made for this diagnosis.
