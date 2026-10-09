# Independent API stress test — 9 October 2026

## Findings

We can stress test the group VM ourselves from client2 without controlling the lecturer's generator. This test used actual, completed experiment histories from the group database.

**The current API can deliver most responses within 5 ms when requests are evenly spaced. It did not do so under bursts.** At the same requested rate of 94 requests/second:

- Evenly spaced requests: mean **4.07 ms**, p95 **6.99 ms**, **84.63%** at or below 5 ms.
- One-second batches: initial mean **83.87 ms**, p95 **188.71 ms**, **1.17%** at or below 5 ms. A later repeat averaged **154.77 ms**, with no responses at or below 5 ms.

Packet timestamps on the group VM confirmed that most of the burst delay occurred between request arrival and response departure on that machine. It was not merely the client reading an already available response late. The prepared cached-response prototype improved the observed burst results, but did not reach the 5 ms target for most requests.

## Setup

- Time: **20:06:55–20:11:09 CEST** on 9 October 2026 (18:06:55–18:11:09 UTC).
- Server: group VM `63.33.242.251`, private `10.0.1.183`, two vCPUs, about 3.756 GiB usable memory.
- Generator: client2, `52.16.239.8`, private `10.0.1.102`, two vCPUs, about 1.869 GiB usable memory. Its existing Minikube and personal consumer stayed running.
- Input: the same 100 real terminated histories used in the earlier diagnosis, with actual timestamps, temperatures and out-of-range flags. The saved fixture export contains 650 histories; these tests select the first 100.
- Both required API endpoints, random inclusive temperature windows, persistent HTTP connections, maximum 50 concurrent requests.
- Each case: 1,600 warm-up requests, excluded from measurements; 20 seconds of measured traffic.
- Requested rates: 94, 188 and 376 requests/second. The first is an earlier measured course rate; the latter two are explicit stress-test settings, twice and four times that rate. They are not claimed to be the lecturer's workload settings.
- The Python async client read each response body during the workload, then validated every response against the real histories after the timed workload ended. This avoids JSON validation blocking other client requests during timing.
- Timing is request start to response headers. Waiting for a client concurrency slot is recorded separately and is excluded from this timer. It is not a complete user's queue-to-body latency.
- Group and client CPU/memory sampled approximately once per second; database activity and queues sampled approximately every three seconds. Packet capture was restricted to client2's private address and ports 3003/13003; raw packet payloads were deleted after extracting timing records.
- Production application code and its four-worker configuration remained unchanged. The candidate ran on private port 13003 and was removed after testing.

## Measured results

All **31,960 measured responses across nine cases were correct; zero errors**. These counts exclude warm-up.

| API / path | Pattern | Requested req/s | Achieved req/s | Mean headers ms | p95 ms | At or below 5 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Current / private | Burst | 94 | 93.98 | 83.87 | 188.71 | 1.17% |
| Current / private | Burst | 188 | 187.93 | 109.32 | 223.76 | 0.00% |
| Current / private | Burst | 376 | 369.15 | 108.11 | 275.93 | 1.33% |
| Candidate / private | Burst | 94 | 93.98 | 12.43 | 35.62 | 21.44% |
| Candidate / private | Burst | 188 | 187.92 | 33.59 | 104.09 | 4.12% |
| Candidate / private | Burst | 376 | 375.75 | 29.36 | 93.97 | 5.94% |
| Current / private, repeat | Burst | 94 | 93.98 | 154.77 | 323.63 | 0.00% |
| Current / public | Burst | 94 | 93.97 | 80.91 | 159.66 | 0.00% |
| Current / private | Evenly spaced | 94 | 94.01 | 4.07 | 6.99 | 84.63% |

The candidate pre-encodes completed immutable histories, serves cached responses directly through ASGI, and uses uvloop/httptools. Both APIs had four workers and a 512-history cache limit. Its application wrapper is a diagnostic prototype, not a production release with completed contract checks.

## Where the delay occurred

Host packet timestamps independently measured incoming HTTP request to outgoing response headers. Every measured private-path request was matched to a response; public-path packets were excluded by the capture filter.

| Case | Client mean ms | Server host packet interval mean ms | Server interval at or below 5 ms |
| --- | ---: | ---: | ---: |
| Current, burst 94 | 83.87 | 83.40 | 1.60% |
| Current, burst 188 | 109.32 | 108.95 | 0.00% |
| Current, burst 376 | 108.11 | 107.74 | 1.61% |
| Candidate, burst 94 | 12.43 | 11.28 | 30.11% |
| Candidate, burst 188 | 33.59 | 32.81 | 5.56% |
| Candidate, burst 376 | 29.36 | 28.65 | 8.82% |
| Current, burst 94 repeat | 154.77 | 154.31 | 0.00% |
| Current, evenly spaced 94 | 4.07 | 3.53 | 90.16% |

These are host software packet timestamps, not exclusive application CPU times or hardware NIC timestamps. The interval includes kernel, Docker forwarding, socket queues, worker scheduling and application processing. It excludes the remote client's network round trip and scheduling. The earlier [stage diagnosis](latency-diagnosis.md) separately measured framework handoffs and database stages.

## Machine resources

| Case | Group CPU mean / maximum, all cores | Minimum available group memory | Client CPU mean |
| --- | ---: | ---: | ---: |
| Current burst 94 | 35.9% / 50.2% | 2,224 MiB | 15.6% |
| Current burst 188 | 57.3% / 83.1% | 2,219 MiB | 17.5% |
| Current burst 376 | 91.1% / 97.2% | 2,220 MiB | 27.6% |
| Candidate burst 94 | 26.5% / 38.5% | 1,916 MiB | 14.0% |
| Candidate burst 188 | 30.3% / 42.1% | 1,901 MiB | 18.3% |
| Candidate burst 376 | 35.3% / 53.2% | 1,904 MiB | 20.7% |
| Current burst 94 repeat | 46.6% / 58.0% | 2,212 MiB | 17.5% |
| Current public burst 94 | 39.7% / 54.4% | 2,226 MiB | 19.8% |
| Current evenly spaced 94 | 27.2% / 32.0% | 2,226 MiB | 18.3% |

The current implementation approached full machine CPU utilization at 376 requests/second. Memory exhaustion was not observed: the group VM retained at least 1,901 MiB available, and client2 retained at least 372 MiB during measured cases. One-second CPU averages cannot rule out brief saturation within each burst.

Database samples showed **zero lock waiters and zero unsent notifications**. Pending readings briefly reached six in the initial 94-request case, three at 188 and two at 376; later cases sampled zero. The candidate increased client backend count to 81, below the configured 100-connection limit. These are periodic samples, not proof that no transient backlog or pool wait occurred between samples. Cached historic queries do not establish cold-query or ingestion capacity.

The monitor was restarted to correct Docker's terminal-format parsing. Earlier host CPU/memory samples were preserved, but initial API-container CPU readings were missing. The 376-request current case has only ten group host samples because of that restart. Other measured cases have approximately 19–20 samples. CPU percentages in this table are whole-machine values; Docker's container CPU percentages use 100% per core and must not be confused with these values.

## Concurrent course traffic and limits

Historical Prometheus queries revealed **official successful HTTP traffic during the test**, with 30-second lookback rates ranging from about 7 to 97 requests/second. The rates around the measured cases differed: roughly 36 during the initial baseline, 54–76 during candidate cases, and 87 during the baseline repeat. The central generator no longer had a current metric series at the final check; an empty instantaneous query alone was insufficient to establish that no official traffic overlapped the tests.

This background traffic and ongoing ingestion shared the group VM. Therefore:

- The measured values describe real shared-machine behavior, not a dedicated idle benchmark.
- Do not claim a precise causal percentage improvement from the sequential candidate comparison.
- The public/private results are not a controlled network-only comparison.
- The evenly spaced test is a demonstrated scenario where most requests meet 5 ms. It does not establish that the course burst workload meets that target.
- These short tests do not establish sustained capacity or the VM's AWS CPU-credit balance/mode.

## Repeating a test ourselves

On client2, the prepared client and actual fixture export are retained in `/home/ubuntu/group17-latency-20261009`. For a 20-second public API burst test:

```bash
cd /home/ubuntu/group17-latency-20261009
python3 profile_api_client.py \
  --base-url http://63.33.242.251:3003 \
  --fixtures fixtures.json --experiments 100 \
  --client-mode async --defer-validation \
  --pattern burst --rate 94 --seconds 20 --concurrency 50 \
  --warmup 1600 --output repeat-public-burst94.jsonl
```

Change `--pattern burst` to `--pattern steady` to spread the same number of requests evenly. Report the pattern and both correctness and latency results together. These commands produce our own measurements; they do not update the lecturer's HTTP histogram.

Exact per-case results, resource records, packet timing records, course traffic queries and request traces are in [the evidence directory](latency-evidence/2026-10-09/stress-test/results.json). Production container identity and image were verified unchanged after cleanup, and `/health` returned `{"status":"ok"}`.
