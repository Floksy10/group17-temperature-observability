# Performance check, 6 October 2026

The group VM ran PostgreSQL, the Kafka consumers, three notification workers,
two API workers, Prometheus and node-exporter. The development Kafka topic was
`group17`; notifications went to the local mock receiver because the provided
development producer's ciphertext was rejected by the real course service.

Each run used 100 simultaneous experiments, two sensors per experiment, one
sample per second and 20 historic measurements. During each run, a client sent
2,000 history API requests with 50 concurrent requests. Every request
succeeded. Notification latency is measured from the original measurement
timestamp to the time the mock receiver accepted the notification.

| Kafka consumers | Historic measurements | Pending sensor readings | Pending notifications | Notification p95 | Notification max | API requests/s | API p95 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2,000 | 0 | 0 | 19.559 s | 20.319 s | 147.9 | 567.7 ms |
| 3 | 2,000 | 0 | 0 | 8.450 s | 9.594 s | 105.5 | 706.7 ms |
| 4 | 2,000 | 0 | 0 | 5.549 s | 6.693 s | 115.6 | 679.3 ms |
| 4 on group VM + 1 on Nick's VM | 2,000 | 0 | 0 | 6.773 s | 9.579 s | 100.7 | 860.7 ms |

Four consumers met the assignment's ten-second notification latency target in
this test, with more margin than three. Docker Compose keeps four consumers as
the default replica count on the group VM. A fifth consumer on Nick's VM also
met the limit, but did not improve latency or API throughput. That VM reaches
the shared PostgreSQL database through an SSH tunnel; network and database
contention can outweigh the extra CPU. Do not assume that adding the two other
personal VMs will improve performance without measuring it.

Additional single-run stress checks explored larger sensor counts and faster
sampling. Each used a fresh set of 100 experiments with 20 historic measurements
per experiment. Except where marked "no API load", 2,000 API requests at
concurrency 50 were sent during the producer run. Every run stored all 2,000 historic
measurements, finished all 100 experiments, and drained the pending-reading and
notification queues. All API requests completed successfully. These results
measure the local mock receiver, not the real Notifications API.

| Sensors | Sampling | Consumers | Kafka commit | API pool per worker | API load | Notification p95 | Notification max | API requests/s | API p95 |
| ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: |
| 4 | 1 s | 4 | synchronous | 15 | 2,000/50 | 14.805 s | 15.270 s | 106.6 | 665.7 ms |
| 4 | 1 s | 8 | synchronous | 15 | 2,000/50 | 14.757 s | 16.193 s | 74.4 | 1,017.1 ms |
| 4 | 1 s | 4 | asynchronous | 15 | 2,000/50 | 11.247 s | 11.814 s | 99.6 | 782.2 ms |
| 4 | 1 s | 8 | asynchronous | 15 | 2,000/50 | 15.492 s | 16.477 s | 69.4 | 926.7 ms |
| 4 | 1 s | 4 | asynchronous | 15 | no API load | 8.704 s | 9.019 s | — | — |
| 4 | 1 s | 4 | asynchronous | 5 | 2,000/50 | 16.545 s | 17.606 s | 92.8 | 818.7 ms |
| 2 | 0.5 s | 4 | asynchronous | 15 | 2,000/50 | 12.714 s | 14.248 s | 78.0 | 765.6 ms |
| 2 | 1 s | 4 | asynchronous | 15 | 2,000/50 | 11.885 s | 13.126 s | 72.7 | 993.0 ms |
| 2 | 1 s | 4 | synchronous | 15 | 2,000/50 | 5.249 s | 6.669 s | 100.7 | 800.8 ms |

With four sensors, synchronous commits and the standard API load, the delay
from measurement timestamp to outbox creation was 14.604 s at p95; delivery
from outbox creation to the mock receiver took 0.243 s at p95. This locates the
delay in ingestion and aggregation, not in the notifier. Turning off INFO logs
in one otherwise identical run reduced notification p95 only to 13.832 s.

The retained configuration is four consumers, synchronous Kafka commits,
the 15-connection API pool per worker, and INFO logs. It repeatedly met the
ten-second target for the two-sensor, one-second scenario. The tested heavier
profiles did not consistently meet the target; supporting those workloads
under simultaneous HTTP load needs further ingestion or database optimization.
These are observations from individual short runs, not a guarantee about every
possible experiment or the real Notifications API.

Two additional runs moved both the development producer and the 2,000-request
API load client from the group VM to Nick's VM. The group VM kept four
consumers, synchronous commits, the 15-connection API pool and INFO logs.
Both runs stored all 2,000 historic measurements and completed all requests:

| Location of test clients | Sensors | Notification p95 | Notification max | API requests/s | API p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nick VM, first run | 4 | 9.690 s | 11.466 s | 177.9 | 370.4 ms |
| Nick VM, second run | 4 | 13.775 s | 13.997 s | 162.6 | 536.3 ms |

Moving the test clients off the group VM improved API throughput in these
runs, but notification latency still varied and the ten-second maximum was
not consistently met. The [four-VM workflow](four-vm-workflow.md) assigns
Ibrahim and Yorick separate API load clients so the group can test that
division of work without adding database tunnels first.

To reproduce a four-sensor producer run from the group VM, while the development
topic and local mock receiver are selected in `.env`:

```bash
cd ~/group17-temperature-observability
python3 scripts/generate_experiments.py --count 100 --sensors 4 \
  --sample-rate-ms 1000 --samples 20 > /tmp/group17-100x4.json
docker run --name group17-100x4-demo \
  --mount type=bind,source="$HOME/group17-auth",target=/experiment-producer/auth,readonly \
  --mount type=bind,source=/tmp/group17-100x4.json,target=/experiment-producer/load.json,readonly \
  dclandau/cec-experiment-producer \
  --topic group17 --brokers kafka.cec.dlandau.nl:19092 \
  --config-file /experiment-producer/load.json
```

Use a new container name for every run, or remove the previous stopped
container with `docker rm group17-100x4-demo`.

To check cumulative state after a run, connect to PostgreSQL on the group VM:

```bash
docker exec group17-temperature-observability-database-1 \
  psql -U temperature -d temperature -c \
  'SELECT count(*) AS experiments, count(*) FILTER (WHERE terminated) AS terminated FROM experiments;'
docker exec group17-temperature-observability-database-1 \
  psql -U temperature -d temperature -c \
  'SELECT count(*) AS historic_measurements FROM measurements WHERE during_experiment;'
docker exec group17-temperature-observability-database-1 \
  psql -U temperature -d temperature -c \
  'SELECT count(*) FILTER (WHERE sent_at IS NULL) AS pending_notifications FROM notification_outbox;'
```

API request test:

```bash
python3 scripts/load_api.py --base-url http://127.0.0.1:3003 \
  --experiment-id <existing-experiment-id> --requests 2000 --concurrency 50
```

## API response-time trial after the course demo

On 9 October 2026, the course HTTP generator measured about 130 ms median and
340 ms p95 over five minutes at approximately 42 successful requests/second.
At lower rates near 20 requests/second, the median fell to roughly 50-70 ms;
at 76-85 requests/second, it rose to 130-160 ms. The group VM still had
available CPU and memory. PostgreSQL executed a representative 120-row
experiment lookup in under 1 ms. These observations suggest testing more API
workers, but they do not establish that workers are the only bottleneck.

The Compose file on this branch prepares `API_WORKERS=4`, while preserving
`API_CACHE_EXPERIMENTS=512`. The running demo deployment was left unchanged.
It currently has two API workers and a 512-experiment cache per worker. The
extra workers use more memory and database connections, so compare the same
workload before keeping them. The database held 2,659 completed experiments;
if the course HTTP generator queries more than 512 of them repeatedly, test a
larger cache in a separate trial so its effect is measurable.

After the demo, run the existing `scripts/load_api.py` from the same client VM,
using the same completed experiment ID, request count, and concurrency before
and after applying this Compose change. Warm the endpoint once before each
run. Check successful request count, throughput, p95 latency, API memory,
database health, and the course Grafana validation-error panels. This single-ID
test measures worker scaling, not cache capacity. To return to the previous
settings, put `API_WORKERS=2` in the ignored `.env` and recreate only `api`.
