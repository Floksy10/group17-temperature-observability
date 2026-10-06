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

Four consumers met the assignment's ten-second notification latency target in
this test, with more margin than three. Docker Compose keeps four consumers as
the default replica count. These results cover the specified 100 concurrent
experiments at a one-second interval; larger sensor counts, longer runs and the
real Notifications API still require validation.

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
