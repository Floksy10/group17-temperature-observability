# Group 17 Temperature Observability

Group solution for the Cloud and Edge Computing Temperature Observability assignment.

## Responsibilities

- Consume Avro experiment events from Kafka.
- Calculate the average temperature across all sensors for each measurement.
- Store experiment temperature history after `experiment_started`.
- Expose the required REST API on port `3003`.
- Notify the course Notifications API when stabilization is reached or the temperature moves out of range.
- Expose monitoring information and support horizontal scaling.

## Repository layout

- `services/api/` — Temperature Observability REST API.
- `services/consumer/` — Kafka event consumer and processing logic.
- `database/` — database schema and migrations.
- `deploy/` — Docker Compose and deployment configuration.
- `monitoring/` — metrics and monitoring configuration.
- `tests/` — automated and integration tests.
- `docs/` — architecture and operating notes.

## Credentials

Credentials and tokens must not be committed. On the group VM they are stored in `/home/ubuntu/group17-auth` and should be mounted read-only into containers that need them.

The official assignment repository is available at <https://github.com/EC-labs/cec-assignment>.

## Run on the group VM

Create an ignored `.env` in the repository root. Set `POSTGRES_PASSWORD` to a
random hexadecimal string, `KAFKA_TOPIC=group17`, and the paths to the group
Kafka credential directory and notification token. Then run:

```bash
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml ps
```

The API serves `http://<group-vm-ip>:3003/temperature`,
`/temperature/out-of-range`, `/health`, and `/dashboard`. The dashboard plots
stored temperature history for an experiment ID. Prometheus listens on port
`3008` for the course Grafana infrastructure dashboard.

For the course demo, set `KAFKA_TOPIC=experiment` in `.env` and recreate the
consumer. The group credentials can only read that topic, so local tests use
`group17`.

The consumer stores each Kafka event in PostgreSQL before committing its
offset. A separate notifier sends durable notification requests with retries.
The REST API excludes stabilization measurements from historic results.
