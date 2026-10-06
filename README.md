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

See [the team handoff](docs/team-handoff.md) for the current deployment, demo
checklist, and optional setup for other personal VMs. The
[four-VM workflow](docs/four-vm-workflow.md) gives each teammate the exact
connection and coordinated load-test steps.

## Credentials

Credentials and tokens must not be committed. On the group VM they are stored in `/home/ubuntu/group17-auth` and should be mounted read-only into containers that need them.

The official assignment repository is available at <https://github.com/EC-labs/cec-assignment>.

## Run on the group VM

Create an ignored `.env` in the repository root. Set `POSTGRES_PASSWORD` to a
random hexadecimal string, `KAFKA_TOPIC=group17`, and the paths to the group
Kafka credential directory and notification token. Then run:

```bash
docker compose --env-file .env -f deploy/compose.yaml up -d --build
docker compose --env-file .env -f deploy/compose.yaml ps
```

The API serves `http://<group-vm-ip>:3003/temperature`,
`/temperature/out-of-range`, `/health`, `/dashboard`, and `/costs`. The
temperature dashboard plots stored history for an experiment ID. The cost
dashboard shows observed CPU and memory usage and an estimated running compute
cost over time. Prometheus listens on port `3008` for infrastructure monitoring.
`VM_HOURLY_COST_USD` in `.env` controls the estimate; the default is $0.0408
for the group VM's t3a.medium in eu-west-1. This is an EC2 compute estimate,
not the full AWS bill. Storage, network, public IPv4, CPU credits, taxes and
discounts are excluded.

For the course demo, set `KAFKA_TOPIC=experiment` in `.env` and recreate the
consumer. The group credentials can only read that topic, so local tests use
`group17`.

The consumer stores each Kafka event in PostgreSQL before committing its
offset. A separate notifier sends durable notification requests with retries.
The REST API excludes stabilization measurements from historic results.

For development with the course producer image, set
`NOTIFICATIONS_URL=http://notifications-mock:3000/api/notify` in `.env` and run
`docker compose --profile test --env-file .env -f deploy/compose.yaml up -d --build`.
The mock checks the HTTP request but cannot validate the encrypted
`measurement_hash`. Before the course demo, switch the URL back to the real
course service and verify one notification using events from the `experiment`
topic. The development producer's hashes were rejected by the real service,
so the mock test alone does not prove that the real notification path works.

## Repeat the load test

On the group VM, with the development topic and mock receiver selected in
`.env`, create 100 simultaneous experiments with two sensors sampled once per
second for 20 measurements:

```bash
cd ~/group17-temperature-observability
python3 scripts/generate_experiments.py --count 100 --samples 20 > /tmp/group17-load.json
docker run -d --name group17-load \
  --mount type=bind,source="$HOME/group17-auth",target=/experiment-producer/auth,readonly \
  --mount type=bind,source=/tmp/group17-load.json,target=/experiment-producer/load.json,readonly \
  dclandau/cec-experiment-producer \
  --config-file /experiment-producer/load.json \
  --topic group17 --brokers kafka.cec.dlandau.nl:19092
docker wait group17-load
```

The container name must be changed or the old stopped container removed before
running the command again. The measured results and verification queries are in
[the performance notes](docs/performance.md).
