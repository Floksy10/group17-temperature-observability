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
