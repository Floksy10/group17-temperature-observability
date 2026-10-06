# Group 17 handoff

## What is running

The group VM (`63.33.242.251`, private address `10.0.1.183`) hosts the
PostgreSQL database, public API, three notification workers, four Kafka
consumers, Prometheus, and node-exporter. Its repository checkout is
`/home/ubuntu/group17-temperature-observability` on branch
`nick/measurement-aggregation`. Docker Compose is configured in
`deploy/compose.yaml`. Credentials are outside Git in `/home/ubuntu/group17-auth`
and `.env`.

Nick's client4 VM (`54.195.153.114`, private address `10.0.1.216`) has an
optional fifth consumer in `deploy/personal-consumer.compose.yaml`. It joins the
same Kafka consumer group and writes to the group VM's PostgreSQL database
through a restricted SSH tunnel. The database is **not** exposed on a public
interface. The tunnel is managed by `group17-db-tunnel.service` on Nick's VM.
The ignored `.env.personal` file contains the database password and Kafka
settings. The optional consumer is currently stopped because the group-only
configuration had better measured latency; start it with the command below
when testing distributed processing. Two teammates' VMs have not been
connected yet.

The data path is: course Kafka events → consumer replicas → PostgreSQL → REST
API and notification outbox → notification workers → course Notifications API.
Consumers only commit Kafka offsets after database processing. Measurements are
averaged across all configured sensors, and the public history excludes
stabilization-phase readings.

## Where to look

- [API health](http://63.33.242.251:3003/health)
- [Temperature chart](http://63.33.242.251:3003/dashboard?experiment-id=611a65a4-a55a-4d6f-b0a0-78085848f37e)
- [Infrastructure cost estimate](http://63.33.242.251:3003/costs)
- [Our Prometheus](http://63.33.242.251:3008/targets)
- [Course Grafana](https://grafana.cec.dlandau.nl/) → **Group Personal View**

The course Grafana uses the lecturers' central metrics and notification
database. Its Group 17 infrastructure target was absent from the central
Prometheus target list on 6 October 2026 even though our Prometheus was
reachable on port 3008. The local `/costs` page therefore provides our own
cost and resource overview. The course's event and HTTP panels will become
meaningful during their official demo workload.

## How to inspect or restart the group deployment

Use your own authorized SSH key to log into the group VM, then:

```bash
cd ~/group17-temperature-observability
docker compose --profile test --env-file .env -f deploy/compose.yaml ps
docker compose --profile test --env-file .env -f deploy/compose.yaml logs --tail 30 consumer notifier api
curl -fsS http://127.0.0.1:3003/health
```

The `test` profile keeps the local mock Notifications receiver available while
the development topic is selected. The production notification URL is set in
`.env` for the course demo. Do not commit `.env`, private SSH keys, Kafka
keystores or the notification token.

To inspect Nick's optional consumer on his VM:

```bash
systemctl status group17-db-tunnel --no-pager
cd ~/group17-temperature-observability
docker compose --env-file .env.personal -f deploy/personal-consumer.compose.yaml ps
docker compose --env-file .env.personal -f deploy/personal-consumer.compose.yaml logs --tail 30
docker compose --env-file .env.personal -f deploy/personal-consumer.compose.yaml start consumer
```

## Performance and scaling decision

We tested 100 concurrent experiments with two sensors each, one sample per
second and 20 stored measurements, while issuing 2,000 API requests at
concurrency 50. Four consumers on the group VM completed every measurement
and notification, with 5.549 s p95 and 6.693 s maximum notification latency
to the local mock receiver. Adding Nick's VM as a fifth consumer still met
the ten-second target but was slower: 6.773 s p95 and 9.579 s maximum.
Full commands and results are in [performance.md](performance.md). Keep four
group VM consumers as the measured baseline; use personal VMs only after a
matching load test demonstrates a benefit for the intended workload.

## Before the course demo

1. On the group VM, set `KAFKA_TOPIC=experiment` and
   `NOTIFICATIONS_URL=https://notifications.cec.dlandau.nl/api/notify` in the
   ignored `.env` file. The group credentials have read access to the demo
   topic. Do not start a local producer on that topic.
2. Recreate the group consumers and notification workers with
   `docker compose --env-file .env -f deploy/compose.yaml up -d --force-recreate consumer notifier`.
   Stop the test mock if it is running; it is no longer the notification target.
3. If Nick's optional consumer participates, set `KAFKA_TOPIC=experiment` in
   `.env.personal` and recreate it with
   `docker compose --env-file .env.personal -f deploy/personal-consumer.compose.yaml up -d --force-recreate`.
   Otherwise stop that personal consumer so the tested four-replica group VM
   configuration is used.
4. Check `/health`, then use the course Grafana and API queries to validate
   the actual demo stream. The development producer's encrypted hashes were
   rejected by the real Notifications API, so the local mock test does **not**
   prove the real notification path. Verify at least one real notification
   before relying on it in assessment.

## Onboarding another personal VM, if useful

1. Give the teammate access to the GitHub repository through their own GitHub
   identity. Clone the same branch on their VM. Never copy Nick's SSH private
   key or the group VM deploy key.
2. Copy the Group 17 Kafka TLS files to `/home/ubuntu/group17-auth` on that VM
   through the teammate's own secure channel. Create an ignored, mode `0600`
   `.env.personal` with `POSTGRES_PASSWORD`, `KAFKA_TOPIC`,
   `KAFKA_GROUP_ID=group17-temperature-observability`, and
   `KAFKA_AUTH_DIR=/home/ubuntu/group17-auth`. Ask the group VM maintainer for
   the database password through a secure channel, not Git or chat.
3. Confirm that the teammate VM can reach the group VM's **private** SSH
   address. Generate a new dedicated Ed25519 tunnel key on that VM. The group
   VM maintainer must add only its public key to `authorized_keys`, restricted
   to the teammate's private IP and `permitopen="127.0.0.1:5432"`. Verify the
   group VM SSH host-key fingerprint before accepting it. Nick's tunnel uses
   `SHA256:BOXpNQHf0c+mr2JC/d+O+tj0VN7id87tIkTveyndL1s` as of this setup.
4. Run an SSH local forward from the VM's Docker host-gateway address on port
   `15432` to group `127.0.0.1:5432`, supervised by systemd. On Nick's VM the
   Docker gateway is `172.17.0.1`; check the teammate VM instead of assuming
   the same address. Keep the forward off public interfaces.
5. Run `docker compose --env-file .env.personal -f deploy/personal-consumer.compose.yaml up -d --build`.
   Confirm a small test produces complete database records and that the new
   consumer actually logs `measurement_aggregated`. Repeat the 100-experiment
   test before adopting the extra VM for the demo.

These steps require each teammate's VM private IP and their own SSH access.
No teammate machine has been changed by this setup.
