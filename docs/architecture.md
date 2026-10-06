# Architecture

The first milestone runs the complete solution on the group VM. The public REST API listens on port `3003`.

The Kafka consumer, REST API, PostgreSQL database, notifier and monitoring
components are separate services. Consumer instances share one Kafka consumer
group and the PostgreSQL database. All experiment state, partial sensor groups,
temperature history, and pending notifications are stored in PostgreSQL. This
lets a replacement consumer continue after a crash or partition rebalance.

```text
Kafka -> consumer -> PostgreSQL -> REST API -> researcher
                            \-> notifier -> course Notifications API
node-exporter + API/consumer metrics -> Prometheus :3008 -> course Grafana
```

The consumer's database transaction finishes before its Kafka offset is
committed. Unique measurement IDs make Kafka redelivery safe. A PostgreSQL
outbox records notification requests in the same transaction as the triggering
measurement; three notifier replicas use row locks to claim different jobs.
The API has two workers and reads only completed measurements after the
experiment started.

Additional personal VMs may later run stateless consumer or API replicas if the course staff permits using them for the group assignment. The group VM remains the public entry point required by the demo instructions.

## Kafka ingestion

The consumer reads the Java-style SSL properties supplied to Group 17,
connects to the course brokers, subscribes to the configured topic, and
decodes each Avro object container. Offsets are committed only after the
decoded event has been stored in PostgreSQL.

The group VM exposes the API and Prometheus. A personal VM can later run an
additional consumer using the same database and Kafka group. PostgreSQL is
currently bound to loopback by default; multi-VM deployment needs a private
network path to it and suitable access control.
