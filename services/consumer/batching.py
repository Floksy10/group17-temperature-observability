"""Bounded transactions and explicit offsets: never acknowledge failed work."""

from confluent_kafka import TopicPartition


def commit_offsets(messages):
    offsets = {}
    for message in messages:
        key = (message.topic(), message.partition())
        offsets[key] = max(offsets.get(key, 0), message.offset() + 1)
    return [TopicPartition(topic, partition, offset)
            for (topic, partition), offset in sorted(offsets.items())]


def compact_readings(records):
    # Experiments are independent. Within each one retain lifecycle and
    # measurement order; only consecutive readings of the SAME measurement merge.
    experiments = {}
    for name, record in records:
        experiments.setdefault(record["experiment"], []).append((name, record))
    compacted = []
    for rows in experiments.values():
        pending = []
        def flush():
            if pending:
                record = dict(pending[-1])
                record["_sensor_readings"] = [(r["sensor"], r["temperature"]) for r in pending]
                record["_record_count"] = len(pending)
                compacted.append(("sensor_temperature_measured", record))
                pending.clear()
        for name, record in rows:
            if name == "sensor_temperature_measured":
                if pending and pending[-1]["measurement_id"] != record["measurement_id"]:
                    flush()
                pending.append(record)
            else:
                flush()
                compacted.append((name, record))
        flush()
    return compacted


def process_batch(messages, pool, decode, process, compact=False):
    # Decode before taking locks. A failed batch is rolled back as a whole.
    records = [(name, record) for message in messages
               for name, record in decode(message)]
    if compact:
        records = compact_readings(records)
    with pool.connection() as connection:
        experiment_ids = sorted({record["experiment"] for _, record in records})
        # Canonical lock order avoids deadlocks between batches sharing experiments.
        connection.execute(
            "SELECT experiment_id FROM experiments "
            "WHERE experiment_id = ANY(%s) ORDER BY experiment_id FOR UPDATE",
            (experiment_ids,),
        ).fetchall()
        results = [(name, record, process(connection, name, record))
                   for name, record in records]
    return results
