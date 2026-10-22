"""Consume and decode experiment events from the course Kafka cluster."""

from __future__ import annotations

import io
import logging
import os
import signal
import socket
import time
from pathlib import Path

from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from prometheus_client import Counter, Histogram, start_http_server
from psycopg_pool import ConnectionPool

from storage_pipeline import process_event
from batching import process_batch, commit_offsets


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("group17-consumer")

AUTH_DIR = Path(os.getenv("KAFKA_AUTH_DIR", "/app/auth"))
PROPERTIES_FILE = AUTH_DIR / "client-ssl.properties"
RUNNING = True
EVENTS = Counter("group17_kafka_events_total", "Processed Kafka events", ["event"])
AGGREGATES = Counter(
    "group17_measurements_total", "Completed experiment measurements"
)
PROCESSING = Histogram(
    "group17_batch_processing_seconds", "Database processing per Kafka batch"
)
COMMIT = Histogram("group17_kafka_commit_seconds", "Kafka offset commit duration")
BATCHES = Histogram("group17_batch_messages", "Messages per database transaction", buckets=(1, 4, 8, 16, 32, 64, 128, 256))
LAG = Histogram("group17_ingestion_lag_seconds", "Measurement timestamp to durable processing", buckets=(.05, .1, .25, .5, 1, 2, 5, 10, 30))
ERRORS = Counter("group17_processing_errors_total", "Failed Kafka events")


def on_commit(error, partitions):
    if error is not None:
        ERRORS.inc()
        LOGGER.warning("offset_commit_failed error=%s; durable database rows will be replayed", error.code())


def load_properties(path: Path) -> dict[str, str]:
    """Load Java-style key=value properties without logging secret values."""
    properties: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        properties[key.strip()] = value.strip()
    return properties


def kafka_configuration() -> dict[str, object]:
    properties = load_properties(PROPERTIES_FILE)
    keystore_password = os.getenv(
        "KAFKA_KEYSTORE_PASSWORD", properties.get("ssl.keystore.password", "")
    )
    if not keystore_password:
        raise RuntimeError("Kafka keystore password is not configured")

    return {
        "bootstrap.servers": os.getenv(
            "KAFKA_BROKERS",
            "kafka.cec.dlandau.nl:19092,"
            "kafka.cec.dlandau.nl:29092,"
            "kafka.cec.dlandau.nl:39092",
        ),
        "group.id": os.getenv(
            "KAFKA_GROUP_ID", "group17-temperature-observability"
        ),
        "client.id": os.getenv("KAFKA_CLIENT_ID", socket.gethostname()),
        "auto.offset.reset": os.getenv("KAFKA_OFFSET_RESET", "latest"),
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "on_commit": on_commit,
        "security.protocol": "SSL",
        "ssl.ca.location": str(AUTH_DIR / "ca.crt"),
        "ssl.keystore.location": str(AUTH_DIR / "kafka.keystore.pkcs12"),
        "ssl.keystore.password": keystore_password,
        "ssl.endpoint.identification.algorithm": "none",
    }


def record_name(message: Message) -> str:
    for key, value in message.headers() or []:
        if key == "record_name" and value is not None:
            return value.decode("utf-8")
    raise ValueError("Kafka message has no record_name header")


def decode_records(payload: bytes | None) -> list[dict[str, object]]:
    if payload is None:
        raise ValueError("Kafka message has no payload")
    with DataFileReader(io.BytesIO(payload), DatumReader()) as reader:
        return list(reader)


def decode_message(message):
    name = record_name(message)
    return [(name, record) for record in decode_records(message.value())]


def record_results(results):
    # Log/metrics after the DB transaction commits, including on batch replay.
    for name, record, result in results:
        EVENTS.labels(name).inc(record.get("_record_count", 1))
        if result is not None:
            AGGREGATES.inc()
            LAG.observe(max(0.0, time.time() - float(record["timestamp"])))
            LOGGER.debug(
                "measurement_aggregated experiment=%s measurement_id=%s "
                "temperature=%.3f notification=%s",
                result.experiment_id, result.measurement_id,
                result.temperature, result.notification_type,
            )
        elif name != "sensor_temperature_measured":
            LOGGER.info("event=%s experiment=%s", name, record["experiment"])


def stop(_signum: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    topic = os.getenv("KAFKA_TOPIC", "group17")
    start_http_server(int(os.getenv("METRICS_PORT", "8001")))
    with ConnectionPool(os.environ["DATABASE_URL"], min_size=1, max_size=2) as pool:
        pool.wait(timeout=30)
        consumer = Consumer(kafka_configuration())
        commit_mode = os.getenv("KAFKA_COMMIT_MODE", "sync")
        if commit_mode not in {"sync", "async"}:
            raise ValueError("KAFKA_COMMIT_MODE must be sync or async")
        durable_offsets = {}

        def revoked(client, partitions):
            keys = {(p.topic, p.partition) for p in partitions}
            owned = [v for k, v in durable_offsets.items() if k in keys]
            if commit_mode == "async" and owned:
                try:
                    client.commit(offsets=owned, asynchronous=False)
                except KafkaException:
                    LOGGER.warning("rebalance_commit_failed; durable rows will be replayed")
            for key in keys:
                durable_offsets.pop(key, None)

        consumer.subscribe([topic], on_revoke=revoked)
        LOGGER.info("consumer_started topic=%s commit_mode=%s", topic, commit_mode)
        try:
            batch_size = int(os.getenv("KAFKA_BATCH_SIZE", "64"))
            batch_wait = float(os.getenv("KAFKA_BATCH_WAIT_SECONDS", "0.02"))
            if not 1 <= batch_size <= 256 or not 0 < batch_wait <= 0.1:
                raise ValueError("Batch size must be 1..256; wait must be (0, .1]")
            pending = []
            failures = 0
            while RUNNING:
                if not pending:
                    incoming = consumer.consume(batch_size, timeout=batch_wait)
                    for message in incoming:
                        if message.error():
                            if message.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            raise KafkaException(message.error())
                        pending.append(message)
                    if not pending:
                        continue
                try:
                    with PROCESSING.time():
                        results = process_batch(
                            pending, pool, decode_message, process_event, compact=True
                        )
                    # Explicit offsets include ONLY the committed database batch.
                    # On any failure retain this batch; never poll past failed work.
                    offsets = commit_offsets(pending)
                    for offset in offsets:
                        durable_offsets[(offset.topic, offset.partition)] = offset
                    with COMMIT.time():
                        consumer.commit(offsets=offsets, asynchronous=(commit_mode == "async"))
                    BATCHES.observe(len(pending))
                    record_results(results)
                    pending = []
                    failures = 0
                except Exception:
                    ERRORS.inc()
                    failures += 1
                    LOGGER.exception("batch_failed retry=%s messages=%s", failures, len(pending))
                    if failures >= 5:
                        raise RuntimeError("Batch failed repeatedly; restart without advancing offsets")
                    time.sleep(min(0.1 * 2 ** min(failures, 5), 2.0))
        finally:
            if commit_mode == "async" and durable_offsets:
                try:
                    consumer.commit(offsets=list(durable_offsets.values()), asynchronous=False)
                except KafkaException:
                    LOGGER.warning("final_offset_commit_failed; durable rows will be replayed")
            consumer.close()
            LOGGER.info("consumer_stopped")


if __name__ == "__main__":
    main()
