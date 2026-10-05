"""Consume and decode experiment events from the course Kafka cluster."""

from __future__ import annotations

import io
import json
import logging
import os
import signal
from pathlib import Path

from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from processor import ExperimentProcessor


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("group17-consumer")

AUTH_DIR = Path(os.getenv("KAFKA_AUTH_DIR", "/app/auth"))
PROPERTIES_FILE = AUTH_DIR / "client-ssl.properties"
RUNNING = True
PROCESSOR = ExperimentProcessor()


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
        "auto.offset.reset": os.getenv("KAFKA_OFFSET_RESET", "latest"),
        "enable.auto.commit": False,
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


def process_message(message: Message) -> None:
    name = record_name(message)
    records = decode_records(message.value())
    for record in records:
        LOGGER.info(
            "event=%s partition=%s offset=%s data=%s",
            name,
            message.partition(),
            message.offset(),
            json.dumps(record, sort_keys=True),
        )

        if name == "experiment_configured":
            PROCESSOR.configure(record)
        elif name == "experiment_started":
            PROCESSOR.start(record)
        elif name == "sensor_temperature_measured":
            measurement = PROCESSOR.add_measurement(record)
            if measurement is not None:
                LOGGER.info(
                    "measurement_aggregated experiment=%s measurement_id=%s "
                    "average_temperature=%.3f out_of_range=%s started=%s",
                    measurement.experiment,
                    measurement.measurement_id,
                    measurement.average_temperature,
                    measurement.out_of_range,
                    measurement.experiment_started,
                )
        elif name == "experiment_terminated":
            PROCESSOR.terminate(record)


def stop(_signum: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    topic = os.getenv("KAFKA_TOPIC", "group17")
    consumer = Consumer(kafka_configuration())
    consumer.subscribe([topic])
    LOGGER.info("consumer_started topic=%s", topic)

    try:
        while RUNNING:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                process_message(message)
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                LOGGER.exception(
                    "message_processing_failed partition=%s offset=%s",
                    message.partition(),
                    message.offset(),
                )
    finally:
        consumer.close()
        LOGGER.info("consumer_stopped")


if __name__ == "__main__":
    main()
