"""Durable experiment state and temperature aggregation in PostgreSQL."""

from dataclasses import dataclass

from psycopg import Connection
from psycopg.rows import dict_row


@dataclass(frozen=True)
class MeasurementResult:
    experiment_id: str
    measurement_id: str
    temperature: float
    during_experiment: bool
    out_of_range: bool
    notification_type: str | None


def process_event(
    connection: Connection, name: str, record: dict
) -> MeasurementResult | None:
    """Process one Kafka record in the caller's database transaction."""
    experiment_id = record["experiment"]
    with connection.cursor(row_factory=dict_row) as cursor:
        if name == "experiment_configured":
            temperature_range = record["temperature_range"]
            sensors = list(record["sensors"])
            if not sensors or len(sensors) != len(set(sensors)):
                raise ValueError("An experiment needs distinct sensors")
            cursor.execute(
                """
                INSERT INTO experiments (
                    experiment_id, researcher, sensors,
                    lower_threshold, upper_threshold
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (experiment_id) DO NOTHING
                """,
                (
                    experiment_id,
                    record["researcher"],
                    sensors,
                    float(temperature_range["lower_threshold"]),
                    float(temperature_range["upper_threshold"]),
                ),
            )
            return None

        cursor.execute(
            "SELECT * FROM experiments WHERE experiment_id = %s FOR UPDATE",
            (experiment_id,),
        )
        experiment = cursor.fetchone()
        if experiment is None:
            raise ValueError(f"Unknown experiment {experiment_id}")

        if name == "stabilization_started":
            cursor.execute(
                "UPDATE experiments SET stabilization_started = TRUE "
                "WHERE experiment_id = %s",
                (experiment_id,),
            )
            return None

        if name == "experiment_started":
            cursor.execute(
                "UPDATE experiments SET started = TRUE "
                "WHERE experiment_id = %s",
                (experiment_id,),
            )
            return None

        if name == "experiment_terminated":
            cursor.execute(
                "UPDATE experiments SET terminated = TRUE "
                "WHERE experiment_id = %s",
                (experiment_id,),
            )
            return None

        if name != "sensor_temperature_measured":
            raise ValueError(f"Unknown Kafka event type {name}")

        sensor_id = record["sensor"]
        if sensor_id not in experiment["sensors"]:
            raise ValueError(f"Unexpected sensor {sensor_id} in {experiment_id}")

        measurement_id = record["measurement_id"]
        cursor.execute(
            "SELECT 1 FROM measurements "
            "WHERE experiment_id = %s AND measurement_id = %s",
            (experiment_id, measurement_id),
        )
        if cursor.fetchone() is not None:
            return None

        cursor.execute(
            """
            INSERT INTO pending_readings (
                experiment_id, measurement_id, sensor_id, temperature
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                experiment_id,
                measurement_id,
                sensor_id,
                float(record["temperature"]),
            ),
        )
        cursor.execute(
            """
            SELECT count(*) AS sensor_count, avg(temperature) AS average
            FROM pending_readings
            WHERE experiment_id = %s AND measurement_id = %s
            """,
            (experiment_id, measurement_id),
        )
        aggregate = cursor.fetchone()
        if aggregate["sensor_count"] < len(experiment["sensors"]):
            return None

        temperature = float(aggregate["average"])
        out_of_range = not (
            experiment["lower_threshold"]
            <= temperature
            <= experiment["upper_threshold"]
        )
        during_experiment = experiment["started"] and not experiment["terminated"]
        notification_type = None

        if (
            experiment["stabilization_started"]
            and not experiment["started"]
            and not experiment["stabilized_notified"]
            and not out_of_range
        ):
            notification_type = "Stabilized"
            cursor.execute(
                "UPDATE experiments SET stabilized_notified = TRUE "
                "WHERE experiment_id = %s",
                (experiment_id,),
            )

        if during_experiment:
            if out_of_range and not experiment["last_out_of_range"]:
                notification_type = "OutOfRange"
            cursor.execute(
                "UPDATE experiments SET last_out_of_range = %s "
                "WHERE experiment_id = %s",
                (out_of_range, experiment_id),
            )

        cursor.execute(
            """
            INSERT INTO measurements (
                experiment_id, measurement_id, measured_at, temperature,
                out_of_range, during_experiment
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                experiment_id,
                measurement_id,
                float(record["timestamp"]),
                temperature,
                out_of_range,
                during_experiment,
            ),
        )
        cursor.execute(
            "DELETE FROM pending_readings "
            "WHERE experiment_id = %s AND measurement_id = %s",
            (experiment_id, measurement_id),
        )

        if notification_type:
            cursor.execute(
                """
                INSERT INTO notification_outbox (
                    experiment_id, measurement_id, notification_type,
                    researcher, cipher_data
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    experiment_id,
                    measurement_id,
                    notification_type,
                    experiment["researcher"],
                    record["measurement_hash"],
                ),
            )

        return MeasurementResult(
            experiment_id,
            measurement_id,
            temperature,
            during_experiment,
            out_of_range,
            notification_type,
        )
