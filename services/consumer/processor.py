from dataclasses import dataclass, field


@dataclass
class ExperimentState:
    researcher: str
    sensors: set[str]
    lower_threshold: float
    upper_threshold: float
    started: bool = False
    terminated: bool = False
    measurements: dict[str, dict[str, float]] = field(default_factory=dict)
    processed_measurements: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class AggregatedMeasurement:
    experiment: str
    measurement_id: str
    timestamp: float
    average_temperature: float
    measurement_hash: str
    experiment_started: bool
    out_of_range: bool


class ExperimentProcessor:
    def __init__(self) -> None:
        self.experiments: dict[str, ExperimentState] = {}

    def configure(self, record: dict) -> None:
        temperature_range = record["temperature_range"]

        self.experiments[record["experiment"]] = ExperimentState(
            researcher=record["researcher"],
            sensors=set(record["sensors"]),
            lower_threshold=float(temperature_range["lower_threshold"]),
            upper_threshold=float(temperature_range["upper_threshold"]),
        )

    def start(self, record: dict) -> None:
        experiment_id = record["experiment"]
        state = self.experiments.get(experiment_id)
        if state is None:
            raise KeyError(f"Unknown experiment: {experiment_id}")

        state.started = True

    def terminate(self, record: dict) -> None:
        experiment_id = record["experiment"]
        state = self.experiments.get(experiment_id)
        if state is None:
            raise KeyError(f"Unknown experiment: {experiment_id}")

        state.terminated = True

    def add_measurement(self, record: dict) -> AggregatedMeasurement | None:
        experiment_id = record["experiment"]
        state = self.experiments.get(experiment_id)
        if state is None:
            raise KeyError(f"Unknown experiment: {experiment_id}")

        sensor_id = record["sensor"]
        if sensor_id not in state.sensors:
            raise ValueError(
                f"Unexpected sensor {sensor_id} for experiment {experiment_id}"
            )

        measurement_id = record["measurement_id"]
        if measurement_id in state.processed_measurements:
            return None

        temperatures = state.measurements.setdefault(measurement_id, {})
        temperatures[sensor_id] = float(record["temperature"])

        if not state.sensors.issubset(temperatures):
            return None

        average_temperature = sum(
            temperatures[sensor] for sensor in state.sensors
        ) / len(state.sensors)

        del state.measurements[measurement_id]
        state.processed_measurements.add(measurement_id)

        return AggregatedMeasurement(
            experiment=experiment_id,
            measurement_id=measurement_id,
            timestamp=float(record["timestamp"]),
            average_temperature=average_temperature,
            measurement_hash=record["measurement_hash"],
            experiment_started=state.started,
            out_of_range=not (
                state.lower_threshold
                <= average_temperature
                <= state.upper_threshold
            ),
        )
