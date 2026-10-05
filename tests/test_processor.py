import unittest

from services.consumer.processor import ExperimentProcessor


class ExperimentProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.processor = ExperimentProcessor()
        self.processor.configure(
            {
                "experiment": "experiment-1",
                "researcher": "researcher@example.com",
                "sensors": ["sensor-1", "sensor-2"],
                "temperature_range": {
                    "lower_threshold": 25.5,
                    "upper_threshold": 26.5,
                },
            }
        )

    def measurement(self, sensor: str, temperature: float) -> dict:
        return {
            "experiment": "experiment-1",
            "sensor": sensor,
            "measurement_id": "measurement-1",
            "timestamp": 123.0,
            "temperature": temperature,
            "measurement_hash": "cipher-data",
        }

    def test_waits_for_all_sensors_and_calculates_average(self) -> None:
        first_result = self.processor.add_measurement(
            self.measurement("sensor-1", 26.0)
        )
        second_result = self.processor.add_measurement(
            self.measurement("sensor-2", 27.0)
        )

        self.assertIsNone(first_result)
        self.assertIsNotNone(second_result)
        self.assertEqual(second_result.average_temperature, 26.5)
        self.assertFalse(second_result.out_of_range)
        self.assertFalse(second_result.experiment_started)

    def test_marks_measurements_after_experiment_start(self) -> None:
        self.processor.start({"experiment": "experiment-1"})
        self.processor.add_measurement(self.measurement("sensor-1", 27.0))
        result = self.processor.add_measurement(
            self.measurement("sensor-2", 28.0)
        )

        self.assertTrue(result.experiment_started)
        self.assertTrue(result.out_of_range)

    def test_ignores_completed_measurement_duplicate(self) -> None:
        self.processor.add_measurement(self.measurement("sensor-1", 26.0))
        self.processor.add_measurement(self.measurement("sensor-2", 26.0))

        duplicate = self.processor.add_measurement(
            self.measurement("sensor-1", 26.0)
        )

        self.assertIsNone(duplicate)


if __name__ == "__main__":
    unittest.main()
