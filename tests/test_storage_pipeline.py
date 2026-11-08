"""Consumer event handling against a real PostgreSQL database.

Set TEST_DATABASE_URL to run, for example:
  docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine
  TEST_DATABASE_URL=postgresql://postgres:test@localhost:55432/postgres \
      python -m unittest tests/test_storage_pipeline.py
Each test uses its own schema, which is dropped afterwards.
"""
import os
import sys
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / 'services' / 'consumer'))

DATABASE_URL = os.getenv('TEST_DATABASE_URL')
if DATABASE_URL:
    import psycopg
    from storage_pipeline import process_event

SENSORS = ['s1', 's2']


@unittest.skipUnless(DATABASE_URL, 'TEST_DATABASE_URL is not set')
class StoragePipelineTest(unittest.TestCase):
    def setUp(self):
        self.schema = 'test_' + uuid.uuid4().hex[:12]
        self.connection = psycopg.connect(DATABASE_URL, autocommit=True)
        self.connection.execute(f'CREATE SCHEMA {self.schema}')
        self.connection.execute(f'SET search_path TO {self.schema}')
        self.connection.execute((ROOT / 'database' / 'schema.sql').read_text())
        self.connection.autocommit = False

    def tearDown(self):
        self.connection.rollback()
        self.connection.autocommit = True
        self.connection.execute(f'DROP SCHEMA {self.schema} CASCADE')
        self.connection.close()

    def event(self, name, **record):
        record.setdefault('experiment', 'e1')
        with self.connection.transaction():
            return process_event(self.connection, name, record)

    def configure(self, lower=25.0, upper=26.0):
        self.event('experiment_configured', researcher='r@example.com', sensors=SENSORS,
                   temperature_range=dict(lower_threshold=lower, upper_threshold=upper))
        self.event('stabilization_started', timestamp=0.0)

    def measure(self, measurement, temperatures, timestamp=1.0):
        result = None
        for sensor, temperature in zip(SENSORS, temperatures):
            result = self.event('sensor_temperature_measured', sensor=sensor,
                                measurement_id=measurement, temperature=temperature,
                                timestamp=timestamp, measurement_hash='hash-' + measurement)
        return result

    def outbox(self):
        return self.connection.execute(
            'SELECT notification_type, measurement_id, researcher, cipher_data '
            'FROM notification_outbox ORDER BY id').fetchall()

    def history(self):
        return self.connection.execute(
            'SELECT measurement_id, temperature, out_of_range FROM measurements '
            'WHERE during_experiment ORDER BY measured_at').fetchall()

    def test_average_is_stored_only_when_every_sensor_reported(self):
        self.configure()
        first = self.event('sensor_temperature_measured', sensor='s1', measurement_id='m1',
                           temperature=25.3, timestamp=1.0, measurement_hash='h')
        self.assertIsNone(first)
        result = self.event('sensor_temperature_measured', sensor='s2', measurement_id='m1',
                            temperature=25.5, timestamp=1.0, measurement_hash='h')
        self.assertAlmostEqual(result.temperature, 25.4)
        pending = self.connection.execute('SELECT count(*) FROM pending_readings').fetchone()[0]
        self.assertEqual(pending, 0)

    def test_stabilized_is_notified_once_and_stabilization_is_not_history(self):
        self.configure()
        self.measure('m1', [27.0, 27.0], 1.0)
        self.measure('m2', [25.5, 25.5], 2.0)
        self.measure('m3', [25.6, 25.6], 3.0)
        self.assertEqual(self.outbox(), [('Stabilized', 'm2', 'r@example.com', 'hash-m2')])
        self.assertEqual(self.history(), [])

    def test_out_of_range_is_notified_only_when_leaving_the_range(self):
        self.configure()
        self.measure('m0', [25.5, 25.5], 0.5)
        self.event('experiment_started', timestamp=1.0)
        self.measure('m1', [25.5, 25.5], 1.0)
        self.measure('m2', [27.0, 27.0], 2.0)
        self.measure('m3', [27.5, 27.5], 3.0)
        self.measure('m4', [25.5, 25.5], 4.0)
        self.measure('m5', [24.0, 24.0], 5.0)
        notified = [(kind, measurement) for kind, measurement, _, _ in self.outbox()]
        self.assertEqual(notified, [('Stabilized', 'm0'), ('OutOfRange', 'm2'), ('OutOfRange', 'm5')])
        self.assertEqual([row[0] for row in self.history() if row[2]], ['m2', 'm3', 'm5'])

    def test_thresholds_are_inclusive(self):
        self.configure()
        self.event('experiment_started', timestamp=0.0)
        self.measure('m1', [26.0, 26.0], 1.0)
        self.measure('m2', [25.0, 25.0], 2.0)
        self.assertEqual(self.outbox(), [])
        self.assertFalse(any(row[2] for row in self.history()))

    def test_first_reading_after_start_can_be_out_of_range(self):
        self.configure()
        self.event('experiment_started', timestamp=0.0)
        self.measure('m1', [30.0, 30.0], 1.0)
        self.assertEqual([row[0] for row in self.outbox()], ['OutOfRange'])

    def test_replayed_readings_do_not_duplicate_history_or_notifications(self):
        self.configure()
        self.event('experiment_started', timestamp=0.0)
        self.measure('m1', [30.0, 30.0], 1.0)
        self.assertIsNone(self.measure('m1', [30.0, 30.0], 1.0))
        self.event('experiment_configured', researcher='other@example.com', sensors=['x'],
                   temperature_range=dict(lower_threshold=0.0, upper_threshold=1.0))
        self.assertEqual(len(self.history()), 1)
        self.assertEqual(len(self.outbox()), 1)
        researcher = self.connection.execute('SELECT researcher FROM experiments').fetchone()[0]
        self.assertEqual(researcher, 'r@example.com')

    def test_compacted_readings_match_individual_readings(self):
        self.configure()
        self.event('experiment_started', timestamp=0.0)
        result = self.event('sensor_temperature_measured', sensor='s2', measurement_id='m1',
                            temperature=25.5, timestamp=1.0, measurement_hash='h',
                            _sensor_readings=[('s1', 25.3), ('s2', 25.5)])
        self.assertAlmostEqual(result.temperature, 25.4)
        self.assertEqual(len(self.history()), 1)

    def test_measurements_after_termination_are_not_history(self):
        self.configure()
        self.event('experiment_started', timestamp=0.0)
        self.measure('m1', [25.5, 25.5], 1.0)
        self.event('experiment_terminated', timestamp=2.0)
        self.measure('m2', [30.0, 30.0], 3.0)
        self.assertEqual([row[0] for row in self.history()], ['m1'])

    def test_unknown_experiment_and_sensor_are_rejected(self):
        with self.assertRaises(ValueError):
            self.measure('m1', [25.5, 25.5])
        self.configure()
        with self.assertRaises(ValueError):
            self.event('sensor_temperature_measured', sensor='unknown', measurement_id='m1',
                       temperature=25.0, timestamp=1.0, measurement_hash='h')


if __name__ == '__main__':
    unittest.main()
