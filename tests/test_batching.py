"""Acknowledgement and rollback boundaries for ingestion batches."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parents[1] / 'services' / 'consumer'))
from batching import commit_offsets, process_batch


def message(partition, offset):
    item = MagicMock()
    item.topic.return_value = 'events'
    item.partition.return_value = partition
    item.offset.return_value = offset
    return item


class BatchTest(unittest.TestCase):
    def test_commits_only_highest_processed_offset_per_partition(self):
        offsets = commit_offsets([message(0, 4), message(1, 8), message(0, 5)])
        self.assertEqual([(v.topic, v.partition, v.offset) for v in offsets],
                         [('events', 0, 6), ('events', 1, 9)])

    def test_failed_record_exits_transaction_with_exception(self):
        pool = MagicMock()
        connection = pool.connection.return_value.__enter__.return_value
        failed = RuntimeError('DB unavailable')
        def process(conn, name, record):
            raise failed
        with self.assertRaisesRegex(RuntimeError, 'DB unavailable'):
            process_batch([message(0, 1)], pool,
                          lambda m: [('event', {'experiment': 'e'})], process)
        self.assertIs(pool.connection.return_value.__exit__.call_args.args[1], failed)

    def test_decode_failure_never_opens_transaction(self):
        pool = MagicMock()
        def decode(m):
            raise ValueError('invalid avro')
        with self.assertRaises(ValueError):
            process_batch([message(0, 1)], pool, decode, MagicMock())
        pool.connection.assert_not_called()

    def test_locks_in_canonical_order_and_preserves_event_order(self):
        pool = MagicMock()
        rows = [('first', {'experiment': 'z'}), ('second', {'experiment': 'a'})]
        observed = []
        process_batch([message(0, 1)], pool, lambda m: rows,
                      lambda conn, name, record: observed.append(name))
        connection = pool.connection.return_value.__enter__.return_value
        self.assertEqual(connection.execute.call_args.args[1], (['a', 'z'],))
        self.assertEqual(observed, ['first', 'second'])


class CompactionTest(unittest.TestCase):
    def test_groups_sensors_but_keeps_lifecycle_boundaries(self):
        from batching import compact_readings
        def reading(sensor, measurement='m1'):
            return 'sensor_temperature_measured', dict(experiment='e', measurement_id=measurement, sensor=sensor, temperature=26.)
        rows = [reading('a'), ('experiment_started', {'experiment': 'e'}), reading('b'), reading('a', 'm2'), reading('b', 'm2')]
        result = compact_readings(rows)
        self.assertEqual([name for name, _ in result], ['sensor_temperature_measured', 'experiment_started', 'sensor_temperature_measured', 'sensor_temperature_measured'])
        self.assertEqual(result[-1][1]['_sensor_readings'], [('a',26.),('b',26.)])
        self.assertEqual(result[-1][1]['_record_count'], 2)

    def test_interleaved_experiments_keep_individual_order(self):
        from batching import compact_readings
        rows = [
            ('experiment_configured', {'experiment':'a'}),
            ('experiment_configured', {'experiment':'b'}),
            ('sensor_temperature_measured', dict(experiment='a',measurement_id='m',sensor='s1',temperature=26.)),
            ('sensor_temperature_measured', dict(experiment='b',measurement_id='m',sensor='s1',temperature=27.)),
            ('sensor_temperature_measured', dict(experiment='a',measurement_id='m',sensor='s2',temperature=28.)),
        ]
        grouped = compact_readings(rows)
        self.assertEqual([r['experiment'] for _,r in grouped], ['a','a','b','b'])
        self.assertEqual(grouped[1][1]['_sensor_readings'], [('s1',26.),('s2',28.)])


if __name__ == '__main__':
    unittest.main()
