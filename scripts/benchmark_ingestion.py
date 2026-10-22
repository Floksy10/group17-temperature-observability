"""Deterministic PostgreSQL ingestion trial; does not publish to course Kafka.

Each run owns a fresh schema in a dedicated benchmark database. Reports DB
throughput, not Kafka/notification delivery latency. Replays verify idempotency.
"""
import argparse
import json
import os
import resource
import sys
import time
from pathlib import Path
from types import SimpleNamespace
import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

sys.path.insert(0, str(Path(__file__).parents[1] / 'services' / 'consumer'))
from storage import process_event
from batching import process_batch


def events(count, sensors, samples):
    sensor_ids = [f's{i}' for i in range(sensors)]
    for e in range(count):
        yield 'experiment_configured', dict(experiment=f'e{e}', researcher='bench@example.com', sensors=sensor_ids,
            temperature_range=dict(lower_threshold=25.5, upper_threshold=26.5))
        yield 'stabilization_started', dict(experiment=f'e{e}')
    for sample in range(samples + 1):
        for e in range(count):
            if sample == 1:
                yield 'experiment_started', dict(experiment=f'e{e}')
            # Stabilize at sample 0, then alternate normal/outside readings.
            temperature = 26. if sample % 2 == 0 else 28.
            for sensor in sensor_ids:
                yield 'sensor_temperature_measured', dict(experiment=f'e{e}', sensor=sensor,
                    measurement_id=f'm{sample}', timestamp=1700000000. + sample,
                    temperature=temperature, measurement_hash=f'hash-{e}-{sample}')
    for e in range(count):
        yield 'experiment_terminated', dict(experiment=f'e{e}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--compact', action='store_true')
    parser.add_argument('--implementation', choices=['baseline', 'pipeline'], default='baseline')
    parser.add_argument('--batch-size', type=int, default=1)
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--sensors', type=int, default=4)
    parser.add_argument('--samples', type=int, default=20)
    parser.add_argument('--schema', required=True)
    args = parser.parse_args()
    global process_event
    if args.implementation == 'pipeline':
        from storage_pipeline import process_event
    dsn = os.environ['DATABASE_URL']
    # Explicitly require a separate database, never the production database.
    if psycopg.conninfo.conninfo_to_dict(dsn).get('dbname') != 'final_bench':
        raise RuntimeError('This test may only use database final_bench')
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(args.schema)))
        connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(args.schema)))
        connection.execute(Path('database/schema.sql').read_text())
    records = list(events(args.count, args.sensors, args.samples))
    batches = [records[i:i + args.batch_size] for i in range(0, len(records), args.batch_size)]
    options = '-csearch_path=' + args.schema
    with ConnectionPool(dsn, min_size=1, max_size=1, kwargs={'options': options}) as pool:
        pool.wait()
        started = time.perf_counter()
        for batch in batches:
            if args.batch_size == 1:
                with pool.connection() as connection:
                    process_event(connection, *batch[0])
            else:
                process_batch(batch, pool, lambda row: [row], process_event, compact=args.compact)
        elapsed = time.perf_counter() - started
        with pool.connection() as connection:
            before = connection.execute('SELECT count(*) FROM measurements').fetchone()[0]
            actual = connection.execute('SELECT count(*), count(*) FILTER (WHERE during_experiment), sum(temperature) FROM measurements').fetchone()
            notifications = connection.execute('SELECT notification_type, count(*) FROM notification_outbox GROUP BY 1 ORDER BY 1').fetchall()
            pending = connection.execute('SELECT count(*) FROM pending_readings').fetchone()[0]
            terminated = connection.execute('SELECT count(*) FROM experiments WHERE terminated').fetchone()[0]
        assert actual[0] == args.count * (args.samples + 1), actual
        assert actual[1] == args.count * args.samples, actual
        expected_sum = args.count * sum(26. if i % 2 == 0 else 28. for i in range(args.samples + 1))
        assert actual[2] == expected_sum, actual
        assert dict(notifications) == {'OutOfRange': args.count * ((args.samples + 1) // 2), 'Stabilized': args.count}, notifications
        assert pending == 0 and terminated == args.count
        # Replay events as after a crash between DB commit and Kafka commit.
        for batch in batches:
            process_batch(batch, pool, lambda row: [row], process_event, compact=args.compact)
        with pool.connection() as connection:
            assert connection.execute('SELECT count(*) FROM measurements').fetchone()[0] == before
            assert connection.execute('SELECT count(*) FROM notification_outbox').fetchone()[0] == sum(v for _, v in notifications)
        # Deliberate mid-transaction failure: first valid insert must roll back.
        injected = [('experiment_configured', dict(experiment='rollback-test', researcher='x', sensors=['s'],
                     temperature_range={'lower_threshold': 0., 'upper_threshold': 1.})),
                    ('experiment_started', {'experiment': 'does-not-exist'})]
        try:
            process_batch(injected, pool, lambda row: [row], process_event)
        except ValueError:
            pass
        else:
            raise AssertionError('Expected injected failure')
        with pool.connection() as connection:
            assert connection.execute("SELECT count(*) FROM experiments WHERE experiment_id = 'rollback-test'").fetchone()[0] == 0
    print(json.dumps(dict(implementation=args.implementation, compact=args.compact, batch_size=args.batch_size, events=len(records), transactions=len(batches),
        seconds=elapsed, events_per_second=len(records) / elapsed, measurements=actual[1],
        notifications=dict(notifications), pending_readings=pending, replay_and_rollback='passed',
        max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)))


if __name__ == '__main__':
    main()
