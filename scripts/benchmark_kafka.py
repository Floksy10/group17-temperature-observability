"""Isolated real-Kafka comparison on group17, using final_bench and local mock.

Never writes to experiment and never calls the real notification service.
Consumers use separate groups and schemas. Producer sends one shared stream.
"""
import argparse
import json
import subprocess
import time
import uuid
from pathlib import Path


def call(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def sql(statement):
    return subprocess.check_output(['docker', 'exec', '-i', 'group17-temperature-observability-database-1',
        'psql', '-U', 'temperature', '-d', 'final_bench', '-At', '-v', 'ON_ERROR_STOP=1'], input=statement.encode()).decode().strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=40)
    parser.add_argument('--candidate-only', action='store_true')
    parser.add_argument('--consumers', type=int, default=2)
    parser.add_argument('--commit-mode', choices=['sync','async'], default='sync')
    parser.add_argument('--candidate-image', default='final-v1')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    run_id = uuid.uuid4().hex[:8]
    network = Path('/tmp/group17-trial-network').read_text().strip()
    dsn = Path('/tmp/group17-final-db.env').read_text().strip().split('=', 1)[1]
    assert dsn.endswith('/final_bench')
    Path('/tmp/group17-dummy-token').write_text('development-only')
    names = []
    mock = 'final-mock-' + run_id
    call(['docker', 'run', '-d', '--name', mock, '--network', network,
        '-v', str(root / 'services/mock_notifications/mock.py') + ':/mock.py:ro',
        '--entrypoint', 'python', 'python:3.12-slim', '/mock.py'], stdout=subprocess.DEVNULL)
    names.append(mock)
    schemas = {}
    try:
        variants = [('candidate', args.candidate_image)] if args.candidate_only else [('baseline', 'baseline-final'), ('candidate', args.candidate_image)]
        for variant, image in variants:
            schema = 'kafka_' + variant + '_' + run_id
            schemas[variant] = schema
            sql('CREATE SCHEMA ' + schema + '; SET search_path TO ' + schema + ';\n' + (root / 'database/schema.sql').read_text())
            variant_dsn = dsn + '?options=-csearch_path%3D' + schema
            for i in range(args.consumers):
                name = f'final-{variant}-{run_id}-{i}'
                call(['docker', 'run', '-d', '--name', name, '--network', network,
                    '-e', 'DATABASE_URL=' + variant_dsn, '-e', 'KAFKA_TOPIC=group17',
                    '-e', 'KAFKA_GROUP_ID=group17-final-' + variant + '-' + run_id,
                    '-e', 'KAFKA_BATCH_SIZE=64', '-e', 'KAFKA_COMMIT_MODE=' + args.commit_mode, '-e', 'LOG_LEVEL=WARNING',
                    '-e', 'KAFKA_AUTH_DIR=/app/auth',
                    '-v', '/home/ubuntu/group17-auth:/app/auth:ro',
                    'group17/temperature-consumer:' + image], stdout=subprocess.DEVNULL)
                names.append(name)
            name = f'final-notifier-{variant}-{run_id}'
            call(['docker', 'run', '-d', '--name', name, '--network', network,
                '-e', 'DATABASE_URL=' + variant_dsn, '-e', 'NOTIFICATIONS_URL=http://' + mock + ':3000/api/notify',
                '-e', 'NOTIFICATIONS_TOKEN_FILE=/dummy-token', '-v', '/tmp/group17-dummy-token:/dummy-token:ro',
                'group17/temperature-notifier:final-v3'], stdout=subprocess.DEVNULL)
            names.append(name)
        time.sleep(8)
        config = subprocess.check_output(['python3', str(root / 'scripts/generate_experiments.py'),
            '--count', str(args.count), '--sensors', '4', '--samples', '20'])
        config_path = Path('/tmp/group17-kafka-trial-' + run_id + '.json')
        config_path.write_bytes(config)
        producer = 'final-producer-' + run_id
        call(['docker', 'run', '-d', '--name', producer, '--network', network,
            '-v', '/home/ubuntu/group17-auth:/experiment-producer/auth:ro',
            '-v', str(config_path) + ':/experiment-producer/load.json:ro',
            'dclandau/cec-experiment-producer', '--topic', 'group17',
            '--brokers', 'kafka.cec.dlandau.nl:19092', '--config-file', '/experiment-producer/load.json'], stdout=subprocess.DEVNULL)
        names.append(producer)
        started = time.monotonic()
        finished = {}
        while time.monotonic() - started < 150:
            for variant, schema in schemas.items():
                value = sql(f'SET search_path TO {schema}; SELECT json_build_object(\'terminated\',(SELECT count(*) FROM experiments WHERE terminated),\'measurements\',(SELECT count(*) FROM measurements WHERE during_experiment),\'pending\',(SELECT count(*) FROM notification_outbox WHERE sent_at IS NULL));')
                state = json.loads(value.splitlines()[-1])
                if state['terminated'] == args.count and state['measurements'] == args.count * 20 and state['pending'] == 0:
                    finished.setdefault(variant, round(time.monotonic() - started, 3))
            if len(finished) == len(variants):
                break
            time.sleep(2)
        assert len(finished) == len(variants), finished
        summaries = {}
        snapshots = {}
        for variant, schema in schemas.items():
            value = sql(f'''SET search_path TO {schema};
SELECT json_build_object(
 'measurements',(SELECT count(*) FROM measurements WHERE during_experiment),
 'pending_readings',(SELECT count(*) FROM pending_readings),
 'notifications',(SELECT count(*) FROM notification_outbox),
 'notification_p95_seconds',(SELECT percentile_cont(.95) WITHIN GROUP (ORDER BY extract(epoch FROM (n.sent_at-to_timestamp(m.measured_at)))) FROM notification_outbox n JOIN measurements m USING(experiment_id,measurement_id)),
 'notification_max_seconds',(SELECT max(extract(epoch FROM (n.sent_at-to_timestamp(m.measured_at)))) FROM notification_outbox n JOIN measurements m USING(experiment_id,measurement_id)));
''')
            summaries[variant] = json.loads(value.splitlines()[-1])
            snapshots[variant] = sql(f'''SET search_path TO {schema};
SELECT md5(string_agg(row(experiment_id,measurement_id,measured_at,temperature,out_of_range,during_experiment)::text,',' ORDER BY experiment_id,measurement_id)) FROM measurements;
SELECT md5(string_agg(row(experiment_id,measurement_id,notification_type,researcher,cipher_data)::text,',' ORDER BY experiment_id,measurement_id,notification_type)) FROM notification_outbox;''')
        if not args.candidate_only:
            assert snapshots['baseline'] == snapshots['candidate'], snapshots
        assert all(v['pending_readings'] == 0 for v in summaries.values())
        result = dict(run_id=run_id, experiments=args.count, sensors=4, samples=20, consumers_per_variant=args.consumers,
            candidate_image=args.candidate_image, commit_mode=args.commit_mode, finished_seconds=finished, results=summaries, exact_data_and_outbox_match=(True if not args.candidate_only else None), all_histories_complete=True,
            note='Notification latency includes local mock delivery; does not prove external-service latency.')
        path = root / 'docs/final-evidence' / ('kafka-' + run_id + '.json')
        path.write_text(json.dumps(result, indent=2))
        print(json.dumps(result))
    finally:
        for name in names:
            subprocess.run(['docker', 'stop', '--timeout', '5', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
