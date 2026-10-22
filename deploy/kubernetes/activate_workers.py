"""Verified handover: same Kafka group, same database, ZERO worker overlap.

Run only after the isolated real-Kafka comparison passed. Credentials remain
in Kubernetes Secrets and never appear in command-line arguments or output.
If startup fails, scale Kubernetes workers to zero before restarting Docker.
"""
import base64
import json
import secrets
import subprocess
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]


def command(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def kubectl(*args):
    return subprocess.check_output(['sudo', 'k3s', 'kubectl', '-n', 'temperature', *args])


def apply(item):
    command('sudo', 'k3s', 'kubectl', 'apply', '-f', '-', input=json.dumps(item).encode(), stdout=subprocess.DEVNULL)


def sql(statement):
    return subprocess.check_output(['docker', 'exec', '-i', 'group17-temperature-observability-database-1',
        'psql', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'temperature', '-d', 'temperature'], input=statement.encode()).decode().strip()


def main():
    evidence = [json.loads(p.read_text()) for p in (ROOT / 'docs/final-evidence').glob('kafka-*.json')]
    assert any(v['exact_data_and_outbox_match'] and v['candidate_image'] == 'final-v4' for v in evidence), 'Real-Kafka test must pass first'
    assert any(v['candidate_image'] == 'final-v4' and v['experiments'] == 100 and v['consumers_per_variant'] == 2 and v['results']['candidate']['notification_max_seconds'] < 10 for v in evidence), 'Heavy-profile latency must pass first'
    for name in ['consumer', 'notifier']:
        deployment = json.loads(kubectl('get', 'deployment', name, '-o', 'json'))
        assert deployment['spec']['replicas'] == 0, 'Preparation requires zero Kubernetes workers'
    original = json.loads(subprocess.check_output(['docker', 'inspect', 'group17-temperature-observability-consumer-5']))[0]
    env = dict(v.split('=', 1) for v in original['Config']['Env'] if '=' in v)
    assert env['KAFKA_TOPIC'] == 'experiment'
    values = {k: env[k] for k in ['KAFKA_TOPIC', 'KAFKA_GROUP_ID', 'KAFKA_BROKERS'] if k in env}
    values.update(NOTIFICATIONS_URL='https://notifications.cec.dlandau.nl/api/notify', LOG_LEVEL='INFO')
    for app, role, grants in [
        ('consumer', 'group17_ingest', 'GRANT SELECT, INSERT, UPDATE, DELETE ON experiments,pending_readings,measurements,notification_outbox TO group17_ingest; GRANT USAGE ON SEQUENCE notification_outbox_id_seq TO group17_ingest;'),
        ('notifier', 'group17_notifier', 'GRANT SELECT, UPDATE ON notification_outbox TO group17_notifier;'),
    ]:
        password = secrets.token_hex(24)
        sql(f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='{role}') THEN CREATE ROLE {role} LOGIN; END IF; END $$; ALTER ROLE {role} PASSWORD '{password}'; GRANT CONNECT ON DATABASE temperature TO {role}; GRANT USAGE ON SCHEMA public TO {role}; " + grants)
        apply(dict(apiVersion='v1', kind='Secret', metadata=dict(name=app + '-database', namespace='temperature'), type='Opaque',
            stringData=dict(DATABASE_URL=f'postgresql://{role}:' + quote(password) + '@database:5432/temperature')))
    for secret_name, files in [('kafka-auth', ['ca.crt', 'client-ssl.properties', 'kafka.keystore.pkcs12', 'kafka.truststore.pkcs12']), ('notification-auth', ['token'])]:
        apply(dict(apiVersion='v1', kind='Secret', metadata=dict(name=secret_name, namespace='temperature'), type='Opaque',
            data={name: base64.b64encode((Path('/home/ubuntu/group17-auth') / name).read_bytes()).decode() for name in files}))
    apply(dict(apiVersion='v1', kind='ConfigMap', metadata=dict(name='runtime', namespace='temperature'), data=values))
    for app, secret in [('consumer', 'kafka-auth'), ('notifier', 'notification-auth')]:
        kubectl('patch', 'deployment', app, '--type=strategic', '-p', json.dumps({'spec': {'template': {'spec': {'volumes': [{'name': 'auth', 'secret': {'secretName': secret, 'defaultMode': 292}}]}}}}))
    kubectl('set', 'image', 'deployment/consumer', 'consumer=group17/temperature-consumer:final-v4')
    kubectl('set', 'env', 'deployment/consumer', 'KAFKA_BATCH_SIZE=64')
    old_consumers = ['group17-temperature-observability-consumer-' + str(i) for i in [5, 6, 7, 8]]
    old_notifiers = ['group17-temperature-observability-notifier-' + str(i) for i in [1, 2, 3]]
    hardened = False
    try:
        command('docker', 'stop', '--timeout', '45', *old_consumers, stdout=subprocess.DEVNULL)
        for _ in range(60):
            if sql("SELECT count(*) FROM notification_outbox WHERE sent_at IS NULL AND next_attempt_at < 'infinity'") == '0':
                break
            time.sleep(.5)
        else:
            raise RuntimeError('Existing notifications did not drain; abort handover')
        command('docker', 'stop', '--timeout', '15', *old_notifiers, stdout=subprocess.DEVNULL)
        # The user explicitly approved retiring legacy TCP admin connections.
        # Local Unix-socket psql administration remains unchanged.
        db = 'group17-temperature-observability-database-1'
        hba = '/var/lib/postgresql/data/pg_hba.conf'
        backup = '/var/lib/postgresql/data/pg_hba.pre-kubernetes.conf'
        command('docker', 'exec', db, 'sh', '-c', f'test -f {backup} || cp {hba} {backup}')
        original_hba = subprocess.check_output(['docker', 'exec', db, 'cat', backup])
        hardened = True
        command('docker', 'exec', '-i', db, 'sh', '-c', f'cat > {hba}',
                input=b'host temperature temperature all reject\n' + original_hba)
        sql('SELECT pg_reload_conf()')
        sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='temperature' AND usename='temperature' AND client_addr IS NOT NULL")
        hardened = True
        for name in old_consumers + old_notifiers:
            assert subprocess.check_output(['docker', 'inspect', '--format', '{{.State.Running}}', name]).strip() == b'false'
        kubectl('scale', 'deployment/notifier', '--replicas=2')
        kubectl('scale', 'deployment/consumer', '--replicas=2')
        kubectl('rollout', 'status', 'deployment/notifier', '--timeout=90s')
        kubectl('rollout', 'status', 'deployment/consumer', '--timeout=90s')
        time.sleep(8)
        logs = kubectl('logs', '-l', 'app=consumer', '--tail=100').decode()
        if 'batch_failed' in logs:
            raise RuntimeError('New consumer reported batch errors')
    except Exception:
        if hardened:
            command('docker', 'exec', 'group17-temperature-observability-database-1', 'sh', '-c', 'cp /var/lib/postgresql/data/pg_hba.pre-kubernetes.conf /var/lib/postgresql/data/pg_hba.conf')
            sql('SELECT pg_reload_conf()')
        kubectl('scale', 'deployment/consumer', 'deployment/notifier', '--replicas=0')
        kubectl('wait', '--for=delete', 'pod', '-l', 'app=consumer', '--timeout=60s')
        kubectl('wait', '--for=delete', 'pod', '-l', 'app=notifier', '--timeout=60s')
        command('docker', 'start', *old_consumers, *old_notifiers, stdout=subprocess.DEVNULL)
        raise
    print('Kubernetes workers active: 2 consumers + 2 notifiers; previous Docker workers stopped. Kafka group preserved.')


if __name__ == '__main__':
    main()
