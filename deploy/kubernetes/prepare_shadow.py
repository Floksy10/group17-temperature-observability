"""Install read-only Kubernetes API shadow; ZERO Kafka/notification workers.

Only a newly generated SELECT-only database role goes into the cluster.
No course Kafka credentials or notification token are read or copied here.
"""
import json
import secrets
import subprocess
from pathlib import Path
from urllib.parse import quote


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def apply(item):
    run(['sudo', 'k3s', 'kubectl', 'apply', '-f', '-'], input=json.dumps(item).encode(), stdout=subprocess.DEVNULL)


def main():
    documents = json.loads(Path(__file__).with_name('manifests.json').read_text())
    deployments = {d['metadata']['name']: d for d in documents['items'] if d['kind'] == 'Deployment'}
    assert deployments['consumer']['spec']['replicas'] == 0
    assert deployments['notifier']['spec']['replicas'] == 0
    password = secrets.token_hex(24)
    # Roles are cluster-wide, but permissions are limited to SELECT in temperature.
    statement = "CREATE ROLE group17_api_readonly LOGIN PASSWORD '" + password + "'; GRANT CONNECT ON DATABASE temperature TO group17_api_readonly; GRANT USAGE ON SCHEMA public TO group17_api_readonly; GRANT SELECT ON ALL TABLES IN SCHEMA public TO group17_api_readonly; ALTER ROLE group17_api_readonly SET default_transaction_read_only = on;"
    run(['docker', 'exec', '-i', 'group17-temperature-observability-database-1', 'psql', '-q', '-v', 'ON_ERROR_STOP=1', '-U', 'temperature', '-d', 'temperature'], input=statement.encode(), stdout=subprocess.DEVNULL)
    apply(dict(apiVersion='v1', kind='Namespace', metadata=dict(name='temperature')))
    apply(dict(apiVersion='v1', kind='Secret', metadata=dict(name='api-database', namespace='temperature'), type='Opaque',
        stringData=dict(DATABASE_URL='postgresql://group17_api_readonly:' + quote(password) + '@database:5432/temperature')))
    apply(dict(apiVersion='v1', kind='ConfigMap', metadata=dict(name='runtime', namespace='temperature'), data=dict(LOG_LEVEL='INFO')))
    run(['docker', 'run', '-d', '--name', 'group17-db-cni-relay', '--restart', 'unless-stopped', '--network', 'host',
        '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--memory', '64m',
        'alpine/socat:1.8.0.1', 'TCP4-LISTEN:15432,bind=10.42.0.1,reuseaddr,fork', 'TCP4:127.0.0.1:5432'])
    run(['sudo', 'k3s', 'kubectl', 'apply', '--dry-run=server', '-f', str(Path(__file__).with_name('manifests.json'))])
    run(['sudo', 'k3s', 'kubectl', 'apply', '-f', str(Path(__file__).with_name('manifests.json'))])
    run(['sudo', 'k3s', 'kubectl', '-n', 'temperature', 'rollout', 'status', 'deployment/api', '--timeout=90s'])


if __name__ == '__main__':
    main()
