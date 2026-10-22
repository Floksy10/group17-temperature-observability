"""Cut over only the read-only API, verifying real fixture responses first.

On failure restore the shadow port and the original Docker API. No Kafka,
notification or database-write configuration is changed by this script.
"""
import gzip
import json
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path


def kubectl(*args):
    return subprocess.check_output(['sudo', 'k3s', 'kubectl', '-n', 'temperature', *args])


def port(value):
    return kubectl('patch', 'service', 'api', '--type=json', '-p', json.dumps([
        {'op': 'replace', 'path': '/spec/ports/0/nodePort', 'value': value}]))


def verify(base, fixture):
    query = urllib.parse.urlencode({'experiment-id': fixture['id']})
    expected = [{'timestamp': p['timestamp'], 'temperature': p['temperature']} for p in fixture['points'] if p['outside']]
    with urllib.request.urlopen(base + '/temperature/out-of-range?' + query, timeout=5) as response:
        assert json.load(response) == expected, 'Response differs from fixture'
    with urllib.request.urlopen(base + '/health', timeout=5) as response:
        assert json.load(response) == {'status': 'ok'}


def main():
    root = Path(__file__).resolve().parents[2]
    with gzip.open(root / 'docs/latency-evidence/2026-10-09/fixtures.json.gz', 'rt') as stream:
        fixture = json.load(stream)[0]
    verify('http://10.0.1.183:13005', fixture)
    pod = json.loads(kubectl('get', 'pods', '-l', 'app=api', '-o', 'json'))['items'][0]['metadata']['name']
    check = "import os,psycopg; c=psycopg.connect(os.environ['DATABASE_URL']); assert c.execute(\"SELECT current_setting('transaction_read_only')\").fetchone()[0]=='on'; assert not c.execute(\"SELECT has_table_privilege(current_user,'measurements','INSERT')\").fetchone()[0]; print('SELECT-only role verified')"
    print(kubectl('exec', pod, '--', 'python', '-c', check).decode().strip())
    try:
        port(3003)
        for _ in range(20):
            try:
                verify('http://10.0.1.183:3003', fixture)
                break
            except Exception:
                time.sleep(.5)
        else:
            raise RuntimeError('Public port did not become ready')
        subprocess.run(['docker', 'stop', '--time', '30', 'group17-temperature-observability-api-1'], check=True)
        verify('http://10.0.1.183:3003', fixture)
    except Exception:
        port(13005)
        subprocess.run(['docker', 'start', 'group17-temperature-observability-api-1'], check=True)
        raise
    print('Kubernetes API active on :3003; original Docker API retained stopped for rollback')


if __name__ == '__main__':
    main()
