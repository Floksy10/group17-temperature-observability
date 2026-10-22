"""Run on group VM: provision isolated trials, never print credentials."""
import json
import os
import subprocess
from pathlib import Path


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def inspect(name):
    return json.loads(subprocess.check_output(['docker', 'inspect', name]))[0]


def main():
    original = inspect('group17-temperature-observability-api-1')
    env = dict(v.split('=', 1) for v in original['Config']['Env'] if '=' in v)
    network = next(iter(original['NetworkSettings']['Networks']))
    dsn = env['DATABASE_URL']
    run(['docker', 'exec', 'group17-temperature-observability-database-1', 'psql', '-U', 'temperature', '-d', 'temperature', '-v', 'ON_ERROR_STOP=1', '-c', 'CREATE DATABASE final_bench'])
    os.umask(0o077)
    Path('/tmp/group17-final-db.env').write_text('DATABASE_URL=' + dsn.rsplit('/', 1)[0] + '/final_bench\n')
    for label, port, image in [('baseline', '13003', 'baseline-final'), ('candidate', '13004', 'final-v1')]:
        run(['docker', 'run', '-d', '--name', 'final-api-' + label, '--network', network,
             '-e', 'DATABASE_URL=' + dsn, '-e', 'API_CACHE_EXPERIMENTS=1024', '-e', 'API_CACHE_BYTES=33554432',
             '-e', 'API_DB_POOL_SIZE=4', '-p', '127.0.0.1:' + port + ':3003',
             '--read-only', '--tmpfs', '/tmp:size=16m', 'group17/temperature-api:' + image])
    Path('/tmp/group17-trial-network').write_text(network)
    print('Isolated trial database and baseline/candidate APIs ready')


if __name__ == '__main__':
    main()
