"""Repoint ONLY explicitly named disposable API trials to final_bench."""
import json
import subprocess
from pathlib import Path


def main():
    db_env = Path('/tmp/group17-final-db.env').read_text().strip()
    assert db_env.endswith('/final_bench')
    network = Path('/tmp/group17-trial-network').read_text().strip()
    for label, port, image in [('baseline', '13003', 'baseline-final'), ('candidate', '13004', 'final-v1')]:
        name = 'final-api-' + label
        subprocess.run(['docker', 'stop', name], check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['docker', 'rm', name], check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['docker', 'run', '-d', '--name', name, '--network', network,
            '--env-file', '/tmp/group17-final-db.env', '--memory', '384m',
            '-e', 'API_DB_POOL_SIZE=4', '-e', 'API_CACHE_BYTES=33554432',
            '-p', '127.0.0.1:' + port + ':3003', '--read-only', '--tmpfs', '/tmp:size=16m',
            'group17/temperature-api:' + image], check=True)


if __name__ == '__main__':
    main()
