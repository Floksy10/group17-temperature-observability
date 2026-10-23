"""Measured API placement cutover. Does not change Kafka or notification workers."""
import json
import subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
OLD=Path('/home/ubuntu/group17-final-work/deploy/kubernetes/manifests.json')

def kube(*args):
    return subprocess.check_output(['sudo','k3s','kubectl','-n','temperature',*args])

def apply(document):
    subprocess.run(['sudo','k3s','kubectl','apply','-f','-'],input=json.dumps(document).encode(),check=True)

def main():
    assert ROOT != OLD.parents[2], 'Run from the latency follow-up checkout'
    evidence=ROOT/'docs/grafana-latency-evidence/http'
    baseline=[json.loads((evidence/f'placement-base{i}.summary.json').read_text()) for i in [1,2]]
    candidate=[json.loads((evidence/f'nick-single-{i}.summary.json').read_text()) for i in [1,2]]
    failover=json.loads((evidence/'reserve-failover.summary.json').read_text())
    assert all(v['errors']==0 for v in baseline+candidate+[failover])
    assert failover['valid']==7500
    assert max(v['metrics']['total_ms']['p95'] for v in candidate)<min(v['metrics']['total_ms']['p95'] for v in baseline)
    live_api=json.loads(kube('get','deployment','api','-o','json'))
    assert live_api['spec']['template']['spec'].get('nodeSelector',{}).get('kubernetes.io/hostname') != 'nick-client4', 'Already switched; use routine manifest apply instead'
    nodes=json.loads(kube('get','nodes','-o','json'))
    assert any(n['metadata']['name']=='nick-client4' and any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in nodes['items'])
    document=json.loads((ROOT/'deploy/kubernetes/manifests.json').read_text())
    before=json.loads(OLD.read_text())
    Path('/tmp/group17-before-api-placement.json').write_text(json.dumps(before))
    subprocess.run(['sudo','k3s','kubectl','apply','--dry-run=server','-f','-'],input=json.dumps(document).encode(),check=True)
    # No test clients are running when these temporary resources are removed.
    for kind,names in [('deployment',['api-candidate','api-candidate-group','gateway-candidate']),('service',['api-candidate-backend','api-candidate-group-backend','gateway-candidate']),('configmap',['gateway-candidate-config'])]:
        kube('delete',kind,*names,'--ignore-not-found=true')
    try:
        reserve=[o for o in document['items'] if o['metadata']['name'] in ['api-reserve','api-reserve-backend']]
        apply(dict(apiVersion='v1',kind='List',items=reserve))
        kube('rollout','status','deployment/api-reserve','--timeout=120s')
        # Reset the old minimum-two recommendation history for this new 1+1 layout.
        kube('delete','hpa','api','--ignore-not-found=true')
        apply(document)
        kube('rollout','status','deployment/api','--timeout=120s')
        kube('rollout','status','deployment/gateway','--timeout=120s')
        print(kube('get','pods','-o','wide').decode())
        print('Primary on Nick; reserve on group. Kafka and notifications unchanged.')
    except Exception:
        apply(before)
        kube('rollout','status','deployment/api','--timeout=120s')
        kube('rollout','status','deployment/gateway','--timeout=120s')
        kube('delete','deployment','api-reserve','--ignore-not-found=true')
        kube('delete','service','api-reserve-backend','--ignore-not-found=true')
        raise

if __name__=='__main__':main()
