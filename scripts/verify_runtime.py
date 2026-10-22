"""Read-only post-deployment audit; never emits Kubernetes Secret contents."""
import json
import argparse
import re
import math
import subprocess
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DB = 'group17-temperature-observability-database-1'
def command(*args):
    return subprocess.check_output(args).decode()
def sql(statement):
    return command('docker','exec',DB,'psql','-qAt','-U','temperature','-d','temperature','-c',statement).strip()
def kube(*args):
    return json.loads(command('sudo','k3s','kubectl','-n','temperature',*args,'-o','json'))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='/tmp/group17-runtime.json',help='Snapshot path; defaults outside the Git checkout')
    args=parser.parse_args()
    report = {'observed_at_utc': datetime.now(timezone.utc).isoformat()}
    report['nodes']=[{'name': v['metadata']['name'],'ready': any(c['type']=='Ready' and c['status']=='True' for c in v['status']['conditions'])} for v in kube('get','nodes')['items']]
    report['pods']=[{'name':p['metadata']['name'],'phase':p['status']['phase'],'ready':all(c.get('ready') for c in p['status'].get('containerStatuses',[])),'restarts':sum(c['restartCount'] for c in p['status'].get('containerStatuses',[]))} for p in kube('get','pods')['items']]
    report['outbox']=json.loads(sql("SELECT json_build_object('active_pending',count(*) FILTER (WHERE sent_at IS NULL AND next_attempt_at < 'infinity'),'quarantined_unsent',count(*) FILTER (WHERE sent_at IS NULL AND next_attempt_at = 'infinity')) FROM notification_outbox"))
    report['database_tcp_roles']=json.loads(sql("SELECT coalesce(json_agg(r),'[]') FROM (SELECT usename,count(*) AS connections FROM pg_stat_activity WHERE datname='temperature' AND client_addr IS NOT NULL GROUP BY usename) r"))
    report['hba_errors']=int(sql("SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL"))
    report['admin_tcp_rejected']=sql("SELECT count(*) FROM pg_hba_file_rules WHERE type='host' AND database=ARRAY['temperature'] AND user_name=ARRAY['temperature'] AND auth_method='reject'")=='1'
    report['raw_source_timestamp_lag']=json.loads(sql("SELECT json_build_object('window_minutes',2,'delivered',count(*),'p95_seconds',percentile_cont(.95) WITHIN GROUP (ORDER BY extract(epoch FROM n.sent_at)-m.measured_at),'max_seconds',max(extract(epoch FROM n.sent_at)-m.measured_at)) FROM notification_outbox n JOIN measurements m USING (experiment_id,measurement_id) WHERE n.sent_at >= now()-interval '2 minutes'"))
    report['raw_source_timestamp_lag']['note']='Raw source timestamps differ from official returned latency; not the course latency metric.'
    logs=command('sudo','k3s','kubectl','-n','temperature','logs','-l','app=notifier','--tail=1000','--since=2m')
    latencies=[float(v) for v in re.findall(r'notification_sent[^\n]*response=([0-9.]+)',logs)]
    latencies=[v for v in latencies if math.isfinite(v) and v >= 0]
    report['official_notification_latency']={'source':'numeric responses from official service in last two minutes of current Pod logs','count':len(latencies),'max_seconds':max(latencies) if latencies else None}
    report['application_resources']=command('sudo','k3s','kubectl','-n','temperature','top','pods')
    for name,url in [('api_health','http://127.0.0.1:3003/health'),('api_readiness','http://127.0.0.1:3003/ready'),('prometheus_targets','http://127.0.0.1:3008/api/v1/targets')]:
        with urllib.request.urlopen(url,timeout=10) as response:
            data=json.load(response)
        report[name]=([{'job': t['labels'].get('job'),'health':t['health'],'error':t['lastError']} for t in data['data']['activeTargets']] if name=='prometheus_targets' else data)
    path=Path(args.output)
    path.write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__': main()
