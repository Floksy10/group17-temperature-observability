"""Render Kubernetes objects. No credentials are included in this file."""
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--active", action="store_true")
parser.add_argument("--consumer-replicas", type=int, default=2)
args = parser.parse_args()

NS = 'temperature'
objects = []


def add(kind, name, spec=None, **extra):
    api = {'Deployment': 'apps/v1', 'HorizontalPodAutoscaler': 'autoscaling/v2',
           'Role': 'rbac.authorization.k8s.io/v1', 'RoleBinding': 'rbac.authorization.k8s.io/v1',
           'NetworkPolicy': 'networking.k8s.io/v1', 'EndpointSlice': 'discovery.k8s.io/v1', 'PodDisruptionBudget': 'policy/v1'}.get(kind, 'v1')
    item = dict(apiVersion=api, kind=kind, metadata=dict(name=name, namespace=NS))
    if spec is not None:
        item['spec'] = spec
    item.update(extra)
    objects.append(item)


def deployment(name, image, replicas, port=None, args=None):
    env = [dict(name='DATABASE_URL', valueFrom=dict(secretKeyRef=dict(name=name + '-database', key='DATABASE_URL')))]
    volumes = [dict(name='tmp', emptyDir=dict(medium='Memory', sizeLimit='16Mi'))]
    mounts = [dict(name='tmp', mountPath='/tmp')]
    container = dict(name=name, image=image, imagePullPolicy='IfNotPresent', env=env,
        envFrom=[dict(configMapRef=dict(name='runtime'))], volumeMounts=mounts,
        securityContext=dict(runAsNonRoot=True, runAsUser=10001, allowPrivilegeEscalation=False,
                             readOnlyRootFilesystem=True, capabilities=dict(drop=['ALL'])),
        resources=dict(requests=dict(cpu='100m', memory='64Mi'), limits=dict(cpu='1000m', memory='256Mi')))
    if args:
        container['args'] = args
    if port:
        container['ports'] = [dict(name='http', containerPort=port)]
    if name == 'api':
        container['resources']['requests']['cpu'] = '250m'
        container['env'] += [dict(name=k, value=v) for k, v in {
            'API_DB_POOL_SIZE': '4', 'API_CACHE_BYTES': '33554432',
            'API_CACHE_MAX_POINTS': '4096', 'API_CACHE_EXPERIMENTS': '1024',
            'PROMETHEUS_URL': 'http://prometheus:9090'}.items()]
        container['lifecycle'] = dict(preStop=dict(exec=dict(command=['python', '-c', 'import time; time.sleep(10)'])))
        container['readinessProbe'] = dict(httpGet=dict(path='/ready', port=3003), periodSeconds=5, timeoutSeconds=2)
        container['livenessProbe'] = dict(tcpSocket=dict(port=3003), periodSeconds=10)
        container['startupProbe'] = dict(httpGet=dict(path='/ready', port=3003), periodSeconds=2, failureThreshold=30)
    if name in ('consumer', 'notifier'):
        volumes.append(dict(name='auth', secret=dict(secretName='kafka-auth' if name == 'consumer' else 'notification-auth', defaultMode=292)))
        mounts.append(dict(name='auth', mountPath='/app/auth', readOnly=True))
        if name == 'consumer':
            container['env'] += [dict(name='KAFKA_AUTH_DIR', value='/app/auth'),
                dict(name='KAFKA_BATCH_SIZE', value='64'), dict(name='KAFKA_BATCH_WAIT_SECONDS', value='0.02'),
                dict(name='METRICS_PORT', value='8001')]
            container['readinessProbe'] = dict(httpGet=dict(path='/', port=8001), initialDelaySeconds=5)
        else:
            container['env'].append(dict(name='NOTIFICATIONS_TOKEN_FILE', value='/app/auth/token'))
            container['readinessProbe'] = dict(exec=dict(command=['/bin/sh', '-c', 'test -f /tmp/notifier-ready']), periodSeconds=2, timeoutSeconds=2)
            container['resources']['requests']['cpu'] = '25m'
            container['resources']['limits']['memory'] = '128Mi'
            container['resources']['limits']['cpu'] = '250m'
    annotations = {'prometheus.io/scrape': 'true', 'prometheus.io/port': str(port)} if port else {}
    add('Deployment', name, dict(replicas=replicas, revisionHistoryLimit=3,
        strategy=dict(type='RollingUpdate', rollingUpdate=dict(maxSurge=1, maxUnavailable=0)),
        selector=dict(matchLabels=dict(app=name)), template=dict(metadata=dict(labels=dict(app=name), annotations=annotations),
        spec=dict(automountServiceAccountToken=False, terminationGracePeriodSeconds=45,
                  containers=[container], volumes=volumes))))


add('Namespace', NS)
objects[-1]['metadata'].pop('namespace')
add('Service', 'database', dict(ports=[dict(port=5432, targetPort=15432)]))
add('EndpointSlice', 'database', addressType='IPv4',
    ports=[dict(name=None, protocol='TCP', port=15432)],
    endpoints=[dict(addresses=['10.42.0.1'], conditions=dict(ready=True))])
objects[-1]['metadata']['labels'] = {'kubernetes.io/service-name': 'database', 'endpointslice.kubernetes.io/managed-by': 'group17'}
deployment('api', 'group17/temperature-api:final-v2', 2, 3003,
    ['uvicorn', 'app:app', '--host', '0.0.0.0', '--port', '3003', '--workers', '1', '--loop', 'uvloop', '--http', 'httptools', '--no-access-log'])
# Shadow installation starts no Kafka readers/notifiers. Activation is explicit.
deployment('consumer', 'group17/temperature-consumer:final-v4', args.consumer_replicas if args.active else 0, 8001)
deployment('notifier', 'group17/temperature-notifier:final-v3', 2 if args.active else 0)
add('Service', 'api', dict(type='NodePort', selector=dict(app='gateway'),
    ports=[dict(name='http', port=3003, targetPort=8080, nodePort=3003 if args.active else 13005)]))
add('Service', 'api-backend', dict(clusterIP='None', selector=dict(app='api'),
    ports=[dict(name='http', port=3003, targetPort=3003)]))
add('ConfigMap', 'gateway-config', data={'nginx.conf': """worker_processes 1;
pid /tmp/nginx.pid;
error_log /dev/stderr warn;
events { worker_connections 2048; }
http {
  access_log off;
  client_body_temp_path /tmp/client_body;
  proxy_temp_path /tmp/proxy;
  fastcgi_temp_path /tmp/fastcgi;
  uwsgi_temp_path /tmp/uwsgi;
  scgi_temp_path /tmp/scgi;
  resolver 10.43.0.10 valid=2s ipv6=off;
  upstream temperature_api {
    zone temperature_api 64k;
    server api-backend.temperature.svc.cluster.local:3003 resolve max_fails=0;
    keepalive 32;
  }
  server {
    listen 8080;
    location / {
      proxy_pass http://temperature_api;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_set_header Host $host;
      proxy_connect_timeout 1s;
      proxy_read_timeout 5s;
      proxy_next_upstream error timeout http_502 http_503 http_504;
      proxy_next_upstream_tries 3;
      proxy_next_upstream_timeout 3s;
      proxy_buffering on;
    }
  }
}
"""})
add('Deployment', 'gateway', dict(replicas=1, selector=dict(matchLabels=dict(app='gateway')),
    template=dict(metadata=dict(labels=dict(app='gateway')), spec=dict(automountServiceAccountToken=False,
        securityContext=dict(fsGroup=10001), containers=[dict(name='gateway', image='nginx:1.28-alpine', command=['sh','-c',"ulimit -n 8192; exec nginx -g 'daemon off;'"],
            ports=[dict(containerPort=8080)], securityContext=dict(runAsUser=10001, runAsNonRoot=True,
            readOnlyRootFilesystem=True, allowPrivilegeEscalation=False, capabilities=dict(drop=['ALL'])),
            resources=dict(requests=dict(cpu='100m',memory='16Mi'),limits=dict(cpu='500m',memory='64Mi')),
            readinessProbe=dict(httpGet=dict(path='/ready',port=8080),initialDelaySeconds=3,timeoutSeconds=2),
            volumeMounts=[dict(name='config',mountPath='/etc/nginx/nginx.conf',subPath='nginx.conf',readOnly=True),dict(name='tmp',mountPath='/tmp')])],
        volumes=[dict(name='config',configMap=dict(name='gateway-config')),dict(name='tmp',emptyDir=dict(medium='Memory',sizeLimit='16Mi'))]))))
add('HorizontalPodAutoscaler', 'api', dict(scaleTargetRef=dict(apiVersion='apps/v1', kind='Deployment', name='api'),
    minReplicas=2, maxReplicas=4, metrics=[dict(type='Resource', resource=dict(name='cpu', target=dict(type='Utilization', averageUtilization=75)))],
    behavior=dict(scaleDown=dict(stabilizationWindowSeconds=180), scaleUp=dict(stabilizationWindowSeconds=30))))
add('PodDisruptionBudget', 'api', dict(minAvailable=1, selector=dict(matchLabels=dict(app='api'))))
add('NetworkPolicy', 'application-ingress', dict(podSelector=dict(matchLabels={}), policyTypes=['Ingress'],
    ingress=[{'from': [dict(podSelector={})]}, dict(ports=[dict(protocol='TCP', port=p) for p in [3003, 8080, 9090]])]))
add('ServiceAccount', 'prometheus')
add('Role', 'prometheus', rules=[dict(apiGroups=[''], resources=['pods'], verbs=['get', 'list', 'watch'])])
add('RoleBinding', 'prometheus', roleRef=dict(apiGroup='rbac.authorization.k8s.io', kind='Role', name='prometheus'),
    subjects=[dict(kind='ServiceAccount', name='prometheus', namespace=NS)])
add('ConfigMap', 'prometheus-config', data={'prometheus.yml': '''global:
  scrape_interval: 5s
scrape_configs:
  - job_name: node
    static_configs:
      - targets: ["node-exporter:9100"]
  - job_name: application
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [temperature]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: 'true'
      - source_labels: [__meta_kubernetes_pod_phase]
        action: keep
        regex: Running
      - source_labels: [__meta_kubernetes_pod_label_app]
        target_label: job
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
'''})
add('Deployment', 'prometheus', dict(replicas=1 if args.active else 0, strategy=dict(type='Recreate'), selector=dict(matchLabels=dict(app='prometheus')),
    template=dict(metadata=dict(labels=dict(app='prometheus')), spec=dict(serviceAccountName='prometheus',
        nodeSelector={'kubernetes.io/hostname': 'group17'}, securityContext=dict(runAsUser=65534, runAsGroup=65534),
        containers=[dict(name='prometheus', image='prom/prometheus:v3.2.1',
            args=['--config.file=/etc/prometheus/prometheus.yml', '--storage.tsdb.path=/prometheus', '--storage.tsdb.retention.time=7d', '--storage.tsdb.retention.size=512MB'],
            ports=[dict(containerPort=9090)],
            resources=dict(requests=dict(cpu='50m', memory='96Mi'), limits=dict(memory='384Mi', cpu='500m')),
            readinessProbe=dict(httpGet=dict(path='/-/ready', port=9090)),
            volumeMounts=[dict(name='config', mountPath='/etc/prometheus', readOnly=True), dict(name='data', mountPath='/prometheus')])],
        volumes=[dict(name='config', configMap=dict(name='prometheus-config')),
                 dict(name='data', hostPath=dict(path='/var/lib/docker/volumes/group17-temperature-observability_prometheus_data/_data', type='Directory'))]))))
add('Service', 'prometheus', dict(type='NodePort', selector=dict(app='prometheus'), ports=[dict(port=9090, targetPort=9090, nodePort=3008 if args.active else 13008)]))
add('Deployment', 'node-exporter', dict(replicas=1, selector=dict(matchLabels=dict(app='node-exporter')),
    template=dict(metadata=dict(labels=dict(app='node-exporter')), spec=dict(automountServiceAccountToken=False,
        nodeSelector={'kubernetes.io/hostname': 'group17'}, containers=[dict(name='node-exporter', image='prom/node-exporter:v1.9.0',
            args=['--path.rootfs=/host'], ports=[dict(containerPort=9100)],
            resources=dict(requests=dict(cpu='10m', memory='16Mi'), limits=dict(cpu='100m', memory='64Mi')),
            volumeMounts=[dict(name='root', mountPath='/host', readOnly=True, mountPropagation='HostToContainer')])],
        volumes=[dict(name='root', hostPath=dict(path='/', type='Directory'))]))))
add('Service', 'node-exporter', dict(selector=dict(app='node-exporter'), ports=[dict(port=9100, targetPort=9100)]))
print(json.dumps(dict(apiVersion='v1', kind='List', items=objects), indent=2))
