"""Move Prometheus while preserving its existing TSDB, with rollback."""
import subprocess
import time
import urllib.request


def kube(*args):
    return subprocess.check_output(['sudo','k3s','kubectl','-n','temperature',*args])


def main():
    old = 'group17-temperature-observability-prometheus-1'
    subprocess.run(['docker','stop','--timeout','30',old],check=True)
    try:
        kube('scale','deployment/prometheus','--replicas=1')
        kube('rollout','status','deployment/prometheus','--timeout=90s')
        kube('patch','service','prometheus','--type=json','-p','[{"op":"replace","path":"/spec/ports/0/nodePort","value":3008}]')
        for attempt in range(20):
            try:
                with urllib.request.urlopen('http://10.0.1.183:3008/-/healthy',timeout=2) as response:
                    assert response.status == 200
                break
            except Exception:
                time.sleep(.5)
        else:
            raise RuntimeError('Prometheus did not become publicly reachable')
    except Exception:
        kube('patch','service','prometheus','--type=json','-p','[{"op":"replace","path":"/spec/ports/0/nodePort","value":13008}]')
        kube('scale','deployment/prometheus','--replicas=0')
        kube('wait','--for=delete','pod','-l','app=prometheus','--timeout=60s')
        subprocess.run(['docker','start',old],check=True)
        raise
    subprocess.run(['docker','stop','group17-temperature-observability-node-exporter-1'],check=True)
    print('Prometheus on Kubernetes :3008, existing history preserved')


if __name__ == '__main__':
    main()
