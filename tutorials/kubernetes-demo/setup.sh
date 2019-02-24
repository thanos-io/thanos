#!/usr/bin/env bash

function applyPersistentVolumeWithGeneratedMetrics() {
    cluster=$1
    replica=$2
    retention=$3

    # Add volume and generated metrics inside it.
    kubectl apply --context=${cluster} -f manifests/prometheus-pv-${replica}.yaml

    rm -rf -- /tmp/prom-out
    mkdir /tmp/prom-out
    go run ./blockgen/main.go --input=./blockgen/container_mem_metrics_${cluster}.json --output-dir=/tmp/prom-out --extra-labels="cluster=${cluster}" --retention=${retention}
    chmod -R 775 /tmp/prom-out
    # Fun with permissions because Prometheus process is run a "noone" in a pod... ):
    minikube -p ${cluster} ssh "sudo rm -rf /data/pv-prometheus-${replica} && sudo mkdir /data/pv-prometheus-${replica} && sudo chmod -R 777 /data/pv-prometheus-${replica}"
    scp -r -i $(minikube -p ${cluster} ssh-key) /tmp/prom-out/* docker@$(minikube -p ${cluster} ip):/data/pv-prometheus-${replica}/
    minikube -p ${cluster} ssh "sudo chmod -R 777 /data/pv-prometheus-${replica}"
}

if [[ "${1}" == "--source-only" ]]; then
    return
fi

set -e

MINIKUBE_RESTART=${1:true}

if ${MINIKUBE_RESTART}; then
    ./cluster-up.sh
fi

kubectl config use-context eu1
kubectl apply -f manifests/alertmanager.yaml
sleep 2s

ALERTMANAGER_URL=$(minikube -p eu1 service alertmanager --format="{{.IP}}:{{.Port}}")
echo "ALERTMANAGER_URL=${ALERTMANAGER_URL}"
if [[ -z "${ALERTMANAGER_URL}" ]]; then
    echo "minikube returns empty result for ALERTMANAGER_URL"
    exit 1
fi

applyPersistentVolumeWithGeneratedMetrics eu1 0 336h
kubectl apply -f manifests/prometheus-rules.yaml
cat manifests/prometheus.yaml | sed "s#%%ALERTMANAGER_URL%%#${ALERTMANAGER_URL}#g" | sed "s#%%CLUSTER%%#eu1#g" | kubectl apply -f -
kubectl apply -f manifests/kube-state-metrics.yaml

kubectl config use-context us1
applyPersistentVolumeWithGeneratedMetrics us1 0 336h
kubectl apply -f manifests/prometheus-rules.yaml
cat manifests/prometheus.yaml | sed "s#%%ALERTMANAGER_URL%%#${ALERTMANAGER_URL}#g" | sed "s#%%CLUSTER%%#us1#g" | kubectl apply -f -
kubectl apply -f manifests/kube-state-metrics.yaml

sleep 1s

kubectl config use-context eu1
PROM_US1_URL=$(minikube -p us1 service prometheus --url)
echo "PROM_US1_URL=${PROM_US1_URL}"
sed "s#%%PROM_US1_URL%%#${PROM_US1_URL}#g" manifests/grafana-datasources.yaml | kubectl apply -f -
kubectl apply -f manifests/grafana.yaml

# Issues.
# - not synced time on minikube?
# - loadbalancer type not possible. It works with minikube tunnel, but it works only for single cluster.
#  NodePort needs to be used.
# - mounting config at prometheus.yml does not work - default one overwrites.
# - kubectl exec -it prometheus-0 -- /bin/kill -SIGHUP 1
# kube sched and controller metrics - descoping: # Hacks to make kube-scheduler and kube-controller-manager metrics available:
#---
#apiVersion: v1
#kind: Service
#metadata:
#  namespace: kube-system
#  name: kube-scheduler-monitoring
#  labels:
#    app: kube-scheduler
#spec:
#  selector:
#    component: kube-scheduler
#  type: ClusterIP
#  clusterIP: None
#  ports:
#    - name: http
#      port: 10251
#      targetPort: 10251
#      protocol: TCP
#---
#apiVersion: v1
#kind: Service
#metadata:
#  namespace: kube-system
#  name: kube-controller-manager-monitoring
#  labels:
#    app: kube-controller-manager
#spec:
#  selector:
#    app: kube-controller-manager
#  type: ClusterIP
#  clusterIP: None
#  ports:
#    - name: http
#      port: 10252
#      targetPort: 10252
#      protocol: TCP
# - alertmanager URL service is not ready?
# -  E0129 12:41:52.352178    6295 start.go:243] Error parsing version semver:  Version string empty
#