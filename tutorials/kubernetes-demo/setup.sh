#!/usr/bin/env bash

set -e

MINIKUBE_RESTART=${1:true}

# Prepare setup with:
#
# eu1:
# * Grafana
# * Alertmanager
# * 1 replica Prometheus with 2w metrics data.
#
# us1:
# * 1 replica Prometheus with 2w metrics data.

if ${MINIKUBE_RESTART}; then
    ./cluster-down.sh
    ./cluster-up.sh
fi

kubectl --context=eu1 apply -f manifests/alertmanager.yaml

sleep 2s

ALERTMANAGER_URL=$(minikube -p eu1 service alertmanager --format="{{.IP}}:{{.Port}}")
if [[ -z "${ALERTMANAGER_URL}" ]]; then
    echo "minikube returns empty result for ALERTMANAGER_URL"
    exit 1
fi

./apply-pv-gen-metrics.sh eu1 0 336h
kubectl --context=eu1 apply -f manifests/prometheus-rules.yaml
cat manifests/prometheus.yaml | sed "s#%%ALERTMANAGER_URL%%#${ALERTMANAGER_URL}#g" | sed "s#%%CLUSTER%%#eu1#g" | kubectl --context=eu1 apply -f -
kubectl --context=eu1 apply -f manifests/kube-state-metrics.yaml

./apply-pv-gen-metrics.sh us1 0 336h
kubectl --context=us1 apply -f manifests/prometheus-rules.yaml
cat manifests/prometheus.yaml | sed "s#%%ALERTMANAGER_URL%%#${ALERTMANAGER_URL}#g" | sed "s#%%CLUSTER%%#us1#g" | kubectl --context=us1 apply -f -
kubectl --context=us1 apply -f manifests/kube-state-metrics.yaml

sleep 1s

PROM_US1_URL=$(minikube -p us1 service prometheus --url)
echo "PROM_US1_URL=${PROM_US1_URL}"
sed "s#%%PROM_US1_URL%%#${PROM_US1_URL}#g" manifests/grafana-datasources.yaml | kubectl --context=eu1 apply -f -
kubectl apply --context=eu1 -f manifests/grafana.yaml
