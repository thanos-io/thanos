#!/usr/bin/env bash

. setup.sh --source-only
. demo-lib.sh

clear

# We assume ./setup.sh was successfully ran.

# Double Esc or Ctrl+Q to close it.
function open() {
    # Image viewer on fullscreen.
    eog -wgf "$@"
}

rc "open slides/1-title.svg" # Slide to 4) init setup.
r "kubectl --context=eu1 get po"
r "kubectl --context=us1 get po"

# Need ctrl+w to close and fullscreen beforehand.
ro "open \$(minikube -p eu1 service prometheus --url)/graph" "google-chrome --app=\"`minikube -p eu1 service prometheus --url`/graph?g0.range_input=1d&g0.expr=container_memory_usage_bytes&g0.tab=0\" > /dev/null"
ro "open \$(minikube -p us1 service prometheus --url)/graph" "google-chrome --app=\"`minikube -p us1 service prometheus --url`/graph?g0.range_input=1d&g0.expr=container_memory_usage_bytes&g0.tab=0\" > /dev/null"
ro "open \$(minikube -p eu1 service alertmanager --url)" "google-chrome --app=`minikube -p eu1 service alertmanager --url` > /dev/null"
ro "open \$(minikube -p eu1 service grafana --url)" "google-chrome --app=\"`minikube -p eu1 service grafana --url`/d/pods_memory/pods-memory?orgId=1\" > /dev/null"

# problems: global view

# problems HA - add naive HA
r "applyPersistentVolumeWithGeneratedMetrics eu1 1"
r "less manifests/prometheus.yaml"
r "colordiff -y manifests/prometheus.yaml manifests/prometheus-ha.yaml | less"
ro "kubectl --context=eu1 apply -f manifests/prometheus-ha.yaml" "cat manifests/prometheus-ha.yaml | sed \"s#%%ALERTMANAGER_URL%%#`minikube -p eu1 service alertmanager --format=\"{{.IP}}:{{.Port}}\"`#g\" | sed \"s#%%CLUSTER%%#eu1#g\" | kubectl --context=eu1 apply -f -"
r "kubectl --context=eu1 get po"
ro "open \$(minikube -p eu1 service prometheus-1 --url)/graph" "google-chrome --app=\"\$(minikube -p eu1 service prometheus-1 --url)/graph?g0.range_input=1d&g0.expr=container_memory_usage_bytes&g0.tab=0\" > /dev/null"


#r "colordiff -y manifests/grafana-datasources.yaml manifests/grafana-datasources-ha.yaml | less"
#ro "kubectl --context=eu1 apply -f manifests/grafana-datasources-ha.yaml" "sed \"s#%%PROM_US1_URL%%#$(minikube -p us1 service prometheus --url)#g\" manifests/grafana-datasources-ha.yaml | kubectl apply -f -"
#r "kubectl --context=eu1 delete po \$(kubectl --context=eu1 get po -l app=grafana -o jsonpath={.items..metadata.name})"
#r "kubectl --context=us1 get po"
#ro "open \$(minikube -p eu1 service grafana --url)" "google-chrome --app=\"`minikube -p eu1 service grafana --url`/d/pods_memory/pods-memory?orgId=1\" > /dev/null"

# FIRST STEP. Sidecar.


rc "open slides/10-the-end.svg"

navigate