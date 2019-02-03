#!/usr/bin/env bash

. setup.sh --source-only
. demo-lib.sh

clear

# We assume ./setup.sh was successfully ran.

# Some yolo aliases (without using `alias`)

# Double Esc or Ctrl+Q to close it.
function open() {
    # Image viewer on fullscreen.
    eog -wgf "$@"
}
function cat() {
    bat -p "$@"
}

MINIO_ACCESS_KEY="smth"
MINIO_SECRET_KEY="Need8Chars"
HOST_IP=

rc "open slides/1-title.svg" # Slide to 2) improbable
r "kubectl --context=eu1 get po"
r "kubectl --context=us1 get po"

# Need ctrl+w to close and fullscreen beforehand!
ro "open \$(minikube -p eu1 service prometheus --url)/graph" "google-chrome --app=\"`minikube -p eu1 service prometheus --url`/graph?g0.range_input=1d&g0.expr=sum(container_memory_usage_bytes)%20by%20(pod%2C%20cluster)&g0.tab=0\" > /dev/null"
ro "open \$(minikube -p eu1 service alertmanager --url)" "google-chrome --app=`minikube -p eu1 service alertmanager --url` > /dev/null"
ro "open \$(minikube -p eu1 service grafana --url)" "google-chrome --app=\"`minikube -p eu1 service grafana --url`/d/pods_memory/pods-memory?orgId=1\" > /dev/null"

# Problems shown in grafana: global view.

# Explain HA.
r "kubectl --context=eu1 get po"
r "kubectl --context=us1 get po"

# Add naive HA.
r "applyPersistentVolumeWithGeneratedMetrics eu1 1 336h"
r "colordiff -y manifests/prometheus.yaml manifests/prometheus-ha.yaml | less -X"
ro "kubectl --context=eu1 apply -f manifests/prometheus-ha.yaml" "cat manifests/prometheus-ha.yaml | sed \"s#%%ALERTMANAGER_URL%%#`minikube -p eu1 service alertmanager --format=\"{{.IP}}:{{.Port}}\"`#g\" | sed \"s#%%CLUSTER%%#eu1#g\" | kubectl --context=eu1 apply -f -"
r "kubectl --context=eu1 get po"
ro "open \$(minikube -p eu1 service prometheus-1 --url)/graph" "google-chrome --app=\"\$(minikube -p eu1 service prometheus-1 --url)/graph?g0.range_input=1d&g0.expr=container_memory_usage_bytes&g0.tab=0\" > /dev/null"
ro "open \$(minikube -p eu1 service prometheus --url)/graph" "google-chrome --app=\"`minikube -p eu1 service prometheus --url`/graph?g0.range_input=3w&g0.expr=sum(container_memory_usage_bytes)%20by%20(pod%2C%20cluster)&g0.tab=0\" > /dev/null"

# Retention problem shown on prom-1 range.

# FIRST STEP: Sidecar.
r "colordiff -y manifests/prometheus-ha.yaml manifests/prometheus-ha-sidecar.yaml | less -X"
ro "kubectl --context=eu1 apply -f manifests/prometheus-ha-sidecar.yaml" "cat manifests/prometheus-ha-sidecar.yaml | sed \"s#%%ALERTMANAGER_URL%%#`minikube -p eu1 service alertmanager --format=\"{{.IP}}:{{.Port}}\"`#g\" | sed \"s#%%CLUSTER%%#eu1#g\" | kubectl --context=eu1 apply -f -"
ro "kubectl --context=us1 apply -f manifests/prometheus-ha-sidecar.yaml" "applyPersistentVolumeWithGeneratedMetrics us1 1 336h && cat manifests/prometheus-ha-sidecar.yaml | sed \"s#%%ALERTMANAGER_URL%%#`minikube -p eu1 service alertmanager --format=\"{{.IP}}:{{.Port}}\"`#g\" | sed \"s#%%CLUSTER%%#us1#g\" | kubectl --context=us1 apply -f -"
r "kubectl --context=eu1 get po"
r "kubectl --context=us1 get po"

# SECOND step: querier
r "cat manifests/thanos-querier.yaml"
ro "kubectl --context=eu1 apply -f manifests/thanos-querier.yaml" "cat manifests/thanos-querier.yaml | sed \"s#%%SIDECAR_US1_0_URL%%#\$(minikube -p us1 service sidecar --format=\"{{.IP}}:{{.Port}}\")#g\" |  sed \"s#%%SIDECAR_US1_1_URL%%#\$(minikube -p us1 service sidecar-1 --format=\"{{.IP}}:{{.Port}}\")#g\" | kubectl --context=eu1 apply -f -"
r "kubectl --context=eu1 get po"
# Show on artifical data!
ro "open \$(minikube -p eu1 service thanos-querier --url)/graph" "google-chrome --app=\"\$(minikube -p eu1 service thanos-querier --url)/graph?g0.range_input=2h&g0.expr=avg(container_memory_usage_bytes)%20by%20(cluster,replica)&g0.tab=0\" > /dev/null"

# THIRD: Connect grafana to querier
r "colordiff -y manifests/grafana-datasources.yaml manifests/grafana-datasources-querier.yaml | less -X"
ro "kubectl --context=eu1 apply -f manifests/grafana-datasources-querier.yaml" "kubectl --context=eu1 apply -f manifests/grafana-datasources-querier.yaml && kubectl --context=eu1 delete po \$(kubectl --context=eu1 get po -l app=grafana -o jsonpath={.items..metadata.name})"
r "kubectl --context=eu1 get po"
ro "open \$(minikube -p eu1 service grafana --url)" "google-chrome --app=\"`minikube -p eu1 service grafana --url`/d/pods_memory/pods-memory?orgId=1\" > /dev/null"

# Show removal of cluster label on grafana - GV and HA done.

# Put yolo local object storage.
r "kubectl --context=eu1 apply -f manifests/minio.yaml"
r "kubectl --context=eu1 get po"
r "mc config host add minio \$(minikube -p eu1 service minio --url) smth Need8Chars --api S3v4 && mc mb minio/demo-bucket"

# FOURTH STEP: Sidecar upload.
r "colordiff -y manifests/prometheus-ha-sidecar.yaml manifests/prometheus-ha-sidecar-lts.yaml | less -X"
ro "kubectl --context=eu1 apply -f manifests/prometheus-ha-sidecar-lts.yaml" "cat manifests/prometheus-ha-sidecar-lts.yaml | sed \"s#%%ALERTMANAGER_URL%%#`minikube -p eu1 service alertmanager --format=\"{{.IP}}:{{.Port}}\"`#g\" | sed \"s#%%CLUSTER%%#eu1#g\" | sed \"s#%%S3_ENDPOINT%%#\$(minikube -p eu1 service minio --format=\"{{.IP}}:{{.Port}}\")#g\" | kubectl --context=eu1 apply -f -"
ro "kubectl --context=us1 apply -f manifests/prometheus-ha-sidecar-lts.yaml" "cat manifests/prometheus-ha-sidecar-lts.yaml | sed \"s#%%ALERTMANAGER_URL%%#`minikube -p eu1 service alertmanager --format=\"{{.IP}}:{{.Port}}\"`#g\" | sed \"s#%%CLUSTER%%#us1#g\" | sed \"s#%%S3_ENDPOINT%%#\$(minikube -p eu1 service minio --format=\"{{.IP}}:{{.Port}}\")#g\" | kubectl --context=us1 apply -f -"
r "kubectl --context=eu1 get po"
r "kubectl --context=us1 get po"

# Show if upload works.
r "mc ls minio/demo-bucket"
r "mc ls minio/demo-bucket/\$(mc ls minio/demo-bucket/ | sed '1q' | cut -d ' ' -f 9)"

# FIFTH: compactor
ro "kubectl --context=eu1 apply -f manifests/thanos-compactor.yaml" "cat manifests/thanos-compactor.yaml | sed \"s#%%S3_ENDPOINT%%#\$(minikube -p eu1 service minio --format=\"{{.IP}}:{{.Port}}\")#g\" | kubectl --context=eu1 apply -f -"
r "kubectl --context=eu1 get po"

# SIXTH: gateway
r "cat manifests/thanos-gateway.yaml"
ro "kubectl --context=eu1 apply -f manifests/thanos-gateway.yaml" "cat manifests/thanos-gateway.yaml | sed \"s#%%S3_ENDPOINT%%#\$(minikube -p eu1 service minio --format=\"{{.IP}}:{{.Port}}\")#g\" | kubectl --context=eu1 apply -f -"
r "kubectl --context=eu1 get po"

# How to make sure we can see store gateway? Simulate outage (no connection?) to us1 cluster!

r "kubectl --context=us1 delete statefulset prometheus"
r "kubectl --context=us1 get po"
# We should see only uploaded data from us1, but no straight connection.
ro "open \$(minikube -p eu1 service thanos-querier --url)/graph" "google-chrome --app=\"\$(minikube -p eu1 service thanos-querier --url)/graph?g0.range_input=2h&g0.expr=avg(container_memory_usage_bytes)%20by%20(cluster,replica)&g0.tab=0\" > /dev/null"

# Show that outage is UNNOTICED!
ro "open \$(minikube -p eu1 service alertmanager --url)" "google-chrome --app=`minikube -p eu1 service alertmanager --url` > /dev/null"

# Step number SEVEN: ruler
r "cat manifests/thanos-ruler.yaml"
ro "kubectl --context=eu1 apply -f manifests/thanos-ruler.yaml" "cat manifests/thanos-ruler.yaml | sed \"s#%%ALERTMANAGER_URL%%#`minikube -p eu1 service alertmanager --format=\"{{.IP}}:{{.Port}}\"`#g\" | sed \"s#%%CLUSTER%%#eu1#g\" | sed \"s#%%S3_ENDPOINT%%#\$(minikube -p eu1 service minio --format=\"{{.IP}}:{{.Port}}\")#g\" | kubectl --context=eu1 apply -f -"
r "kubectl --context=eu1 get po"
ro "open \$(minikube -p eu1 service thanos-ruler --url)/graph" "google-chrome --app=\"\$(minikube -p eu1 service thanos-ruler --url)\" > /dev/null"


# Show outage is at least noticed.
ro "open \$(minikube -p eu1 service alertmanager --url)" "google-chrome --app=`minikube -p eu1 service alertmanager --url` > /dev/null"

# The end.
rc "open slides/7-the-end.svg"

navigate true