#!/usr/bin/env bash

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
ro "open \$(minikube -p eu1 service prometheus --url)/targets" "google-chrome --app=`minikube -p eu1 service prometheus --url`/targets > /dev/null"
ro "open \$(minikube -p us1 service prometheus --url)/targets" "google-chrome --app=`minikube -p us1 service prometheus --url`/targets > /dev/null"
ro "open \$(minikube -p eu1 service alertmanager --url)" "google-chrome --app=`minikube -p eu1 service alertmanager --url` > /dev/null"
ro "open \$(minikube -p eu1 service grafana --url)" "google-chrome --app=`minikube -p eu1 service grafana --url` > /dev/null"
rc "open slides/4-initial-setup.svg"


rc "open slides/10-the-end.svg"

navigate