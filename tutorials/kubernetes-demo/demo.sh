#!/usr/bin/env bash

. demo-lib.sh

clear

# We assume ./setup.sh was successfully ran.

# Double Esc or Ctrl+Q to close it.
function open() {
    # Image viewer on fullscreen.
    eog -wgf "$@"
}


rc "open slides/1-title.svg"
r "ls -l"
# Need ctrl+w to close and fullscreen beforehand.
rc "google-chrome --app=`minikube -p eu1 service prometheus --url` # Prometheus eu1"
r "kubectl --context=us1 -n=kube-system get po"
rc "open slides/10-the-end.svg"

navigate