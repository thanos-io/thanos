#!/usr/bin/env bash

export HISTFILE="/home/bartek/.zsh_history_demo_thanos_2019"

#minikube start --cache-images --vm-driver=kvm2 -p leaf1 -v 99999 --kubernetes-version="v1.13.2"
#minikube start --cache-images --vm-driver=kvm2 -p leaf1 -v 99999 --kubernetes-version="v1.13.2"
#minikube start --cache-images --vm-driver=kvm2 -p leaf1 -v 99999 --kubernetes-version="v1.13.2"


kubectl config use-context leaf1