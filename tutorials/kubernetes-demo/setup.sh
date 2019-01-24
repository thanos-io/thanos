#!/usr/bin/env bash

export HISTFILE="/home/bartek/.zsh_history_demo_thanos_2019"

minikube start --cache-images --vm-driver=kvm2 -p us1 --kubernetes-version="v1.13.2" \
    --memory=4096 \
    --extra-config=kubelet.authentication-token-webhook=true \
    --extra-config=kubelet.authorization-mode=Webhook \
    --extra-config=scheduler.address=0.0.0.0 \
    --extra-config=controller-manager.address=0.0.0.0

minikube start --cache-images --vm-driver=kvm2 -p eu1 --kubernetes-version="v1.13.2" \
    --memory=4096 \
    --extra-config=kubelet.authentication-token-webhook=true \
    --extra-config=kubelet.authorization-mode=Webhook \
    --extra-config=scheduler.address=0.0.0.0 \
    --extra-config=controller-manager.address=0.0.0.0

kubectl config use-context eu1



