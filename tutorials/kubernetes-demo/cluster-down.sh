#!/usr/bin/env bash

set -e
export HISTFILE="/home/bartek/.zsh_history_demo_thanos_2019"

minikube -p us1 stop
minikube -p us1 delete

minikube -p eu1 stop
minikube -p eu1 delete
