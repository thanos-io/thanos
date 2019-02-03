#!/usr/bin/env bash

set -e

minikube -p us1 stop
minikube -p us1 delete

minikube -p eu1 stop
minikube -p eu1 delete
