#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${DIR}/envs.sh

cd ${DIR}
echo "Starting Thanos service for gathering all thanos gossip peers."
kubectl apply -f manifests/thanos

echo "Starting Prometheus pod with sidecar."
kubectl apply -f manifests/prometheus

echo "Starting Thanos query pod targeting sidecar."
kubectl apply -f manifests/thanos-query