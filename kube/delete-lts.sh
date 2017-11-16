#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${DIR}/envs.sh

cd ${DIR}
kubectl delete -f manifests/thanos
kubectl delete -f manifests/prometheus-gcs
kubectl delete -f manifests/thanos-query
kubectl delete -f manifests/thanos-store