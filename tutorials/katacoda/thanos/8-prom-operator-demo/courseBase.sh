#!/usr/bin/env bash

mkdir -p /root/manifests/crds
mkdir -p /root/manifests/prometheus
mkdir -p /root/manifests/operator
mkdir -p /root/manifests/ruler
mkdir -p /root/manifests/query
mkdir -p /root/manifests/svcmonitors
mkdir -p /root/manifests/receiver

mkdir -p /root/manifests/us1
mkdir -p /root/manifests/storeapius1

launch.sh
