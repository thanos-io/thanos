#!/usr/bin/env bash

docker pull quay.io/prometheus/prometheus:v2.32.0-beta.0
docker pull quay.io/thanos/thanos:v0.21.0

mkdir /root/editor
