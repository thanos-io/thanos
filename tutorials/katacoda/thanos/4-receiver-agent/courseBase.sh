#!/usr/bin/env bash

docker pull quay.io/prometheus/prometheus:main # Change to beta.
docker pull quay.io/thanos/thanos:v0.21.0

mkdir /root/editor
