#!/usr/bin/env bash

docker pull minio/minio:RELEASE.2019-01-31T00-31-19Z
docker pull quay.io/prometheus/prometheus:v2.20.0
docker pull quay.io/thanos/thanos:v0.16.0
docker pull quay.io/thanos/thanosbench:v0.1.0

mkdir /root/editor
