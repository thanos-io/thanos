#!/usr/bin/env bash

docker pull quay.io/prometheus/prometheus:v2.20.0
docker pull quay.io/thanos/thanos:v0.22.0
docker pull quay.io/thanos/thanosbench:v0.2.0-rc.1
docker pull minio/minio:RELEASE.2019-01-31T00-31-19Z

mkdir /root/editor
