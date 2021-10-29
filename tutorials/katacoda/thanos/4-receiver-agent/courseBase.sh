#!/usr/bin/env bash

docker pull quay.io/bwplotka/prometheus:agent1 # TODO(bwplotka): Move to official Prometheus release once available.
docker pull quay.io/thanos/thanos:v0.21.0

mkdir /root/editor
