#!/usr/bin/env bash

docker pull quay.io/prometheus/prometheus:main # TODO(bwplotka): Lock to the release with agent.
docker pull quay.io/thanos/thanos:v0.21.0

mkdir /root/editor
