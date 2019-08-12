#!/usr/bin/env bash

ssh root@docker "docker pull prom/prometheus:latest && docker pull quay.io/thanos/thanos:v0.6.0"
