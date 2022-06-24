#!/bin/bash
#
# This is a script for running the Prometheus Alert Generator Compliance
# test suite. Read more at: https://github.com/prometheus/compliance/tree/main/alert_generator
#
# It is the most minimal setup you can use for testing on a local machine.
# The script will start all necessary components (receive, ruler, querier)
# with appropriate confguration.
#
# After all comopnents are running, you can start the alert generator compliance tester
# with `thanos-example.yaml`` configuration provided in here:
# https://github.com/prometheus/compliance/blob/main/alert_generator/test-prometheus.yaml
set -euo pipefail

trap 'kill 0' SIGTERM

THANOS_EXECUTABLE=${THANOS_EXECUTABLE:-"thanos"}

export TMP_DATA=$(mktemp -d /tmp/data-XXXX)
export ALERT_COMPLIANCE_RULES=$(mktemp /tmp/rules-XXXX.yaml)

curl -sNL -o ${ALERT_COMPLIANCE_RULES} "https://raw.githubusercontent.com/prometheus/compliance/main/alert_generator/rules.yaml"

 ${THANOS_EXECUTABLE} receive \
    --label "receive_replica=\"0\"" \
    --tsdb.path=${TMP_DATA} &

# We make sure to filter out the 'receive_replica' and 'tenant_id' labels,
# which are added by the receiver (they cannot be present during the test).
${THANOS_EXECUTABLE} query \
    --http-address 0.0.0.0:19192 \
    --store 0.0.0.0:10901 \
    --rule 0.0.0.0:20901 \
    --grpc-address 0.0.0.0:19099 \
    --query.replica-label="tenant_id" \
    --query.replica-label="receive_replica" &

# Script downloads the compliance test rules into a tmp file that `--rule-file` is pointing to.
${THANOS_EXECUTABLE} rule \
    --rule-file=${ALERT_COMPLIANCE_RULES} \
    --alertmanagers.url="http://0.0.0.0:8080" \
    --query=0.0.0.0:19192 \
    --http-address=0.0.0.0:20902 \
    --grpc-address=0.0.0.0:20901 \
    --data-dir=${TMP_DATA} &

sleep 0.5

printf "\nAll services started, you can start the alert compliance tester! Waiting on a signal to quit\n"

wait
