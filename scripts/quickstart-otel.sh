#!/usr/bin/env bash
#
# Starts three Prometheus servers scraping themselves and sidecars for each.
# Two query nodes are started and all are clustered together.

trap 'kill 0' SIGTERM

PROMETHEUS_EXECUTABLE=${PROMETHEUS_EXECUTABLE:-"prometheus"}
THANOS_EXECUTABLE=${THANOS_EXECUTABLE:-"thanos"}
REMOTE_WRITE_ENABLED=true

if [ ! $(command -v "$PROMETHEUS_EXECUTABLE") ]; then
  echo "Cannot find or execute Prometheus binary $PROMETHEUS_EXECUTABLE, you can override it by setting the PROMETHEUS_EXECUTABLE env variable"
  exit 1
fi

if [ ! $(command -v "$THANOS_EXECUTABLE") ]; then
  echo "Cannot find or execute Thanos binary $THANOS_EXECUTABLE, you can override it by setting the THANOS_EXECUTABLE env variable"
  exit 1
fi

sleep 0.5

if [ -n "${REMOTE_WRITE_ENABLED}" ]; then

  for i in $(seq 0 1 2); do
    ${THANOS_EXECUTABLE} receive \
      --debug.name receive${i} \
      --log.level debug \
      --tsdb.path "./data/remote-write-receive-${i}-data" \
      --grpc-address 0.0.0.0:1${i}907 \
      --grpc-grace-period 1s \
      --http-address 0.0.0.0:1${i}909 \
      --http-grace-period 1s \
      --receive.replication-factor 1 \
      --tsdb.min-block-duration 5m \
      --tsdb.max-block-duration 5m \
      --label "receive_replica=\"${i}\"" \
      --label 'receive="true"' \
      --receive.local-endpoint 127.0.0.1:1${i}907 \
      --receive.hashrings '[{"endpoints":["127.0.0.1:10907","127.0.0.1:11907","127.0.0.1:12907"]}]' \
      --remote-write.address 0.0.0.0:1${i}908  &

    STORES="${STORES} --store 127.0.0.1:1${i}907"
  done

fi

sleep 0.5

QUERIER_JAEGER_CONFIG=$(
  cat <<-EOF
		type: JAEGER
		config:
		  service_name: thanos-query
		  sampler_type: ratelimiting
		  sampler_param: 2
	EOF
)

# Start two query nodes.
for i in $(seq 0 1); do
  ${THANOS_EXECUTABLE} query \
    --debug.name query-"${i}" \
    --log.level debug \
    --grpc-address 0.0.0.0:109"${i}"3 \
    --grpc-grace-period 1s \
    --http-address 0.0.0.0:109"${i}"4 \
    --http-grace-period 1s \
    --query.replica-label prometheus \
    --tracing.config="${QUERIER_JAEGER_CONFIG}" \
    --query.replica-label receive_replica \
    ${STORES} &
done

sleep 0.5

echo "all started; waiting for signal"

wait
