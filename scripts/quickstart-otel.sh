#!/usr/bin/env bash
#
# Starts three Prometheus servers scraping themselves and sidecars for each.
# Two query nodes are started and all are clustered together.

trap 'kill 0' SIGTERM

PROMETHEUS_EXECUTABLE=${PROMETHEUS_EXECUTABLE:-"prometheus"}
THANOS_EXECUTABLE=${THANOS_EXECUTABLE:-"thanos"}
OTEL_EXECUTABLE=${OTEL_EXECUTABLE:-"otelcol-contrib"}
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

# Setup alert / rules config file.
  cat >data/otel-config.yaml <<-EOF
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
  prometheus:
    config:
      scrape_configs:
        - job_name: otel-collector
          scrape_interval: 5s
          static_configs:
            - targets: [localhost:8888]
exporters:
  otlphttp/thanos:
    endpoint: "http://localhost:10908"
    tls:
      insecure: true
  debug:
    verbosity: detailed
extensions:
  health_check:
  pprof:
service:
  telemetry:
    logs:
      level: "debug"
  extensions: [pprof, health_check]
  pipelines:
    metrics:
      receivers:
        - prometheus
        - otlp
      exporters:
        - otlphttp/thanos
        #- debug
EOF

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

# Requires otel-contrib binary which can be grabbed from https://github.com/open-telemetry/opentelemetry-collector-releases/releases
${OTEL_EXECUTABLE} \
  --config=data/otel-config.yaml

sleep 0.5

echo "all started; waiting for signal"

wait
