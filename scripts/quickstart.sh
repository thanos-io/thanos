#!/usr/bin/env bash
#
# Starts three Prometheus servers scraping themselves and sidecars for each.
# Two query nodes are started and all are clustered together.

trap 'kill 0' SIGTERM

# Start local object storage, if desired.
if [ -n "${MINIO_ENABLED}" ]
then
  export MINIO_ACCESS_KEY="THANOS"
  export MINIO_SECRET_KEY="ITSTHANOSTIME"
  export S3_ACCESS_KEY=${MINIO_ACCESS_KEY}
  export S3_SECRET_KEY=${MINIO_SECRET_KEY}
  export S3_BUCKET="thanos"
  export S3_ENDPOINT="127.0.0.1:9000"
  rm -rf data/minio
  mkdir -p data/minio

  minio server ./data/minio \
      --address ${S3_ENDPOINT} &
  sleep 3
  # create the bucket
  mc config host add tmp http://${S3_ENDPOINT} THANOS ITSTHANOSTIME
  mc mb tmp/${S3_BUCKET}
  mc config host rm tmp
fi

# Start three Prometheus servers monitoring themselves.
for i in `seq 1 3`
do
  rm -rf data/prom${i}
  mkdir -p data/prom${i}/

  cat > data/prom${i}/prometheus.yml <<- EOF
global:
  external_labels:
    prometheus: prom-${i}
scrape_configs:
- job_name: prometheus
  scrape_interval: 5s
  static_configs:
  - targets:
    - "localhost:909${i}"
- job_name: thanos-sidecar
  scrape_interval: 5s
  static_configs:
  - targets:
    - "localhost:1919${i}"
- job_name: thanos-store
  scrape_interval: 5s
  static_configs:
  - targets:
    - "localhost:19791"
- job_name: thanos-query
  scrape_interval: 5s
  static_configs:
  - targets:
    - "localhost:19491"
    - "localhost:19492"
EOF

  prometheus \
    --config.file         data/prom${i}/prometheus.yml \
    --storage.tsdb.path   data/prom${i} \
    --log.level           warn \
    --web.listen-address  0.0.0.0:909${i} &

  sleep 0.25
done

sleep 0.5

# Start one sidecar for each Prometheus server.
for i in `seq 1 3`
do
  thanos sidecar \
    --debug.name                sidecar-${i} \
    --grpc-address              0.0.0.0:1909${i} \
    --http-address              0.0.0.0:1919${i} \
    --prometheus.url            http://localhost:909${i} \
    --tsdb.path                 data/prom${i} \
    --gcs.bucket                "${GCS_BUCKET}" \
    --s3.endpoint               "${S3_ENDPOINT}" \
    --s3.bucket                 "${S3_BUCKET}" \
    --s3.access-key             "${S3_ACCESS_KEY}" \
    --s3.secret-key             "${S3_SECRET_KEY}" \
    --s3.insecure               \
    --cluster.address           0.0.0.0:1939${i} \
    --cluster.advertise-address 127.0.0.1:1939${i} \
    --cluster.peers             127.0.0.1:19391 &

  sleep 0.25
done

sleep 0.5

if [ -n "${GCS_BUCKET}" -o -n "${S3_ENDPOINT}" ]
then
  thanos store \
    --debug.name                store \
    --log.level debug \
    --grpc-address              0.0.0.0:19691 \
    --http-address              0.0.0.0:19791 \
    --tsdb.path                 data/store \
    --gcs.bucket                "${GCS_BUCKET}" \
    --s3.endpoint               "${S3_ENDPOINT}" \
    --s3.bucket                 "${S3_BUCKET}" \
    --s3.access-key             "${S3_ACCESS_KEY}" \
    --s3.insecure               \
    --s3.secret-key             "${S3_SECRET_KEY}" \
    --cluster.address           0.0.0.0:19891 \
    --cluster.advertise-address 127.0.0.1:19891 \
    --cluster.peers             127.0.0.1:19391 &
fi

sleep 0.5

# Start to query nodes.
for i in `seq 1 2`
do
  thanos query \
    --debug.name                query-${i} \
    --grpc-address              0.0.0.0:1999${i} \
    --http-address              0.0.0.0:1949${i} \
    --cluster.address           0.0.0.0:1959${i} \
    --cluster.advertise-address 127.0.0.1:1959${i} \
    --cluster.peers             127.0.0.1:19391 &
done

wait