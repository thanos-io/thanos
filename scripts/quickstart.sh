#!/usr/bin/env bash
#
# Starts three Prometheus servers scraping themselves and sidecars for each.
# Two query nodes are started and all are clustered together.
# The installed binaries for Prometheus and Thanos are called, i.e. `go install`
# must be called when testing this with development versions.

trap 'kill 0' SIGTERM

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
EOF

  prometheus \
    --config.file         data/prom${i}/prometheus.yml \
    --storage.tsdb.path   data/prom${i} \
    --web.listen-address  0.0.0.0:909${i} &

  sleep 0.25
done

sleep 0.5

# Start one sidecar for each Prometheus server.
for i in `seq 1 3`
do
  thanos sidecar \
    --api-address               0.0.0.0:1909${i} \
    --metrics-address           0.0.0.0:1919${i} \
    --prometheus.url            http://localhost:909${i} \
    --tsdb.path                 data/prom${i} \
    --cluster.address           0.0.0.0:1939${i} \
    --cluster.advertise-address 127.0.0.1:1939${i} \
    --cluster.peers             127.0.0.1:19391 &

  sleep 0.25
done

sleep 0.5

# Start to query nodes.
for i in `seq 1 2`
do
  thanos query \
    --api-address               0.0.0.0:1949${i} \
    --cluster.address           0.0.0.0:1959${i} \
    --cluster.advertise-address 127.0.0.1:1959${i} \
    --cluster.peers             127.0.0.1:19391 &
done

wait