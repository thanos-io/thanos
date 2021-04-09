## Global View + HA: Querier with 1y of data.

```
cd /root/editor && CURR_DIR=$(pwd)
```{{execute}}

### Generating data:

* Plan for 1y worth of metrics data:

```
docker run -it --rm quay.io/thanos/thanosbench:v0.2.0-rc.1 block plan -p continuous-365d-tiny --max-time=6h > ${CURR_DIR}/block-spec.yaml
```{{execute}}

* Plan and generate data for `cluster="eu1", replica="0"` Prometheus:

```
mkdir ${CURR_DIR}/prom-eu1-replica0 && docker run -it --rm quay.io/thanos/thanosbench:v0.2.0-rc.1 block plan -p continuous-365d-tiny --labels 'cluster="eu1"' --max-time=6h | \
    docker run -v ${CURR_DIR}/prom-eu1-replica0:/out -i quay.io/thanos/thanosbench:v0.2.0-rc.1 block gen --output.dir /out
```{{execute}}

* Plan and generate data for `cluster="eu1", replica="1"` Prometheus:

```
mkdir ${CURR_DIR}/prom-eu1-replica1 && docker run -it --rm quay.io/thanos/thanosbench:v0.2.0-rc.1 block plan -p continuous-365d-tiny --labels 'cluster="eu1"' --max-time=6h | \
    docker run -v ${CURR_DIR}/prom-eu1-replica1:/out -i quay.io/thanos/thanosbench:v0.2.0-rc.1 block gen --output.dir /out
```{{execute}}

* Plan and generate data for `cluster="us1", replica="0"` Prometheus:

```
mkdir ${CURR_DIR}/prom-us1-replica0 && docker run -it --rm quay.io/thanos/thanosbench:v0.2.0-rc.1 block plan -p continuous-365d-tiny --labels 'cluster="us1"' --max-time=6h | \
    docker run -v ${CURR_DIR}/prom-us1-replica0:/out -i quay.io/thanos/thanosbench:v0.2.0-rc.1 block gen --output.dir /out
```{{execute}}

### Create Promethues configs to use:

* `cluster="eu1", replica="0"` Prometheus:

```
cat <<EOF > ${CURR_DIR}/prom-eu1-replica0-config.yaml
global:
  scrape_interval: 5s
  external_labels:
    cluster: eu1
    replica: 0
    tenant: team-eu # Not needed, but a good practice if you want to grow this to multi-tenant system some day.

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9091','127.0.0.1:9092']
EOF
```{{execute}}

* `cluster="eu1", replica="1"` Prometheus:

```
cat <<EOF > ${CURR_DIR}/prom-eu1-replica1-config.yaml
global:
  scrape_interval: 5s
  external_labels:
    cluster: eu1
    replica: 1
    tenant: team-eu # Not needed, but a good practice if you want to grow this to multi-tenant system some day.

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9091','127.0.0.1:9092']
EOF
```{{execute}}

* `cluster="us1", replica="0"` Prometheus:

```
cat <<EOF > ${CURR_DIR}/prom-us1-replica0-config.yaml
global:
  scrape_interval: 5s
  external_labels:
    cluster: us1
    replica: 0
    tenant: team-us

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9093']
EOF
```{{execute}}

### Ports for Prometheus-es/Prometheis/Prometheus

```
PROM_EU1_0_PORT=9091 && \
PROM_EU1_1_PORT=9092 && \
PROM_US1_0_PORT=9093
PROM_EU1_0_EXT_ADDRESS=https://[[HOST_SUBDOMAIN]]-${PROM_EU1_0_PORT}-[[KATACODA_HOST]].environments.katacoda.com && \
PROM_EU1_1_EXT_ADDRESS=https://[[HOST_SUBDOMAIN]]-${PROM_EU1_1_PORT}-[[KATACODA_HOST]].environments.katacoda.com && \
PROM_US1_0_EXT_ADDRESS=https://[[HOST_SUBDOMAIN]]-${PROM_US1_0_PORT}-[[KATACODA_HOST]].environments.katacoda.com
```{{execute}}

### Deploying Prometheus-es/Prometheis/Prometheus instances

```
docker run -it --rm quay.io/prometheus/prometheus:v2.20.0 --help
```{{execute}}

* `cluster="eu1", replica="0"` Prometheus:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-eu1-replica0-config.yaml:/etc/prometheus/prometheus.yml \
    -v ${CURR_DIR}/prom-eu1-replica0:/prometheus \
    -u root \
    --name prom-eu1-0 \
    quay.io/prometheus/prometheus:v2.20.0 \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --storage.tsdb.retention.time=1000d \
    --storage.tsdb.max-block-duration=2h \
    --storage.tsdb.min-block-duration=2h \
    --web.listen-address=:${PROM_EU1_0_PORT} \
    --web.external-url=${PROM_EU1_0_EXT_ADDRESS} \
    --web.enable-lifecycle \
    --web.enable-admin-api
```{{execute}}

* `cluster="eu1", replica="1"` Prometheus:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-eu1-replica1-config.yaml:/etc/prometheus/prometheus.yml \
    -v ${CURR_DIR}/prom-eu1-replica1:/prometheus \
    -u root \
    --name prom-eu1-1 \
    quay.io/prometheus/prometheus:v2.20.0 \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --storage.tsdb.retention.time=1000d \
    --storage.tsdb.max-block-duration=2h \
    --storage.tsdb.min-block-duration=2h \
    --web.listen-address=:${PROM_EU1_1_PORT} \
    --web.external-url=${PROM_EU1_1_EXT_ADDRESS} \
    --web.enable-lifecycle \
    --web.enable-admin-api
```{{execute}}

* `cluster="us1", replica="0"` Prometheus:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-us1-replica0-config.yaml:/etc/prometheus/prometheus.yml \
    -v ${CURR_DIR}/prom-us1-replica0:/prometheus \
    -u root \
    --name prom-us1-0 \
    quay.io/prometheus/prometheus:v2.20.0 \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --storage.tsdb.retention.time=1000d \
    --storage.tsdb.max-block-duration=2h \
    --storage.tsdb.min-block-duration=2h \
    --web.listen-address=:${PROM_US1_0_PORT} \
    --web.external-url=${PROM_US1_0_EXT_ADDRESS} \
    --web.enable-lifecycle \
    --web.enable-admin-api
```{{execute}}

### Step1: Sidecar

```
docker run -it --rm quay.io/thanos/thanos:v0.19.0 --help
```{{execute}}


* `cluster="eu1", replica="0"` sidecar:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-eu1-replica0-config.yaml:/etc/prometheus/prometheus.yml \
    --name prom-eu1-0-sidecar \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --http-address 0.0.0.0:19091 \
    --grpc-address 0.0.0.0:19191 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url "http://127.0.0.1:${PROM_EU1_0_PORT}"
```{{execute}}

* `cluster="eu1", replica="1"` sidecar:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-eu1-replica1-config.yaml:/etc/prometheus/prometheus.yml \
    --name prom-eu1-1-sidecar \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --http-address 0.0.0.0:19092 \
    --grpc-address 0.0.0.0:19192 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url "http://127.0.0.1:${PROM_EU1_1_PORT}"
```{{execute}}

* `cluster="us1", replica="0"` sidecar:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-us1-replica0-config.yaml:/etc/prometheus/prometheus.yml \
    --name prom-us1-0-sidecar \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --http-address 0.0.0.0:19093 \
    --grpc-address 0.0.0.0:19193 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url "http://127.0.0.1:${PROM_US1_0_PORT}"
```{{execute}}

### Step2: Global View + HA: Querier

```
docker run -d --net=host --rm \
    --name querier \
    quay.io/thanos/thanos:v0.19.0 \
    query \
    --http-address 0.0.0.0:9090 \
    --grpc-address 0.0.0.0:19190 \
    --query.replica-label replica \
    --store 127.0.0.1:19191 \
    --store 127.0.0.1:19192 \
    --store 127.0.0.1:19193
```{{execute}}

Visit https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com to see Thanos UI.
