## Prometheus 

Execute following commands:

### Prepare "persistent volumes"

```
mkdir -p prometheus_data
```{{execute}}

### Deploying Prometheus with sidecar

Let's deploy a couple of Prometheus instances and let them scrape themselves, so we can produce some metrics.

### Prepare configuration

Click `Copy To Editor` for each config to propagate the configs to each file.

First, Prometheus server that scrapes itself:

<pre class="file" data-filename="prometheus0.yml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: eu0
    replica: 0 

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9090']
</pre>

<pre class="file" data-filename="prometheus1.yml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: eu1
    replica: 0 

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9091']
</pre>

<pre class="file" data-filename="prometheus2.yml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: eu2
    replica: 0 

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9092']
</pre>

### Deploy Prometheus with sidecar

```
for i in $(seq 0 2); do
docker run -d --net=host --rm \
    -v $(pwd)/prometheus"${i}".yml:/etc/prometheus/prometheus.yml \
    -v $(pwd)/prometheus_data:/prometheus"${i}" \
    -u root \
    --name prometheus"${i}" \
    quay.io/prometheus/prometheus:v2.20.0 \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --web.listen-address=:909"${i}" \
    --web.external-url=https://[[HOST_SUBDOMAIN]]-909"${i}"-[[KATACODA_HOST]].environments.katacoda.com \
    --web.enable-lifecycle \
    --web.enable-admin-api && echo "Prometheus ${i} started!"
    
docker run -d --net=host --rm \
    -v $(pwd)/prometheus"${i}".yml:/etc/prometheus/prometheus.yml \
    --name prometheus-sidecar"${i}" \
    -u root \
    quay.io/thanos/thanos:v0.16.0-rc.1 \
    sidecar \
    --http-address=0.0.0.0:1909"${i}" \
    --grpc-address=0.0.0.0:1919"${i}" \
    --reloader.config-file=/etc/prometheus/prometheus.yml \
    --prometheus.url=http://127.0.0.1:909"${i}" && echo "Started Thanos Sidecar for Prometheus ${i}!"
done
```{{execute}}

### Verify

```
docker ps
```{{execute}}

## Thanos Global View

And now, let's deploy Thanos Querier to have a global overview on our services.

### Deploy Querier

```
docker run -d --net=host --rm \
    --name querier \
    quay.io/thanos/thanos:v0.16.0-rc.1 \
    query \
    --http-address 0.0.0.0:10902 \
    --grpc-address 0.0.0.0:10901 \
    --query.replica-label replica \
    --store 127.0.0.1:19190 \
    --store 127.0.0.1:19191 \
    --store 127.0.0.1:19192 && echo "Started Thanos Querier!"
```{{execute}}

### Deploy Thanos Query Frontend

First, let's create necessary cache configuration for Frontend:

<pre class="file" data-filename="frontend.yml" data-target="replace">
type: IN-MEMORY
config:
  max_size: "0"
  max_size_items: 2048
  validity: "6h"
</pre>

And deploy Query Frontend:

```
docker run -d --net=host --rm \
    -v $(pwd)/frontend.yml:/etc/thanos/frontend.yml \
    --name query-frontend \
    quay.io/thanos/thanos:v0.16.0-rc.1 \
    query-frontend \
    --http-address 0.0.0.0:20902 \
    --query-frontend.compress-responses \
    --query-frontend.downstream-url=http://127.0.0.1:10902 \
    --query-frontend.log-queries-longer-than=5s \
    --query-range.split-interval=1m \
    --query-range.max-retries-per-request=5 \
    --query-range.response-cache-config-file=/etc/thanos/frontend.yml \
    --cache-compression-type="snappy" && echo "Started Thanos Query Frontend!"
```{{execute}}

### Setup Verification

Once started you should be able to reach the Querier, Query Frontend and Prometheus.

* [Prometheus](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/)
* [Querier](https://[[HOST_SUBDOMAIN]]-10902-[[KATACODA_HOST]].environments.katacoda.com/)
* [Query Frontend](https://[[HOST_SUBDOMAIN]]-20902-[[KATACODA_HOST]].environments.katacoda.com/)
