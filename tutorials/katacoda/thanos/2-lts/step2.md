# Thanos Sidecars

At the end of this step, we will have two running Prometheus instances with sidecar each.

## Step 2 - Adding Thanos Sidecar

Here, we will modify our configuration files to include the sidecars.

Click `Copy To Editor` for each config to propagate the configs to each file.

<pre class="file" data-filename="prometheus0_eu1.yml" data-target="replace">
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: eu1
    replica: 0

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9090']
  - job_name: 'sidecar'
    static_configs:
      - targets: ['127.0.0.1:19090']
</pre>

and

<pre class="file" data-filename="prometheus0_us1.yml" data-target="replace">
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: us1
    replica: 0

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9091']
  - job_name: 'sidecar'
    static_configs:
      - targets: ['127.0.0.1:19091']
</pre>

## Deployment

Click snippets to add sidecars to each Prometheus instance.

### Adding sidecar to "EU1" Prometheus

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus0_eu1.yml:/etc/prometheus/prometheus.yml \
    --name prometheus-0-sidecar-eu1 \
    -u root \
    quay.io/thanos/thanos:v0.12.2 \
    sidecar \
    --http-address 0.0.0.0:19090 \
    --grpc-address 0.0.0.0:19190 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url http://127.0.0.1:9090 && echo "Started sidecar for Prometheus 0 EU1"
```{{execute}}

### Adding sidecars to each replica of Prometheus in "US1"

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus0_us1.yml:/etc/prometheus/prometheus.yml \
    --name prometheus-0-sidecar-us1 \
    -u root \
    quay.io/thanos/thanos:v0.12.2 \
    sidecar \
    --http-address 0.0.0.0:19091 \
    --grpc-address 0.0.0.0:19191 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url http://127.0.0.1:9091 && echo "Started sidecar for Prometheus 0 US1"
```{{execute}}

TODO : Add verification taken from the katacoda