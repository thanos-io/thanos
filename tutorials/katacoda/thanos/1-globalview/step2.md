# Step 2 - Installing Thanos sidecar

Let's take the setup from the previous step and seamlessly install Thanos to add Global View with HA handling feature.

## Thanos Components

Thanos is a single Go binary capable to run in different modes. Each mode represents a different
component and can be invoked in a single command.

Let's take a look at all the Thanos commands:

```
docker run --rm quay.io/thanos/thanos:v0.19.0 --help
```{{execute}}

You should see multiple commands that solves different purposes.

In this step we will focus on `thanos sidecar`:

```
  sidecar [<flags>]
    sidecar for Prometheus server
```

## Sidecar

Sidecar as the name suggests should be deployed together with Prometheus. Sidecar has multiple features:

* It exposes Prometheus metrics as a common Thanos [StoreAPI](https://thanos.io/tip/thanos/integrations.md/#storeapi). StoreAPI
is a generic gRPC API allowing Thanos components to fetch metrics from various systems and backends.
* It is essentially in further long term storage options described in [next]() courses.
* It is capable to watch for configuration and Prometheus rules (alerting or recording) and notify Prometheus for dynamic reloads:
  * optionally substitute with environment variables
  * optionally decompress if gzipp-ed

You can read more about sidecar [here](https://thanos.io/tip/components/sidecar.md/)

## Installation

To allow Thanos to efficiently query Prometheus data, let's install sidecar to each Prometheus instances we deployed in the previous step as shown below:

![sidecar install](https://docs.google.com/drawings/d/e/2PACX-1vRHlEJd9OVH80hczxkqZKYDVXxwugX55VWKtLJhS6R7D3BbmkBW9qGyyD4JyLbAe9CK9EzvurWTagTR/pub?w=1058&h=330)

For this setup the only configuration required for sidecar is the Prometheus API URL and access to the configuration file.
Former will allow us to access Prometheus metrics, the latter will allow sidecar to reload Prometheus configuration in runtime.

Click snippets to add sidecars to each Prometheus instance.

### Adding sidecar to "EU1" Prometheus

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus0_eu1.yml:/etc/prometheus/prometheus.yml \
    --name prometheus-0-sidecar-eu1 \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
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
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --http-address 0.0.0.0:19091 \
    --grpc-address 0.0.0.0:19191 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url http://127.0.0.1:9091 && echo "Started sidecar for Prometheus 0 US1"
```{{execute}}

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus1_us1.yml:/etc/prometheus/prometheus.yml \
    --name prometheus-1-sidecar-us1 \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --http-address 0.0.0.0:19092 \
    --grpc-address 0.0.0.0:19192 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url http://127.0.0.1:9092 && echo "Started sidecar for Prometheus 1 US1"
```{{execute}}

## Verification

Now, to check if sidecars are running well, let's modify Prometheus scrape configuration to include our added sidecars.

As always click `Copy To Editor` for each config to propagate the configs to each file.

Note that only thanks to the sidecar, all those changes will be immediately reloaded and updated in Prometheus!

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
      - targets: ['127.0.0.1:9091','127.0.0.1:9092']
  - job_name: 'sidecar'
    static_configs:
      - targets: ['127.0.0.1:19091','127.0.0.1:19092']
</pre>

<pre class="file" data-filename="prometheus1_us1.yml" data-target="replace">
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: us1
    replica: 1

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9091','127.0.0.1:9092']
  - job_name: 'sidecar'
    static_configs:
      - targets: ['127.0.0.1:19091','127.0.0.1:19092']
</pre>

Now you should see new, updated configuration on each Prometheus. For example here in [Prometheus 0 EU1 /config](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/config).
In the same time [`up`](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.expr=up&g0.tab=1) should show `job=sidecar` metrics.

Since now Prometheus has access to sidecar metrics we can query for [`thanos_sidecar_prometheus_up`](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.expr=thanos_sidecar_prometheus_up&g0.tab=1)
to check if sidecar has access to Prometheus.

## Next

Great! Now you should have setup deployed as in the presented image:

![sidecar install](https://docs.google.com/drawings/d/e/2PACX-1vRHlEJd9OVH80hczxkqZKYDVXxwugX55VWKtLJhS6R7D3BbmkBW9qGyyD4JyLbAe9CK9EzvurWTagTR/pub?w=1058&h=330)

In the next step, we will add a final component allowing us to fetch Prometheus metrics from a single endpoint.
