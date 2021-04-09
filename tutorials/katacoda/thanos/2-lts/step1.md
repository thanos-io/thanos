# Step 1 - Initial Prometheus Setup

In this tutorial, we will mimic the usual state with a Prometheus server running for... a year!.
We will use it to seamlessly backup all old data in the object storage and configure Prometheus for continuous backup mode, which
will allow us to cost-effectively achieve unlimited retention for Prometheus.

Last but not the least, we will go through setting all up for querying and automated maintenance (e.g compactions, retention and downsampling).

In order to showcase all of this, let's start with a single cluster setup from the previous course. Let's start this initial Prometheus setup, ready?

## Generate Artificial Metrics for 1 year

Actually, before starting Prometheus, let's generate some **artificial data**. You most likely want to learn about Thanos fast,
so you probably don't have months to wait for this tutorial until Prometheus collects the month of metrics, do you? (:

We will use our handy [thanosbench](https://github.com/thanos-io/thanosbench) project to do so! Let's generate Prometheus
data (in form of TSDB blocks) with just 5 series (gauges) that spans from a year ago until now (-6h)!

Execute the following command (should take few seconds):

```
mkdir -p /root/prom-eu1 && docker run -i quay.io/thanos/thanosbench:v0.2.0-rc.1 block plan -p continuous-365d-tiny --labels 'cluster="eu1"' --max-time=6h | docker run -v /root/prom-eu1:/prom-eu1 -i quay.io/thanos/thanosbench:v0.2.0-rc.1 block gen --output.dir prom-eu1
```{{execute}}

On successful block creation you should see following log lines:

```
level=info ts=2020-10-20T18:28:42.625041939Z caller=block.go:87 msg="all blocks done" count=13
level=info ts=2020-10-20T18:28:42.625100758Z caller=main.go:118 msg=exiting cmd="block gen"
```

Run the below command to see dozens of generated TSDB blocks:

```
ls -lR /root/prom-eu1
```{{execute}}

## Prometheus Configuration File

Here, we will prepare configuration files for the Prometheus instance that will run with our pre-generated data.
It will also scrape our components we will use in this tutorial.

Click `Copy To Editor` for config to propagate the configs to file.

<pre class="file" data-filename="prometheus0_eu1.yml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: eu1
    replica: 0
    tenant: team-eu # Not needed, but a good practice if you want to grow this to multi-tenant system some day.

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9090']
  - job_name: 'sidecar'
    static_configs:
      - targets: ['127.0.0.1:19090']
  - job_name: 'minio'
    metrics_path: /minio/prometheus/metrics
    static_configs:
      - targets: ['127.0.0.1:9000']
  - job_name: 'querier'
    static_configs:
      - targets: ['127.0.0.1:9091']
  - job_name: 'store_gateway'
    static_configs:
      - targets: ['127.0.0.1:19092']
</pre>

## Starting Prometheus Instance

Let's now start the container representing Prometheus instance.

Note `-v /root/prom-eu1:/prometheus \` and `--storage.tsdb.path=/prometheus` that allows us to place our generated data
in Prometheus data directory.

Let's deploy Prometheus now. Note that we disabled local Prometheus compactions `storage.tsdb.max-block-duration` and `min` flags.
Currently, this is important for the basic object storage backup scenario to avoid conflicts between the bucket and local compactions.
Read more [here](https://thanos.io/tip/components/sidecar.md/#sidecar).

We also extend Prometheus retention: `--storage.tsdb.retention.time=1000d`. This is because Prometheus by default removes all data older
than 2 weeks. And we have a year (:

### Deploying "EU1"

```
docker run -d --net=host --rm \
    -v /root/editor/prometheus0_eu1.yml:/etc/prometheus/prometheus.yml \
    -v /root/prom-eu1:/prometheus \
    -u root \
    --name prometheus-0-eu1 \
    quay.io/prometheus/prometheus:v2.20.0 \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.retention.time=1000d \
    --storage.tsdb.path=/prometheus \
    --storage.tsdb.max-block-duration=2h \
    --storage.tsdb.min-block-duration=2h \
    --web.listen-address=:9090 \
    --web.external-url=https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com \
    --web.enable-lifecycle \
    --web.enable-admin-api
```{{execute}}

## Setup Verification

Once started you should be able to reach the Prometheus instance here and query.. 1 year of data!

* [Prometheus-0 EU1](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1y&g0.expr=continuous_app_metric0&g0.tab=0)

## Thanos Sidecar & Querier

Similar to previous course, let's setup global view querying with sidecar:

```
docker run -d --net=host --rm \
    --name prometheus-0-eu1-sidecar \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --http-address 0.0.0.0:19090 \
    --grpc-address 0.0.0.0:19190 \
    --prometheus.url http://127.0.0.1:9090
```{{execute}}

And Querier. As you remember [Thanos sidecar](https://thanos.io/tip/components/query.md/) exposes `StoreAPI`
so we will make sure we point the Querier to the gRPC endpoints of the sidecar:

```
docker run -d --net=host --rm \
    --name querier \
    quay.io/thanos/thanos:v0.19.0 \
    query \
    --http-address 0.0.0.0:9091 \
    --query.replica-label replica \
    --store 127.0.0.1:19190
```{{execute}}

## Setup verification

Similar to previous course let's check if the Querier works as intended. Let's look on
[Querier UI `Store` page](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/stores).

This should list the sidecar, including the external labels.

On graph you should also see our 5 series for 1y time, thanks to Prometheus and sidecar StoreAPI: [Graph](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1y&g0.max_source_resolution=0s&g0.expr=continuous_app_metric0&g0.tab=0).

Click `Continue` to see how we can move this data to much cheaper and easier to operate object storage.
