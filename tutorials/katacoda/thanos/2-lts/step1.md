# Step 1 - Initial Prometheus Setup

In this tutorial, we will mimic the usual state with a Prometheus server running for several months.
We will use it to seamlessly backup all old data in the object storage and configure Prometheus for continuous backup mode, which
will allow us to cost-effectively achieve unlimited retention for Prometheus.

Last but not the least, we will go through setting all up for querying and automated maintenance (e.g compactions, retention and downscaling).
  
In order to showcase all of this, let's start with a single cluster setup from the previous course. Let's start this initial Prometheus setup, ready?

## Generate Artificial Metrics for 1 month with thanosbench

Actually, before starting Prometheus, let's generate some **artificial data**. You most likely want to learn about Thanos fast,
so you probably don't have a month to wait for this tutorial until Prometheus collects the month of metrics, do you? (:

We will use our handy [thanosbench](https://github.com/thanos-io/thanosbench) project to do so! Let's generate Prometheus
blocks with just some 5 series (yolo gauges) that spans from a month ago until now!

Execute the following command (It might take up to a minute):

```
mkdir -p test && docker run -i quay.io/thanos/thanosbench:v0.1.0 block plan -p key-k8s-30d-tiny --labels 'cluster="one"' --max-time 2019-10-18T00:00:00Z | docker run -v /root/test:/test -i quay.io/thanos/thanosbench:v0.1.0 block gen --output.dir test
```{{execute}}

On successful block creation you should see following log lines:

```
level=info ts=2020-10-20T18:28:42.625041939Z caller=block.go:87 msg="all blocks done" count=9
level=info ts=2020-10-20T18:28:42.625100758Z caller=main.go:118 msg=exiting cmd="block gen"
```

Run `ls -l test/` to see the generated 9 blocks.

## Prometheus Configuration Files

Here, we will prepare configuration files for the Prometheus instance that will run with our pre-generated data.
It will also scrape our components we will use in this tutorial.

Click `Copy To Editor` for config to propagate the configs to file.

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
  - job_name: 'minio'
    metrics_path: /minio/prometheus/metrics
    static_configs:
      - targets: ['127.0.0.1:9000']
  - job_name: 'store_gateway'
    static_configs:
      - targets: ['127.0.0.1:19095']
</pre>

## Starting Prometheus Instances

Let's now start the container representing Prometheus instance.

Note `-v $(pwd)/test:/prometheus \` and `--storage.tsdb.path=/prometheus` that allows us to place our generated data in Prometheus data directory.

Execute the following commands:

### Prepare "persistent volumes"


### Deploying "EU1"

Let's deploy Prometheus now. Note that we disabled local Prometheus compactions `storage.tsdb.max-block-duration` and `min` flags. Currently, this is important for the basic object storage backup scenario to avoid conflicts between the bucket and local compactions. Read more [here](https://thanos.io/tip/components/sidecar.md/#sidecar).

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus0_eu1.yml:/etc/prometheus/prometheus.yml \
    -v $(pwd)/test:/prometheus \
    -u root \
    --name prometheus-0-eu1 \
    quay.io/prometheus/prometheus:v2.19.0 \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --storage.tsdb.max-block-duration=2h \
    --storage.tsdb.min-block-duration=2h \
    --web.listen-address=:9090 \
    --web.external-url=https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com \
    --web.enable-lifecycle \
    --web.enable-admin-api && echo "Prometheus EU1 started!"
```{{execute}}

## Setup Verification

Once started you should be able to reach the Prometheus instance here:

* [Prometheus-0 EU1](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/)

# Installing Thanos sidecar

At the end of this step, we will have running Prometheus instance with sidecar deployed. You can read more about sidecar [here](https://thanos.io/tip/components/sidecar.md/).


## Deployment

Click snippets to add a sidecar to the Prometheus instance.

### Adding sidecar to "EU1" Prometheus

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus0_eu1.yml:/etc/prometheus/prometheus.yml \
    -v $(pwd)/test:/prometheus \
    --name prometheus-0-sidecar-eu1 \
    -u root \
    quay.io/thanos/thanos:v0.16.0 \
    sidecar \
    --http-address 0.0.0.0:19090 \
    --grpc-address 0.0.0.0:19190 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url http://127.0.0.1:9090 && echo "Started sidecar for Prometheus 0 EU1"
```{{execute}}

Now, you should have a sidecar running well. Since now Prometheus has access to sidecar metrics we can query for [`thanos_sidecar_prometheus_up`](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.expr=thanos_sidecar_prometheus_up&g0.tab=1) to check if sidecar has access to Prometheus.

## Problem statement:

Let's try to play with this setup a bit.

Grab a coffee (or your favorite tasty beverage). Let's verify whether the blocks were uploaded before or not? Interesting? 😉

Tip: Look for `prometheus_tsdb_reloads_total` metric 🕵️‍

* Check here <a href="https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1h&g0.expr=prometheus_tsdb_reloads_total&g0.tab=1&g1.range_input=5m&g1.expr=prometheus_tsdb_head_series&g1.tab=0">`prometheus_tsdb_reloads_total`</a>

## Deploying Thanos Querier

Let' now start the Query component. As you remember [Thanos sidecar](https://thanos.io/tip/components/query.md/) exposes `StoreAPI`
so we will make sure we point the Querier to the gRPC endpoints of the sidecar:

Click below snippet to start the Querier.

```
docker run -d --net=host --rm \
    --name querier \
    quay.io/thanos/thanos:v0.16.0 \
    query \
    --http-address 0.0.0.0:29090 \
    --query.replica-label replica \
    --store 127.0.0.1:19190 \
    --store 127.0.0.1:10906 && echo "Started Thanos Querier"
```{{execute}}

## Setup verification

Thanos Querier exposes very similar UI to the Prometheus, but on top of many `StoreAPIs, you wish to connect to.

To check if the Querier works as intended let's look on [Querier UI `Store` page](https://[[HOST_SUBDOMAIN]]-29090-[[KATACODA_HOST]].environments.katacoda.com/stores).

This should list the sidecar, including the external label.
