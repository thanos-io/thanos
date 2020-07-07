# Step 3 - Installing Thanos Store

In this step, we will learn about Thanos Store Gateway, how to start and what problems are solved by it.

## Thanos Components

Thanos is a single Go binary capable to run in different modes. Each mode represents a different component and can be invoked in a single command.

Let's take a look at all the Thanos commands:

```docker run --rm quay.io/thanos/thanos:v0.12.2 --help```

You should see multiple commands that solves different purposes, a block storage based long-term storage for Prometheus.

In this step we will focus on thanos `store gateway`:

```
  store [<flags>]
    Store node giving access to blocks in a bucket provider
```

## Store Gateway/ Store :

* This component implements the Store API on top of historical data in an object storage bucket. It acts primarily as an API gateway and therefore does not need significant amounts of local disk space.
* It joins a Thanos cluster on startup and advertises the data it can access.
* It keeps a small amount of information about all remote blocks on the local disk and keeps it in sync with the bucket.
This data is generally safe to delete across restarts at the cost of increased startup times.


You can read more about [Store](https://thanos.io/components/store.md/) here.

## Installation

Here, we will modify our configuration files to include the store gateway and querier.

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
  - job_name: 'store_gateway'
    static_configs:
      - targets: ['127.0.0.1:19090']
  - job_name: 'querier'
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
  - job_name: 'store_gateway'
    static_configs:
      - targets: ['127.0.0.1:19091']
  - job_name: 'querier'
    static_configs:
      - targets: ['127.0.0.1:19091']
</pre>

## Deployment

Click snippets to deploy thanos store to each Prometheus instance.

### Deploying store to "EU1" Prometheus

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus0_eu1.yml:/etc/prometheus/prometheus.yml \
    --name prometheus-0-store-eu1 \
    -u root \
    store \
    --http-address 0.0.0.0:19090 \
    --grpc-address 0.0.0.0:19190 \
    --objstore.config-file=/etc/prometheus/prometheus.yml \
    --prometheus.url http://127.0.0.1:9090 && echo "Deployed store gateway for Prometheus 0 EU1"
```{{execute}}

### Deploying store to "US1" Prometheus

```
docker run -d --net=host --rm \
    -v $(pwd)/prometheus0_us1.yml:/etc/prometheus/prometheus.yml \
    --name prometheus-0-store-us1 \
    -u root \
    store \
    --http-address 0.0.0.0:19091 \
    --grpc-address 0.0.0.0:19191 \
    --objstore.config-file=/etc/prometheus/prometheus.yml \
    --prometheus.url http://127.0.0.1:9091 && echo "Deployed store gateway for Prometheus 0 US1"
```{{execute}}

## Next

Voila ! In the next step, we will talk about downsampling, why it's important and see how queries are still working served by Thanos Store Gateway.