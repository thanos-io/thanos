# Problem Statement & Setup

## Problem Statement

Let's imagine that you run a company called `Wayne Enterprises`. This company runs two clusters: `Batcave` & `Batcomputer`. Each of these sites runs an instance of Prometheus that collects metrics data from applications and services running there.

However, these sites are special. For security reasons, they do not expose public endpoints to the Prometheus instances running there, and so cannot be accessed directly from other parts of your infrastructure.

As the person responsible for implementing monitoring these sites, you have two requirements to meet:

1. Implement a global view of this data. `Wayne Enterprises` needs to know what is happening in all parts of the company - including secret ones!
1. Global view must be queryable in near real-time. We can't afford any delay in monitoring these locations!

Firstly, let us setup two Prometheus instances...

## Setup

### Batcave

Let's use a very simple configuration file, that tells prometheus to scrape its own metrics page every 5 seconds.

<pre class="file" data-filename="prometheus-batcave.yaml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: batcave
    replica: 0

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9090']
</pre>

Run the prometheus instance:

```
docker run -d --net=host --rm \
    -v /root/editor/prometheus-batcave.yaml:/etc/prometheus/prometheus.yaml \
    -v /root/prometheus-batcave-data:/prometheus \
    -u root \
    --name prometheus-batcave \
    quay.io/prometheus/prometheus:v2.27.0 \
    --config.file=/etc/prometheus/prometheus.yaml \
    --storage.tsdb.path=/prometheus \
    --web.listen-address=:9090 \
    --web.external-url=https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com \
    --web.enable-lifecycle
```{{execute}}

Verify that `prometheus-batcave` is running by navigating to the [Batcave Prometheus UI](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com).

<details>
 <summary>Why do we enable the web lifecycle flag?</summary>

  By specifying `--web.enable-lifecycle`, we tell Prometheus to expose the `/-/reload` HTTP endpoint.

  This lets us tell Prometheus to dynamically reload its configuration, which will be useful later in this tutorial.
</details>


### Batcomputer

Almost exactly the same configuration as above, except we run the Prometheus instance on port `9091`.

<pre class="file" data-filename="prometheus-batcomputer.yaml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: batcomputer
    replica: 0

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['127.0.0.1:9091']
</pre>

```
docker run -d --net=host --rm \
    -v /root/editor/prometheus-batcomputer.yaml:/etc/prometheus/prometheus.yaml \
    -v /root/prometheus-batcomputer:/prometheus \
    -u root \
    --name prometheus-batcomputer \
    quay.io/prometheus/prometheus:v2.27.0 \
    --config.file=/etc/prometheus/prometheus.yaml \
    --storage.tsdb.path=/prometheus \
    --web.listen-address=:9091 \
    --web.external-url=https://2886795291-9091-elsy05.environments.katacoda.com \
    --web.enable-lifecycle
```{{execute}}

Verify that `prometheus-batcomputer` is running by navigating to the [Batcomputer Prometheus UI](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com).

With these Prometheus instances configured and running, we can now start to architect our global view of all of `Wayne Enterprises`.
