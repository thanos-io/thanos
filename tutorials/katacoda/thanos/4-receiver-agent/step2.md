
# Setup `batmobile` and `batcopter` lightweight metric collection

With `Wayne Enterprise` platform ready to ingest Remote Write data, we need to think about how we satisfy our three requirements:

1. Implement a global view of this data. `Wayne Enterprises` needs to know what is happening in all company parts - including secret ones!
2. `Batmobile` and `Batcopter` can be out of network for some duration of time. You don't want to lose precious data.
3. `Batmobile` and `Batcopter` do not have large compute power, so you want an efficient solution that avoids extra computations and storage.

How are we going to do this?

Previously, the recommended approach was to deploy Prometheus to our edge environment which scrapes required applications, then remote writes all metrics to Thanos Receive. This will work and satisfy 1st and 2nd requirements, however Prometheus does some things that we don't need in this specific scenario:

* We don't want to alert locally or use recording rules from our edge places.
* We don't want to query data locally, and we want to store it for an as short duration as possible.

Prometheus was designed as a stateful time-series database, and it adds certain mechanics which are not desired for full forward mode. For example:

* Prometheus builds additional memory structures for easy querying from memory.
* Prometheus does not remove data when it is safely sent via remote write. It waits for at least two hours and only after the TSDB block is persisted to the disk, it may or may not be removed, depending on retention configuration.

This is where Agent mode comes in handy! It is a native Prometheus mode built into the Prometheus binary. If you add the `--agent` flag when running Prometheus, it will run a dedicated, specially streamlined database, optimized for forwarding purposes, yet able to persist scraped data in the event of a crash, restart or network disconnection.

Let's try to deploy it to fulfil `batmobile` and `batcopter` monitoring requirements.

## Deploy `Prometheus Agent` on `batmobile`

Let's use a very simple configuration file that tells prometheus agent to scrape its own metrics page every 5 seconds and forwards it's to our running `Thanos Receive`.

<pre class="file" data-filename="prom-agent-batmobile.yaml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: batmobile
    replica: 0

scrape_configs:
  - job_name: 'prometheus-agent'
    static_configs:
      - targets: ['127.0.0.1:9090']

remote_write:
- url: 'http://127.0.0.1:10908/api/v1/receive'
</pre>

Run the prometheus in agent mode:

```
docker run -d --net=host --rm \
-v /root/editor/prom-agent-batmobile.yaml:/etc/prometheus/prometheus.yaml \
-v /root/prom-batmobile-data:/prometheus \
-u root \
--name prom-agent-batmobile \
quay.io/prometheus/prometheus:v2.32.0-beta.0 \
--enable-feature=agent \
--config.file=/etc/prometheus/prometheus.yaml \
--storage.agent.path=/prometheus \
--web.listen-address=:9090
```{{execute}}

This runs Prometheus Agent, which will scrape itself and forward all to Thanos Receive. It also exposes UI with pages that relate to scraping, service discovery, configuration and build information.

Verify that `prom-agent-batmobile` is running by navigating to the [Batmobile Prometheus Agent UI](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/targets).

You should see one target: Prometheus Agent on `batmobile` itself.

## Deploy `Prometheus Agent` on `batcopter`

Similarly, we can configure and deploy the second agent:

<pre class="file" data-filename="prom-agent-batcopter.yaml" data-target="replace">
global:
  scrape_interval: 5s
  external_labels:
    cluster: batcopter
    replica: 0

scrape_configs:
  - job_name: 'prometheus-agent'
    static_configs:
      - targets: ['127.0.0.1:9091']

remote_write:
- url: 'http://127.0.0.1:10908/api/v1/receive'
</pre>

```
docker run -d --net=host --rm \
-v /root/editor/prom-agent-batcopter.yaml:/etc/prometheus/prometheus.yaml \
-v /root/prom-batcopter-data:/prometheus \
-u root \
--name prom-agent-batcopter \
quay.io/prometheus/prometheus:v2.32.0-beta.0 \
--enable-feature=agent \
--config.file=/etc/prometheus/prometheus.yaml \
--storage.agent.path=/prometheus \
--web.listen-address=:9091
```{{execute}}

Verify that `prom-agent-batcopter` is running by navigating to the [Batcopter Prometheus Agent UI](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/targets).

You should see one target: Prometheus Agent on `batcopter` itself.

Now, let's navigate to the last step to verify our `Wayne Enterprises` setup!
