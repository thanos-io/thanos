# Configure Prometheus Remote Write

Our problem in the last step was that we have have not yet configured Prometheus to `remote_write` to our `Thanos Receive` instance.

We need to tell `prometheus-batcave` & `prometheus-batcomputer` where to write their data to.

## Update Configuration

The docs for this configuration option can be found [here](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

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
remote_write:
- url: 'http://127.0.0.1:10908/api/v1/receive'
</pre>

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
remote_write:
- url: 'http://127.0.0.1:10908/api/v1/receive'
</pre>

## Reload Configuration

Since we supplied the `--web.enable-lifecycle` flag in our Prometheus instances, we can dynamically reload the configuration by `curl`-ing the `/-/reload` endpoints.

```
curl -X POST http://127.0.0.1:9090/-/reload
curl -X POST http://127.0.0.1:9091/-/reload
```{{execute}}

Verify this has taken affect by checking the `/config` page on our Prometheus instances:
* `prometheus-batcave` [config page](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/config)
* `prometheus-batcomputer` [config page](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/config)

In both cases you should see the `remote_write` options in the configuration.