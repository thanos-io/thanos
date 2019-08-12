# Step 1 - Configure our Prometheus servers

We'll start by having two Prometheus servers running representing two data centers. We'll call them `us1` and `eu1`.

## Configuration

Here's the configuration for the first server:

<pre class="file" data-filename="prometheus_us1.yml" data-target="replace">
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    dc: us1

scrape_configs:
  - job_name: 'example'
    static_configs:
      - targets: ['127.0.0.1:9090']
</pre>

And for the second:

<pre class="file" data-filename="prometheus_eu1.yml" data-target="replace">
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    dc: eu1

scrape_configs:
  - job_name: 'example'
    static_configs:
      - targets: ['127.0.0.1:9091']
</pre>
