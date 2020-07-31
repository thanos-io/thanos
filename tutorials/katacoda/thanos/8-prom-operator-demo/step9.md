### What if we have to stream Prometheus data?

Let's deploy Thanos Receive to receive Prometheus data!

`manifests/receiver/thanos-receive-statefulset.yaml`{{open}}

The important part of it is `hashring.json`, for now harcoded manually (controller for this [exists](https://github.com/observatorium/thanos-receive-controller)):

`manifests/receiver/thanos-receive-configmap.yaml`{{open}}

```
kubectl apply -f /root/manifests/receiver/
```{{execute}}

```
kubectl get po
```{{execute}}

Now let's configure Prometheus to stream data to receive:

`manifests/prometheus/prometheus.yaml`{{open}}

<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="    objectStorageConfig:
      key: thanos.yaml
      name: thanos-objstore-config">  remoteWrite:
    - url: http://thanos-receiver-lb.svc.default.cluster.local:19291/write</pre>

```
kubectl apply -f /root/manifests/prometheus/prometheus.yaml
```{{execute}}

```
kubectl get po
```{{execute}}

Let's check what we see in [Thanos Query UI](https://[[HOST_SUBDOMAIN]]-30093-[[KATACODA_HOST]].environments.katacoda.com/new/graph?g0.range_input=1h&g0.max_source_resolution=0s&g0.expr=prometheus_tsdb_head_series&g0.tab=0)

After some time we should see the data from both Prometheus sidecars and receiver.
