## Thanks to streaming to some Thanos Receive replicated hashring, we can now remove sidecar, there is no need for it anymore.

`manifests/prometheus/prometheus.yaml`{{open}}

<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="  remoteWrite:
    - url: http://thanos-receiver-lb.svc.default.cluster.local:19291/write"></pre>

```
kubectl apply -f /root/manifests/prometheus/prometheus.yaml
```{{execute}}

```
kubectl get po
```{{execute}}

Now we should not see any data from sidecars in [Thanos Query UI](https://[[HOST_SUBDOMAIN]]-30093-[[KATACODA_HOST]].environments.katacoda.com/new/graph?g0.range_input=1h&g0.max_source_resolution=0s&g0.expr=prometheus_tsdb_head_series&g0.tab=0)

You can also configure to upload receiver data to object storage. (: 