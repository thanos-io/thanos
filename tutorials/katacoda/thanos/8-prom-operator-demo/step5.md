## Deploy 2 replicas of Prometheus through operator

Let's see how Prometheus definition looks like:

`manifests/prometheus/prometheus.yaml`{{open}}

Adding Thanos is as easy as adding two lines:
 
<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="  # Nice, but what about Thanos?">  thanos:
    version: v0.14.0
    # Thanos sidecar will be now included!</pre>

You can see what Prometheus resource can include [here](https://github.com/prometheus-operator/prometheus-operator/blob/v0.40.0/Documentation/api.md#prometheus).

```
kubectl apply -f /root/manifests/prometheus/
```{{execute}}

```
kubectl get po
```{{execute}}

Let's see what it scrapes: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30090-[[KATACODA_HOST]].environments.katacoda.com/new/targets)

You should see no targets indeed! Check next step how to fix this.
