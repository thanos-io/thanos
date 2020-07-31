At Red Hat we help to maintain many open source projects.

One of such project is the https://github.com/coreos/prometheus-operator, first project that
started leveraging CRDs to operate stateful resources like Prometheus.

Thanos, while mostly built from stateless components, is well integrated with Prometheus Operator.

This quick demo will show you quickly how to deploy Prometheuses with Thanos with seamless HA support.

TODO: Probably split it each section to each step...

## It would be nice to just have Prometheus type, right?

```
kubectl get prometheus
```{{execute}}

## Apply Prometheus-Operator Custom Resource Definitions

For example Prometheus CRD you can see here:

`manifests/crds/monitoring.coreos.com_prometheuses.yaml`{{open}}

```
ls -l /root/manifests/crds
```{{execute}}

```
kubectl apply -f /root/manifests/crds/
```{{execute}}

## Let's test them out!

```
kubectl get prometheus
```{{execute}}

```
watch -n 5 kubectl get po
```{{execute}}

CRDs are defined!

## Create Prometheus Operatror

```
kubectl apply -f /root/manifests/operator/
```{{execute}}

```
watch -n 5 kubectl get po
```{{execute}}

Prometheus Operator Deployed!

## Deploy 2 replicas of Prometheus through operator

Let's see how Prometheus definition looks like:

`manifests/prometheus/prometheus.yaml`{{open}}

Adding Thanos is as easy as adding two lines:
 
<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="  # Nice, but what about Thanos?">  thanos:
    version: v0.14.0
    # Thanos sidecar will be now included!</pre>

```
kubectl apply -f /root/manifests/prometheus/
```{{execute}}

```
watch -n 5 kubectl get po
```{{execute}}

Let's see what it scrapes: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30090-[[KATACODA_HOST]].environments.katacoda.com/new/targets)

Nothing?

## Define what to scrape using Service Monitors

`manifests/svcmonitors/prometheus-service-monitor.yaml`{{open}}

```
kubectl apply -f /root/manifests/svcmonitors/
```{{execute}}

Now we should see targets, and we can query: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30090-[[KATACODA_HOST]].environments.katacoda.com/new/targets)

## But there are two replicas. How to use them?

Thanks to sidecars we have gRPC StoreAPI endpoints and we can create headless services for them: 

`manifests/query/storeapis-service.yaml`{{open}}

So now we can spin up Querier connected to those:

`manifests/query/thanos-query-deployment.yaml`{{open}}

```
kubectl apply -f /root/manifests/query/
```{{execute}}

```
watch -n 5 kubectl get po
```{{execute}}

Let's now query the data from both replicas: [Thanos Query UI](https://[[HOST_SUBDOMAIN]]-30093-[[KATACODA_HOST]].environments.katacoda.com/new/graph?g0.range_input=1h&g0.max_source_resolution=0s&g0.expr=prometheus_tsdb_head_series&g0.tab=0)

## Prometheus Operators supports Thanos Ruler as well!

`manifests/ruler/thanos-ruler.yaml`{{open}}

```
kubectl apply -f /root/manifests/ruler/
```{{execute}}

```
watch -n 5 kubectl get po
```{{execute}}

Let's now see the UI of Ruler: [Thanos Ruler UI](https://[[HOST_SUBDOMAIN]]-30094-[[KATACODA_HOST]].environments.katacoda.com)

### Adding long term storage?

This is as easy as small change to prometheus.yaml:

<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="    # Thanos sidecar will be now included!">    objectStorageConfig:
      key: thanos.yaml
      name: thanos-objstore-config</pre>

And definining `thanos-objstore-config` secret with thanos.yaml` file:

```
type: s3
config:
  bucket: thanos
  endpoint: <objectstorage.endpoint.url>
  access_key: XXX
  secret_key: XXX
```
 
On the way you might want to make the retention much shorter and disable local Prometheus compaction with `disableCompaction` CR option.

With that you can check out other tutorials how to read blocks from object storage using [Store Gateway](https://thanos.io/components/store.md/) (:  

### What if have to stream Prometheus data?

Let's deploy Thanos Receive to receive Prometheus data!

`manifests/receiver/thanos-receive-statefulset.yaml`{{open}}

The important part of it is `hashring.json`, for now harcoded manually:

`manifests/receiver/thanos-receive-configmap.yaml`{{open}}

```
kubectl apply -f /root/manifests/receiver/
```{{execute}}

```
watch -n 5 kubectl get po
```{{execute}}

Now let's configure Prometheus to stream data to receive:

<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="    objectStorageConfig:
      key: thanos.yaml
      name: thanos-objstore-config">  remoteWrite:
    url: thanos-receiver-lb.svc.default.cluster.local:19291</pre>

```
kubectl apply -f /root/manifests/prometheus/prometheus.yaml
```{{execute}}

```
watch -n 5 kubectl get po
```{{execute}}

Let's check what we see in [Thanos Query UI](https://[[HOST_SUBDOMAIN]]-30093-[[KATACODA_HOST]].environments.katacoda.com/new/graph?g0.range_input=1h&g0.max_source_resolution=0s&g0.expr=prometheus_tsdb_head_series&g0.tab=0)
