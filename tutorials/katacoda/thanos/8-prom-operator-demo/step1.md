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

`/root/manifests/crds/monitoring.coreos.com_prometheuses.yaml`{{open}}

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
kubectl get crds
```{{execute}}

CRDs are defined!

## Create Prometheus Operatror

```
kubectl apply -f /root/manifests/operator/
```{{execute}}

```
kubectl get po
```{{execute}}

Prometheus Operator Deployed!

## Deploy 2 replicas of Prometheus through operator

Let's see how Prometheus definition looks like:

`/root/manifests/prometheus/prometheus.yaml`{{open}}

Adding Thanos is as easy as adding two lines:
 
<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="  # Nice, but what about Thanos?">  thanos:
    version: v0.14.0</pre>

```
kubectl apply -f /root/manifests/prometheus/
```{{execute}}

```
kubectl get po
```{{execute}}

Let's see what it scrapes: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30090-[[KATACODA_HOST]].environments.katacoda.com/targets)

Nothing?

## Define what to scrape using Service Monitors

`/root/manifests/svcmonitors/prometheus-service-monitor.yaml`{{open}}

```
kubectl apply -f /root/manifests/svcmonitors/
```{{execute}}

Now we should see targets, and we can query: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30090-[[KATACODA_HOST]].environments.katacoda.com/targets)

## But there are two replicas. How to use them?

Thanks to sidecars we have gRPC endpoints: 

`/root/manifests/query/thanos-query-deployment.yaml`{{open}}