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
kubectl get po
```{{execute}}

Let's now query the data from both replicas: [Thanos Query UI](https://[[HOST_SUBDOMAIN]]-30093-[[KATACODA_HOST]].environments.katacoda.com/new/graph?g0.range_input=1h&g0.max_source_resolution=0s&g0.expr=prometheus_tsdb_head_series&g0.tab=0)
