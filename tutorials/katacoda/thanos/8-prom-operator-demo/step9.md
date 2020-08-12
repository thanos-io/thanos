## Now with two clusters, can I aggregate query across those?

Again, thanks to sidecars we have gRPC StoreAPI endpoints and we can create headless services for US1 namespace as well! 

`manifests/storeapius1/thanos-query-service-storeapi.yaml`{{open}}

As you can see, querier should already have `dnssrv+_grpc._tcp.prometheus-storeapi.us1.svc.cluster.local` route configured:

`manifests/query/thanos-query-deployment.yaml`{{open}}

Let's deploy our cross-namespace service:

```
kubectl apply -f /root/manifests/storeapius1/
```{{execute}}

```
kubectl -n us1 get services
```{{execute}}

Let's now query the data from both NAMESPACES, each with 2 Prometheus replicas: [Thanos Query UI](https://[[HOST_SUBDOMAIN]]-30093-[[KATACODA_HOST]].environments.katacoda.com/new/graph?g0.range_input=1h&g0.max_source_resolution=0s&g0.expr=prometheus_tsdb_head_series&g0.tab=0)
