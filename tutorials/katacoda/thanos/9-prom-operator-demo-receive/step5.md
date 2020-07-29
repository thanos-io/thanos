TBD

## Create Thanos Query Resources

```
kubectl apply -f /root/manifests/query
```{{execute}}

## Let's test them out!

```
kubectl get deployments -l app.kubernetes.io/name=thanos-query
```{{execute}}

Thanos Query deployed!

Check [Querier UI `Store` page](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/stores).

Thanks!
