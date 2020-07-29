TBD

## Create a Thanos Sidecar service to make Sidecars discoverable

```
kubectl apply -f /root/manifests/sidecar
```{{execute}}

## Create Thanos Ruler

```
kubectl apply -f /root/manifests/ruler
```{{execute}}

## Create Thanos Query

```
kubectl apply -f /root/manifests/query
```{{execute}}

Thanks Deployed!
