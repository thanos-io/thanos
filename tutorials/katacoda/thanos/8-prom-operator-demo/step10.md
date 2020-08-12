## Prometheus Operators supports Thanos Ruler as well!

`manifests/ruler/thanos-ruler.yaml`{{open}}

```
kubectl apply -f /root/manifests/ruler/
```{{execute}}

```
kubectl get po
```{{execute}}

Let's now see the UI of Ruler: [Thanos Ruler UI](https://[[HOST_SUBDOMAIN]]-30094-[[KATACODA_HOST]].environments.katacoda.com)
