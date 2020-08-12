## This is nice... but if we have more clusters? 

Let's simulate we have another cluster, by using `us1` namespace.

```
kubectl create ns us1
```{{execute}}

Let's deploy similar stack to us1:

```
kubectl apply -f /root/manifests/us1/
```{{execute}}

We should be able to see the stack starting up:

```
kubectl -n us1 get po
```{{execute}}

And after a while, Prometheus UI being up: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30190-[[KATACODA_HOST]].environments.katacoda.com/new/targets)