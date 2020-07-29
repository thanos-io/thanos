TBD

## Let's deploy as Minio Object Store

```
ls -l /root/manifests/minio
```{{execute}}

```
kubectl apply -f /root/manifests/minio/
```{{execute}}

## Let's test them out!

```
kubectl get statefulsets -l app.kubernetes.io/name=thanos-receive
```{{execute}}

Minio deployed!

## Apply Thanos Receive manifests

```
ls -l /root/manifests/receive
```{{execute}}

```
kubectl apply -f /root/manifests/receive/
```{{execute}}

## Let's test them out!

```
kubectl get statefulsets -l app.kubernetes.io/name=thanos-receive
```{{execute}}

Receive deployed!
