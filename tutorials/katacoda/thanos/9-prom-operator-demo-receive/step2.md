TBD

## Apply Thanos Receive Controller manifests

```
ls -l /root/manifests/controller
```{{execute}}

```
kubectl apply -f /root/manifests/controller/
```{{execute}}

## Let's test them out!

```
kubectl get deployments -l app.kubernetes.io/name=thanos-receive-controller
```{{execute}}

Controller deployed!
