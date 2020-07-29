TBD

## Apply Thanos Receive Controller manifests

```
ls -l /root/manifests/controller
```{{execute}}

## Create Service Account for Thanos Receive Controller

```
kubectl apply -f /root/manifests/controller/thanos-receive-controller-service-account.yaml
```{{execute}}

## Create Thanos Receive Controller resources

```
kubectl apply -f /root/manifests/controller/thanos-receive-controller-deployment.yaml
kubectl apply -f /root/manifests/controller/thanos-receive-controller-service.yaml
```{{execute}}

Controller deployed!
