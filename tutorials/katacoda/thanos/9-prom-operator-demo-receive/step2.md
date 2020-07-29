TBD

## Apply Thanos Receive Controller manifests

```
ls -l /root/manifests/controller
```{{execute}}

## Create Roles for Prometheus Operator

```
kubectl apply -f /root/manifests/controller/thanos-receive-controller-role.yaml
kubectl apply -f /root/manifests/controller/thanos-receive-controller-role-binding.yaml
```{{execute}}

## Create Service Account for Thanos Receive Controller

```
kubectl apply -f /root/manifests/controller/thanos-receive-controller-service-account.yaml
```{{execute}}

## Create Thanos Receive Controller resources

```
kubectl apply -f /root/manifests/controller/thanos-receive-controller-deployment.yaml
kubectl apply -f /root/manifests/controller/thanos-receive-controller-service.yaml
kubectl apply -f /root/manifests/controller/thanos-receive-controller-configmap.yaml
```{{execute}}

## Create a Service Monitor to scrape Thanos Receive Controller

```
kubectl apply -f /root/manifests/controller/thanos-receive-controller-service-monitor.yaml
```{{execute}}

Controller deployed!
