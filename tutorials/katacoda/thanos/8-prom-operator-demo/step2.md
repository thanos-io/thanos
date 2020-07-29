TBD

## Create Roles for Prometheus

```
kubectl apply -f /root/manifests/prometheus/prometheus-role.yaml
kubectl apply -f /root/manifests/prometheus/prometheus-role-binding.yaml
```{{execute}}

## Create Prometheus and its Service

```
kubectl apply -f /root/manifests/prometheus/prometheus.yaml
kubectl apply -f /root/manifests/prometheus/prometheus-service.yaml
```{{execute}}

## Create a Service Monitor to scrape Prometheus

```
kubectl apply -f /root/manifests/prometheus/prometheus-service-monitor.yaml
```{{execute}}


Prometheus Deployed!
