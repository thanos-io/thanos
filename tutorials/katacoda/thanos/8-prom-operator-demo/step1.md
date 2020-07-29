At Red Hat we help to maintain many open source projects.

One of such project is the https://github.com/coreos/prometheus-operator, first project that
started leveraging CRDs to operate stateful resources like Prometheus. 

Thanos, while mostly built from stateless components, is well integrated with Prometheus Operator.

This quick demo will show you quickly how to deploy Prometheuses with Thanos with seamless HA support. 

TODO: Probably split it each section to each step...

## It would be nice to just have Prometheus type, right?

```
kubectl get prometheus
```{{execute}}

## Apply Prometheus-Operator Custom Resource Definitions

```
/root/manifests/crds/monitoring.coreos.com_prometheuses.yaml
```{{open}}

```
ls -l /root/manifests/crds
```{{execute}}

```
kubectl apply -f /root/manifests/crds/
```{{execute}}

## Let's test them out!

```
kubectl get prometheus
```{{execute}}

CRDs are defined!

## Create Roles for Prometheus Operator

```
kubectl apply -f /root/manifests/operator/prometheus-operator-cluster-role.yaml
kubectl apply -f /root/manifests/operator/prometheus-operator-cluster-role-binding.yaml
```{{execute}}

## Create Service Account for Prometheus Operator

```
kubectl apply -f /root/manifests/operator/prometheus-operator-service-account.yaml
```{{execute}}

## Create Prometheus Operator resources

```
kubectl apply -f /root/manifests/operator/prometheus-operator-deployment.yaml
kubectl apply -f /root/manifests/operator/prometheus-operator-service.yaml
```{{execute}}

## Create a Service Monitor to scrape Prometheus Operator

```
kubectl apply -f /root/manifests/operator/prometheus-operator-service-monitor.yaml
```{{execute}}

Prometheus Operator Deployed!
