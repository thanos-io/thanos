## It would be nice to just have Prometheus type, right?

```
kubectl get prometheus
```{{execute}}

## Apply Prometheus-Operator Custom Resource Definitions

For example Prometheus CRD you can see here:

`manifests/crds/monitoring.coreos.com_prometheuses.yaml`{{open}}

```
ls -l /root/manifests/crds
```{{execute}}

```
kubectl apply -f /root/manifests/crds/
```{{execute}}
