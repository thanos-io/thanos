## Define what to scrape using Service Monitors

`manifests/svcmonitors/prometheus-operator-service-monitor.yaml`{{open}}

You can see what ServiceMonitor resource can include [here](https://github.com/prometheus-operator/prometheus-operator/blob/v0.40.0/Documentation/api.md#servicemonitor).

```
kubectl apply -f /root/manifests/svcmonitors/
```{{execute}}

Now we should see some targets, and we can query: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30090-[[KATACODA_HOST]].environments.katacoda.com/new/targets)
