## Define what to scrape using Service Monitors

`manifests/svcmonitors/prometheus-service-monitor.yaml`{{open}}

```
kubectl apply -f /root/manifests/svcmonitors/
```{{execute}}

Now we should see targets, and we can query: [Prometheus UI](https://[[HOST_SUBDOMAIN]]-30090-[[KATACODA_HOST]].environments.katacoda.com/new/targets)
