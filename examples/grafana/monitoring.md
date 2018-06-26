# Grafana Dashboards

There are 4 Grafana dashboards targeted for environments running Kubernetes:

- [Thanos Compact](thanos-compact.json)
- [Thanos Query](thanos-query.json)
- [Thanos Store](thanos-store.json)
- [Thanos Sidecar](thanos-sidecar.json)

You can import them via `Import -> Paste JSON` in Grafana.
These dashboards require Grafana 5, importing them in older versions are known not to work.

# Configuration

All dashboards can be configured via `labelselector` and `labelvalue` constants, which are used to pinpoint Thanos components.

Let's say we have a service configured with following annotation:

```
apiVersion: v1
kind: Service
metadata:
  annotations:
    prometheus.io/path: /metrics
    prometheus.io/port: "10902"
    prometheus.io/scrape: "true"
  labels:
    name: prometheus
  name: prometheus
spec:
  ports:
  - name: prometheus
    port: 9090
    protocol: TCP
    targetPort: 9090
  selector:
    app: prometheus
```

In this case `labelselector` should be `name` and `labelvalue` should be `prometheus` as metrics will have `name="prometheus"` label associated with them.

