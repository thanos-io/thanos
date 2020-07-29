---
apiVersion: monitoring.coreos.com/v1
kind: ThanosRuler
metadata:
  name: thanos-ruler
  namespace: default
  labels:
    app: thanos-ruler
spec:
  image: quay.io/thanos/thanos:v0.11.0
  ruleSelector:
    matchLabels:
      role: thanos-example
  queryEndpoints:
  - dnssrv+_http._tcp.thanos-query.default.svc.cluster.local
---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  creationTimestamp: null
  labels:
    prometheus: example-alert
    role: thanos-example
  name: prometheus-example-alerts
  namespace: default
spec:
  groups:
  - name: ./example-alert.rules
    rules:
    - alert: ExampleAlert
      expr: vector(1)
