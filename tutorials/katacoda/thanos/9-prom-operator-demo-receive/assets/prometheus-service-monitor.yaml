apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: prometheus
  namespace: default
  labels:
    app: prometheus
spec:
  selector:
    matchLabels:
      app: prometheus
  endpoints:
  - port: web
    interval: 30s
