apiVersion: apps/v1
kind: Deployment
metadata:
  name: thanos-query
  namespace: default
  labels:
    app: thanos-query
spec:
  replicas: 1
  selector:
    matchLabels:
      app: thanos-query
  template:
    metadata:
      labels:
        app: thanos-query
    spec:
      containers:
      - name: thanos-query
        image: quay.io/thanos/thanos:v0.11.0
        args:
        - query
        - --log.level=debug
        - --query.replica-label=prometheus_replica
        - --query.replica-label=thanos_ruler_replica
        - --store=dnssrv+_grpc._tcp.thanos-sidecar.default.svc.cluster.local
        - --store=dnssrv+_grpc._tcp.thanos-ruler.default.svc.cluster.local
        ports:
        - name: http
          containerPort: 10902
        - name: grpc
          containerPort: 10901
