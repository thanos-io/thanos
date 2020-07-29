apiVersion: v1
kind: Service
metadata:
  name: thanos-sidecar
  namespace: default
  labels:
    app: thanos-sidecar
spec:
  clusterIP: None
  selector:
    prometheus: self
  ports:
  - name: grpc
    port: 10901
    targetPort: grpc
