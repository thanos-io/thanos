apiVersion: v1
kind: Service
metadata:
  name: thanos-query
  namespace: default
  labels:
    app: thanos-query
spec:
  selector:
    app: thanos-query
  ports:
  - name: http
    port: 9090
    targetPort: http
