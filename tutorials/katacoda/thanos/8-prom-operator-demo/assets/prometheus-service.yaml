apiVersion: v1
kind: Service
metadata:
  labels:
    app: prometheus
    prometheus: self
  name: prometheus-self
  namespace: default
spec:
  ports:
  - name: web
    port: 9090
    protocol: TCP
    targetPort: web
  selector:
    prometheus: self
