To deploy thanos sidecar along with Prometheus using [community helm chart](https://prometheus-community.github.io/helm-charts) - just run the next command, putting the values to a file `values.yaml` and changing `--namespace` value beforehand:

```
helm upgrade --version="25.11.0" --install --namespace="my-lovely-namespace" --values values.yaml  prometheus-thanos-sidecar prometheus-community/prometheus
```

Please note that you need to replace the two placeholders in the values: `BUCKET_REPLACE_ME` and `CLUSTER_NAME`.
It's also mandatory to create a secret for the sidecar to access the object storage bucket.
In the example below, `GCS` is used and for that a secret with name `thanos-storage-secret` got manually created and used in here.

A more detailed documentation about the different values can be found on the [helm chart repository](https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus).

An example of the `values.yaml` file:
```yaml
rbac:
  create: true

alertmanager:
  enabled: false

prometheus-pushgateway:
  enabled: false

prometheus-node-exporter:
  enabled: false

kube-state-metrics:
  enabled: false

initChownData:
  resources:
    limits:
      memory: 16Mi
      cpu: 50m
    requests:
      memory: 16Mi
      cpu: 50m

server:
  extraArgs:
    log.level: debug
    storage.tsdb.min-block-duration: 2h # Don't change this, see docs/components/sidecar.md
    storage.tsdb.max-block-duration: 2h # Don't change this, see docs/components/sidecar.md
  retention: 4h
  service:
    annotations:
      prometheus.io/scrape: "true"
      prometheus.io/port: "9090"
  statefulSet:
    enabled: true
  podAnnotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "10902"
  sidecarContainers:
    thanos-sidecar:
      image: quay.io/thanos/thanos:v0.34.0
      resources:
        requests:
          memory: "512Mi"
      env:
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: /etc/secret/sa
      args:
        - "sidecar"
        - "--log.level=debug"
        - "--tsdb.path=/data/"
        - "--prometheus.url=http://127.0.0.1:9090"
        - "--objstore.config={type: GCS, config: {bucket: BUCKET_REPLACE_ME}}"
        - "--reloader.config-file=/etc/prometheus-config/prometheus.yml"
        - "--reloader.config-envsubst-file=/etc/prometheus-shared/prometheus.yml"
        - "--reloader.rule-dir=/etc/prometheus-config/rules"
      ports:
        - name: sidecar-http
          containerPort: 10902
        - name: grpc
          containerPort: 10901
        - name: cluster
          containerPort: 10900
      volumeMounts:
        - name: storage-volume
          mountPath: /data
        - name: thanos-storage-secret
          mountPath: /etc/secret
        - name: config-volume
          mountPath: /etc/prometheus-config
          readOnly: false
        - name: prometheus-config-shared
          mountPath: /etc/prometheus-shared/
          readOnly: false
  configPath: /etc/prometheus-shared/prometheus.yml
  replicaCount: 1
  persistentVolume:
    size: 20Gi
  extraVolumes: # spec.template.spec.volumes
    - name: prometheus-config-shared
      emptyDir: {}
  extraVolumeMounts: # spec.template.spec.containers.volumeMounts for prometheus container
    - name: prometheus-config-shared
      mountPath: /etc/prometheus-shared/
  resources:
    requests:
      memory: 1Gi
  global:
    scrape_interval: 5s
    scrape_timeout: 4s
    external_labels:
      prometheus_group: CLUSTER_NAME
      prometheus_replica: '$(HOSTNAME)'
    evaluation_interval: 5s
  extraSecretMounts:
    - name: thanos-storage-secret
      mountPath: /etc/secret/
      subPath: sa
      readOnly: false
      secretName: thanos-storage-secret

# as thanos sidecar is taking care of the config reload
# we can disable the prometheus configmap reload
configmapReload:
  prometheus:
    enabled: false

serverFiles:
  alerts: {}
  rules: {}
```
