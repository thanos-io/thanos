---
title: Running Thanos using Envoy to proxy connections to external sidecars
type: docs
menu: operating
---

# Running Thanos using Envoy to proxy connections to external sidecars

If want to run Thanos within a cluster, allowing insecure connections to other Thanos components, while simultaneously allowing secure (TLS) connections to remote sidecars, you will need to setup a proxy server.     

Envoy can be implemented with a sidecar container (example shown here) within the Thanos Querier pod. It will perform TLS origination to connect to secure remote sidecars while forwarding their communications unencrypted back to Thanos Querier (within the pod).

## Envoy

Envoy is a proxy server that has good HTTP2 and gRPC support and is relatively straightforward to configure for this purpose.

### Scenario

You have an "Observer Cluster" that is hosting Thanos Querier along with Thanos Storegateway you also have a Thanos Sidecar that you would like to connect to within the cluster.

However you also need to connect from the querier to several remote instances of Thanos Sidecar within a cluster (The following example configs assume the use of NGINX Ingress in the remote cluster).

Of course you want to use TLS to encrypt the connection to the remote clusters, but you don't want to use TLS within the cluster (to reduce no. of ingress, provisioning certificates, custom helm charts as most don't expose cert config for components other than querier etc.)

In this scenario you need to use a proxy as described above.  You will need to do the following steps:

 - Configure an envoy sidecar container to the Thanos Querier pod (unfortunately this also isn't supported by a lot of Thanos charts) an example pod config is below (see `deployment.yaml`)
 - Make sure that the envoy sidecar has the correct certificates (using a mounted secret) and a valid configuration (using a mounted configmap) an example envoy config is below (`envoy.yaml`)
 - Configure a service for the envoy sidecar an example service is shown below (`service.yaml`) you may have another service already for local cluster access (for Thanos Ruler or Grafana etc.)
 - Point the querier at the service and the correct port an example `--store ` field is below (`thanos-querier args`)
 - Make sure your remote cluster has TLS setup and an appropriate HTTP2 supported ingress, example below `ingress.yaml`

## `deployment.yaml`

- `[port_name]` is the name of the port specified within the service (see `service.yaml`)
- `[service-name]` is the name of the envoy service
- `[namespace]` is the name of the envoy service namespace

You may need to change cluster.local depending on your cluster domain.  
The `--store` entries for thanos storegateway etc. may be named different in your setup
```
kind: Deployment
apiVersion: apps/v1
metadata:
  name: thanos-global-test-querier
  namespace: thanos-global
  labels:
    name: thanos-global-test-querier
  replicas: 2
  selector:
    matchLabels:
      name: thanos-global-test-querier
  template:
    metadata:
      labels:
        name: thanos-global-test-querier
    spec:
      volumes:
        - name: config
          configMap:
            name: thanos-global-test-envoy-config
            defaultMode: 420
            optional: false
        - name: certs
          secret:
            secretName: thanos-global-test-envoy-certs
            defaultMode: 420
            optional: false
      containers:
        - name: querier
          image: 'thanosio/thanos:v0.15.0'
          args:
            - query
            - '--log.level=info'
            - '--grpc-address=0.0.0.0:10901'
            - '--http-address=0.0.0.0:10902'
            - '--query.replica-label=replica'
            - >-
              --store=dnssrv+_grpc._tcp.thanos-global-test-storegateway.thanos-global.svc.cluster.local
            - >-
              --store=dnssrv+_grpc._tcp.thanos-global-test-sidecar.thanos-global.svc.cluster.local
            - >-
              --store=dnssrv+_grpc._tcp.thanos-global-test-ruler.thanos-global.svc.cluster.local
            - >-
              --store=dnssrv+_[port_name]._tcp.[service-name].[namespace].svc.cluster.local
            - >-
              --store=dnssrv+_[port_name_2]._tcp.[service-name].[namespace].svc.cluster.local
          ports:
            - name: http
              containerPort: 10902
              protocol: TCP
            - name: grpc
              containerPort: 10901
              protocol: TCP
          resources: {}
          livenessProbe:
            httpGet:
              path: /-/healthy
              port: http
              scheme: HTTP
            initialDelaySeconds: 30
            timeoutSeconds: 1
            periodSeconds: 30
            successThreshold: 1
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /-/ready
              port: http
              scheme: HTTP
            initialDelaySeconds: 30
            timeoutSeconds: 1
            periodSeconds: 30
            successThreshold: 1
            failureThreshold: 3
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
          securityContext:
            privileged: false
            runAsUser: 1001
            runAsGroup: 0
            runAsNonRoot: false
            readOnlyRootFilesystem: false
            allowPrivilegeEscalation: true
        - name: envoy-sidecar
          image: 'envoyproxy/envoy:v1.16.0'
          args:
            - '-c'
            - /config/envoy.yaml
            - '-l'
            - debug
          ports:
            - name: [port_name]
              containerPort: 10000
              protocol: TCP
            - name: [port_name_2]
              containerPort: 10001
              protocol: TCP
          resources: {}
          volumeMounts:
            - name: config
              mountPath: /config
              mountPropagation: None
            - name: certs
              mountPath: /certs
              mountPropagation: None
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
      dnsPolicy: ClusterFirst
      serviceAccountName: thanos-global-test-querier-sa
      serviceAccount: thanos-global-test-querier-sa
      automountServiceAccountToken: false
      shareProcessNamespace: false
      securityContext: {}
      schedulerName: default-scheduler
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 25%
      maxSurge: 25%
  revisionHistoryLimit: 10
  progressDeadlineSeconds: 600
```

## `envoy.yaml`
- This is a static envoy configuration
- You will need to update this for every sidecar you would like to talk to (or you can look into envoy dynamic configuration `XDS` etc.) or simpler get something else to generate the static configuration
  - For example you could use Terraform `templatefile()` to generate the envoy configuration using `for` loops etc.

```
admin:
  access_log_path: /tmp/admin_access.log
  address:
    socket_address: { address: 0.0.0.0, port_value: 9901 }
​
static_resources:
  listeners:
  - name: sidecar_name
    address:
      socket_address: { address: 0.0.0.0, port_value: 10000 }
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        config:
          codec_type: auto
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: local_service
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: sidecar_name, host_rewrite: thanos.sidecardomain.com }
          http_filters:
          - name: envoy.router
  - name: sidecar_name_2
    address:
      socket_address: { address: 0.0.0.0, port_value: 10001 }
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        config:
          codec_type: auto
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: local_service
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: sidecar_name_2, host_rewrite: thanos.sidecardomain.com }
          http_filters:
          - name: envoy.router
  clusters:
  - name: sidecar_name
    connect_timeout: 30s
    type: logical_dns
    http2_protocol_options: {}
    dns_lookup_family: V4_ONLY
    lb_policy: round_robin
    hosts: [{ socket_address: { address: thanos.sidecardomain.com, port_value: 443 }}]
    tls_context:
      common_tls_context:
        validation_context:
          trusted_ca:
            filename: /certs/ca.crt
        alpn_protocols:
        - h2
        - http/1.1
      sni: thanos.sidecardomain.com
  - name: sidecar_name_2
    connect_timeout: 30s
    type: logical_dns
    http2_protocol_options: {}
    dns_lookup_family: V4_ONLY
    lb_policy: round_robin
    hosts: [{ socket_address: { address: thanos-2.sidecardomain.com, port_value: 443 }}]
    tls_context:
      common_tls_context:
        validation_context:
          trusted_ca:
            filename: /certs/ca.crt
        alpn_protocols:
        - h2
        - http/1.1
      sni: thanos-2.sidecardomain.com
```


## `service.yaml`
- This is the service for the envoy sidecar
- You will need to define a new port for every sidecar you would like to add

```
kind: Service
apiVersion: v1
metadata:
  name: thanos-global-test-envoy
  namespace: thanos-global
  labels:
    name: thanos-global-test-envoy
spec:
  ports:
    - name: [port_name]
      protocol: TCP
      port: 10000
      targetPort: 10000
    - name: [port_name_2]
      protocol: TCP
      port: 10001
      targetPort: 10001
  selector:
    name: thanos-global-test-querier
  type: ClusterIP
  sessionAffinity: None
```

## `ingress.yaml`
- This is an example ingress for a remote sidecar using NGINX ingress
- You must use TLS (limitation from NGINX) as HTTP2 is only support on a separate listener
- You must have certs configured and the CA added into the envoy sidecar earlier to allow verification.

```
kind: Ingress
apiVersion: extensions/v1beta1
metadata:
  name: monitoring-rancher-monitor-thanos-gateway
  namespace: monitoring
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: GRPC
    nginx.ingress.kubernetes.io/force-ssl-redirect: 'true'
    nginx.ingress.kubernetes.io/grpc-backend: 'true'
    nginx.ingress.kubernetes.io/protocol: h2c
    nginx.ingress.kubernetes.io/proxy-read-timeout: '160'
spec:
  tls:
    - hosts:
        - thanos.sidecardomain.com
      secretName: thanos-tls
  rules:
    - host: thanos.sidecardomain.com
      http:
        paths:
          - path: /
            backend:
              serviceName: monitoring-rancher-monitor-prometheus
              servicePort: 10901
```
