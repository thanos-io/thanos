---
title: Receive
type: docs
menu: components
---

# Receive

The receive component of Thanos implements the [Prometheus remote endpoint](https://prometheus.io/docs/operating/integrations/#remote-endpoints-and-storage) functionality.
It also implements the Store API, which allows being queried by Thanos query component.
In addition it allows writing to longterm Objectstorage, which in return is accessible to Thanos query component through Thanos store.

In general the remote write functionality is the better way of propagating Prometheus data to a different store 
and should be preferred over [Prometheus federation](https://prometheus.io/docs/prometheus/latest/federation),
especially when federation would return a lot of metrics.

## Load Distribution and Tenancy

Thanos receive provides tenancy features. This allows having multiple tenants evenly distributed on the same setup.
Tenancy in Thanos receive component is achieved by hashrings initially configured in json format.

## Simple HA Setup

The simplest HA setup is achieved by having two Thanos receive instances.
The Prometheus instance is then configured to remote write to both of these Thanos receive endpoints.
There's no need for the hashring functionality in this simple and zero-tenants setup.

Excerpt from the `prometheus.yml` configuration file:
```yaml
remote_write:
  - url: http://thanos1.example:19291/api/v1/receive
  - url: http://thanos2.example:19291/api/v1/receive
```

## Flags

```$
usage: thanos receive [<flags>]

Accept Prometheus remote write API requests and write to local tsdb (EXPERIMENTAL, this may change drastically without notice)

Flags:
  -h, --help                     Show context-sensitive help (also try --help-long and --help-man).
      --version                  Show application version.
      --log.level=info           Log filtering level.
      --log.format=logfmt        Log format to use.
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing configuration. See format details: https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag (lower priority). Content of YAML file with tracing configuration. See format details:
                                 https://thanos.io/tracing.md/#configuration
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for HTTP Server.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components.
      --grpc-grace-period=2m     Time to wait after an interrupt received for GRPC Server.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to disable TLS
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)
      --remote-write.address="0.0.0.0:19291"
                                 Address to listen on for remote write requests.
      --remote-write.server-tls-cert=""
                                 TLS Certificate for HTTP server, leave blank to disable TLS
      --remote-write.server-tls-key=""
                                 TLS Key for the HTTP server, leave blank to disable TLS
      --remote-write.server-tls-client-ca=""
                                 TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)
      --remote-write.client-tls-cert=""
                                 TLS Certificates to use to identify this client to the server
      --remote-write.client-tls-key=""
                                 TLS Key for the client's certificate
      --remote-write.client-tls-ca=""
                                 TLS CA Certificates to use to verify servers
      --remote-write.client-server-name=""
                                 Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1
      --tsdb.path="./data"       Data directory of TSDB.
      --label=key="value" ...    External labels to announce. This flag will be removed in the future when handling multiple tsdb instances is added.
      --objstore.config-file=<file-path>
                                 Path to YAML file that contains object store configuration. See format details: https://thanos.io/storage.md/#configuration
      --objstore.config=<content>
                                 Alternative to 'objstore.config-file' flag (lower priority). Content of YAML file that contains object store configuration. See format details:
                                 https://thanos.io/storage.md/#configuration
      --tsdb.retention=15d       How long to retain raw samples on local storage. 0d - disables this retention
      --receive.hashrings-file=<path>
                                 Path to file that contains the hashring configuration.
      --receive.hashrings-file-refresh-interval=5m
                                 Refresh interval to re-read the hashring configuration file. (used as a fallback)
      --receive.local-endpoint=RECEIVE.LOCAL-ENDPOINT
                                 Endpoint of local receive node. Used to identify the local node in the hashring configuration.
      --receive.tenant-header="THANOS-TENANT"
                                 HTTP header to determine tenant for write requests.
      --receive.replica-header="THANOS-REPLICA"
                                 HTTP header specifying the replica number of a write request.
      --receive.replication-factor=1
                                 How many times to replicate incoming write requests.
```
