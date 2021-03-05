---
title: Receive-route
type: docs
menu: components
---

# Receive-route

The `thanos receive-route` command implements the [Prometheus Remote Write API](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write). It receives metrics from the target and forwards them to the right [receiver](./receive.md) in the hashring. It also buffers the incoming requests and handles changes in hashring configurations.
We recommend this component to users who can only push into a Thanos due to air-gapped, or egress only environments. Please note the [various pros and cons of pushing metrics](https://docs.google.com/document/d/1H47v7WfyKkSLMrR8_iku6u9VB73WrVzBHb2SB6dL9_g/edit#heading=h.2v27snv0lsur).

For further information on tuning Prometheus Remote Write [see remote write tuning document](https://prometheus.io/docs/practices/remote_write/).

# Example

```bash
thanos receive-route \
    --http-address 0.0.0.0:10909 \
    --receive.replication-factor 1 \
    --receive.hashrings-file ./data/hashring.json \
    --remote-write.address 0.0.0.0:10908 \
```

The example of `remote_write` Prometheus configuration:

```yaml
remote_write:
- url: http://<thanos-receive-container-ip>:10908/api/v1/receive
```

where `<thanos-receive-containter-ip>` is an IP address reachable by Prometheus Server.

The example content of `hashring.json`:

```json
[
    {
        "endpoints": [
            "127.0.0.1:10907",
            "127.0.0.1:11907",
            "127.0.0.1:12907"
        ]
    }
]
```
With such configuration, router listens for remote write on `<ip>10908/api/v1/receive` and will forward to the correct receiver in hashring
for tenancy and replication.

## Flags

[embedmd]:# (flags/receive.txt $)
```$
usage: thanos receive-route [<flags>]

Accept Prometheus remote write API requests and forward them to a receive node.

Flags:
  -h, --help                  Show context-sensitive help (also try --help-long
                              and --help-man).
      --version               Show application version.
      --log.level=info        Log filtering level.
      --log.format=logfmt     Log format to use. Possible options: logfmt or
                              json.
      --tracing.config-file=<file-path>
                              Path to YAML file with tracing configuration. See
                              format details:
                              https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config=<content>
                              Alternative to 'tracing.config-file' flag (lower
                              priority). Content of YAML file with tracing
                              configuration. See format details:
                              https://thanos.io/tip/thanos/tracing.md/#configuration
      --http-address="0.0.0.0:10902"
                              Listen host:port for HTTP endpoints.
      --http-grace-period=2m  Time to wait after an interrupt received for HTTP
                              Server.
      --remote-write.address="0.0.0.0:19291"
                              Address to listen on for remote write requests.
      --remote-write.server-tls-cert=""
                              TLS Certificate for HTTP server, leave blank to
                              disable TLS.
      --remote-write.server-tls-key=""
                              TLS Key for the HTTP server, leave blank to
                              disable TLS.
      --remote-write.server-tls-client-ca=""
                              TLS CA to verify clients against. If no client CA
                              is specified, there is no client verification on
                              server side. (tls.NoClientCert)
      --remote-write.client-tls-cert=""
                              TLS Certificates to use to identify this client to
                              the server.
      --remote-write.client-tls-key=""
                              TLS Key for the client's certificate.
      --remote-write.client-tls-ca=""
                              TLS CA Certificates to use to verify servers.
      --remote-write.client-server-name=""
                              Server name to verify the hostname on the returned
                              gRPC certificates. See
                              https://tools.ietf.org/html/rfc4366#section-3.1
      --receive.hashrings-file=<path>
                              Path to file that contains the hashring
                              configuration.
      --receive.hashrings-file-refresh-interval=5m
                              Refresh interval to re-read the hashring
                              configuration file. (used as a fallback)
      --receive.local-endpoint=RECEIVE.LOCAL-ENDPOINT
                              Endpoint of local receive node. Used to identify
                              the local node in the hashring configuration.
      --receive.tenant-header="THANOS-TENANT"
                              HTTP header to determine tenant for write
                              requests.
      --receive.default-tenant-id="default-tenant"
                              Default tenant ID to use when none is provided via
                              a header.
      --receive.replication-factor=1
                              How many times to replicate incoming write
                              requests.

```


