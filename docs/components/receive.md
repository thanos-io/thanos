---
title: Receiver
type: docs
menu: components
---

# Receiver

The `thanos receive` command implements the [Prometheus Remote Write API](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write). It builds on top of existing Prometheus TSDB and retains their usefulness while extending their functionality with long-term-storage, horizontal scalability, and downsampling. It exposes the StoreAPI so that [Thanos Queriers](./query.md) can query received metrics in real-time. The [Thanos Sidecar](./sidecar.md) is not sufficient for this, as the system would always lag the block length behind (typically 2 hours).

We recommend this component to users who can only push into a Thanos due to air-gapped, or egress only environments. Please note the [various pros and cons of pushing metrics](https://docs.google.com/document/d/1H47v7WfyKkSLMrR8_iku6u9VB73WrVzBHb2SB6dL9_g/edit#heading=h.2v27snv0lsur).

Thanos Receive supports multi-tenancy by using labels. See [Multitenancy documentation here](../operating/multi-tenancy.md).

For more information please check out [initial design proposal](../proposals/201812_thanos-remote-receive.md).
For further information on tuning Prometheus Remote Write [see remote write tuning document](https://prometheus.io/docs/practices/remote_write/).

> NOTE: As the block producer it's important to set correct "external labels" that will identify data block across Thanos clusters. See [external labels](../storage.md#external-labels) docs for details.

# Example

```bash
thanos receive \
    --tsdb.path "/path/to/receive/data/dir" \
    --grpc-address 0.0.0.0:10907 \
    --http-address 0.0.0.0:10909 \
    --receive.replication-factor 1 \
    --label "receive_replica=\"0\"" \
    --label "receive_cluster=\"eu1\"" \
    --receive.local-endpoint 127.0.0.1:10907 \
    --receive.hashrings-file ./data/hashring.json \
    --remote-write.address 0.0.0.0:10908 \
    --objstore.config-file "bucket.yml"
```

The example of `remote_write` Prometheus configuration:

```yaml
remote_write:
- url: http://<thanos-receive-container-ip>:10908/api/v1/receive
```

where `<thanos-receive-containter-ip>` is an IP address reachable by Prometheus Server.

The example content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

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
With such configuration any receive is listens for remote write on `<ip>10908/api/v1/receive` and will forward to correct one in hashring if needed
for tenancy and replication.

## Flags

[embedmd]:# (flags/receive.txt $)
```$
usage: thanos receive [<flags>]

Accept Prometheus remote write API requests and write to local tsdb.

Flags:
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --version                  Show application version.
      --log.level=info           Log filtering level.
      --log.format=logfmt        Log format to use. Possible options: logfmt or
                                 json.
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing configuration.
                                 See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (lower priority). Content of YAML file with
                                 tracing configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-grace-period=2m     Time to wait after an interrupt received for
                                 GRPC Server.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no client
                                 CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --remote-write.address="0.0.0.0:19291"
                                 Address to listen on for remote write requests.
      --remote-write.server-tls-cert=""
                                 TLS Certificate for HTTP server, leave blank to
                                 disable TLS.
      --remote-write.server-tls-key=""
                                 TLS Key for the HTTP server, leave blank to
                                 disable TLS.
      --remote-write.server-tls-client-ca=""
                                 TLS CA to verify clients against. If no client
                                 CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --remote-write.client-tls-cert=""
                                 TLS Certificates to use to identify this client
                                 to the server.
      --remote-write.client-tls-key=""
                                 TLS Key for the client's certificate.
      --remote-write.client-tls-ca=""
                                 TLS CA Certificates to use to verify servers.
      --remote-write.client-server-name=""
                                 Server name to verify the hostname on the
                                 returned TLS certificates. See
                                 https://tools.ietf.org/html/rfc4366#section-3.1
      --tsdb.path="./data"       Data directory of TSDB.
      --label=key="value" ...    External labels to announce. This flag will be
                                 removed in the future when handling multiple
                                 tsdb instances is added.
      --objstore.config-file=<file-path>
                                 Path to YAML file that contains object store
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                                 Alternative to 'objstore.config-file' flag
                                 (lower priority). Content of YAML file that
                                 contains object store configuration. See format
                                 details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --tsdb.retention=15d       How long to retain raw samples on local
                                 storage. 0d - disables this retention.
      --receive.hashrings-file=<path>
                                 Path to file that contains the hashring
                                 configuration. A watcher is initialized to
                                 watch changes and update the hashring
                                 dynamically.
      --receive.hashrings=<content>
                                 Alternative to 'receive.hashrings-file' flag
                                 (lower priority). Content of file that contains
                                 the hashring configuration.
      --receive.hashrings-file-refresh-interval=5m
                                 Refresh interval to re-read the hashring
                                 configuration file. (used as a fallback)
      --receive.local-endpoint=RECEIVE.LOCAL-ENDPOINT
                                 Endpoint of local receive node. Used to
                                 identify the local node in the hashring
                                 configuration.
      --receive.tenant-header="THANOS-TENANT"
                                 HTTP header to determine tenant for write
                                 requests.
      --receive.default-tenant-id="default-tenant"
                                 Default tenant ID to use when none is provided
                                 via a header.
      --receive.tenant-label-name="tenant_id"
                                 Label name through which the tenant will be
                                 announced.
      --receive.replica-header="THANOS-REPLICA"
                                 HTTP header specifying the replica number of a
                                 write request.
      --receive.replication-factor=1
                                 How many times to replicate incoming write
                                 requests.
      --tsdb.allow-overlapping-blocks
                                 Allow overlapping blocks, which in turn enables
                                 vertical compaction and vertical query merge.
      --tsdb.wal-compression     Compress the tsdb WAL.
      --tsdb.no-lockfile         Do not create lockfile in TSDB data directory.
                                 In any case, the lockfiles will be deleted on
                                 next startup.
      --hash-func=               Specify which hash function to use when
                                 calculating the hashes of produced files. If no
                                 function has been specified, it does not
                                 happen. This permits avoiding downloading some
                                 files twice albeit at some performance cost.
                                 Possible values are: "", "SHA256".

```
