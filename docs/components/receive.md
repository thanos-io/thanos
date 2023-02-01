# Receiver

The `thanos receive` command implements the [Prometheus Remote Write API](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write). It builds on top of existing Prometheus TSDB and retains its usefulness while extending its functionality with long-term-storage, horizontal scalability, and downsampling. Prometheus instances are configured to continuously write metrics to it, and then Thanos Receive uploads TSDB blocks to an object storage bucket every 2 hours by default. Thanos Receive exposes the StoreAPI so that [Thanos Queriers](query.md) can query received metrics in real-time.

We recommend this component to users who can only push into a Thanos due to air-gapped, or egress only environments. Please note the [various pros and cons of pushing metrics](https://docs.google.com/document/d/1H47v7WfyKkSLMrR8_iku6u9VB73WrVzBHb2SB6dL9_g/edit#heading=h.2v27snv0lsur).

Thanos Receive supports multi-tenancy by using labels. See [Multi-tenancy documentation here](../operating/multi-tenancy.md).

Thanos Receive supports ingesting [exemplars](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#exemplars) via remote-write. By default, the exemplars are silently discarded as `--tsdb.max-exemplars` is set to `0`. To enable exemplars storage, set the `--tsdb.max-exemplars` flag to a non-zero value. It exposes the ExemplarsAPI so that the [Thanos Queriers](query.md) can query the stored exemplars. Take a look at the documentation for [exemplars storage in Prometheus](https://prometheus.io/docs/prometheus/latest/disabled_features/#exemplars-storage) to know more about it.

For more information please check out [initial design proposal](../proposals-done/201812-thanos-remote-receive.md). For further information on tuning Prometheus Remote Write [see remote write tuning document](https://prometheus.io/docs/practices/remote_write/).

> NOTE: As the block producer it's important to set correct "external labels" that will identify data block across Thanos clusters. See [external labels](../storage.md#external-labels) docs for details.

## TSDB stats

Thanos Receive supports getting TSDB stats using the `/api/v1/status/tsdb` endpoint. Use the `THANOS-TENANT` HTTP header to get stats for individual Tenants. The output format of the endpoint is compatible with [Prometheus API](https://prometheus.io/docs/prometheus/latest/querying/api/#tsdb-stats).

Note that each Thanos Receive will only expose local stats and replicated series will not be included in the response.

## Tenant lifecycle management

Tenants in Receivers are created dynamically and do not need to be provisioned upfront. When a new value is detected in the tenant HTTP header, Receivers will provision and start managing an independent TSDB for that tenant. TSDB blocks that are sent to S3 will contain a unique `tenant_id` label which can be used to compact blocks independently for each tenant.

A Receiver will automatically decommission a tenant once new samples have not been seen for longer than the `--tsdb.retention` period configured for the Receiver. The tenant decommission process includes flushing all in-memory samples for that tenant to disk, sending all unsent blocks to S3, and removing the tenant TSDB from the filesystem. If a tenant receives new samples after being decommissioned, a new TSDB will be created for the tenant.

Note that because of the built-in decommissioning process, the semantic of the `--tsdb.retention` flag in the Receiver is different than the one in Prometheus. For Receivers, `--tsdb.retention=t` indicates that the data for a tenant will be kept for `t` amount of time, whereas in Prometheus, `--tsdb.retention=t` denotes that the last `t` duration of data will be maintained in TSDB. In other words, Prometheus will keep the last `t` duration of data even when it stops getting new samples.

## Example

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

```yaml mdox-exec="go run scripts/cfggen/main.go --name=gcs.Config"
type: GCS
config:
  bucket: ""
  service_account: ""
prefix: ""
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

With such configuration any receive listens for remote write on `<ip>10908/api/v1/receive` and will forward to correct one in hashring if needed for tenancy and replication.

## Limits & gates (experimental)

Thanos Receive has some limits and gates that can be configured to control resource usage. Here's the difference between limits and gates:

- **Limits**: if a request hits any configured limit the client will receive an error response from the server.
- **Gates**: if a request hits a gate without capacity it will wait until the gate's capacity is replenished to be processed. It doesn't trigger an error response from the server.

To configure the gates and limits you can use one of the two options:

- `--receive.limits-config-file=<file-path>`: where `<file-path>` is the path to the YAML file. Any modification to the indicated file will trigger a configuration reload. If the updated configuration is invalid an error will be logged and it won't replace the previous valid configuration.
- `--receive.limits-config=<content>`: where `<content>` is the content of YAML file.

By default all the limits and gates are **disabled**.

### Understanding the configuration file

The configuration file follows a few standards:

1. The value `0` (zero) is used to explicitly define "there is no limit" (infinite limit).
2. In the configuration of default limits (in the `default` section) or global limits (in the `global` section), a value that is not present means "no limit".
3. In the configuration of per tenant limits (in the `tenants` section), a value that is not present means they are the same as the default.

All the configuration for the remote write endpoint of Receive is contained in the `write` key. Inside it there are 3 subsections:

- `global`: limits, gates and/or options that are applied considering all the requests.
- `default`: the default values for limits in case a given tenant doesn't have any specified.
- `tenants`: the limits for a given tenant.

For a Receive instance with configuration like below, it's understood that:

1. The Receive instance has a max concurrency of 30.
2. The Receive instance has head series limiting enabled as it has `meta_monitoring_.*` options in `global`.
3. The Receive instance has some default request limits as well as head series limits that apply of all tenants, **unless** a given tenant has their own limits (i.e. the `acme` tenant and partially for the `ajax` tenant).
4. Tenant `acme` has no request limits, but has a higher head_series limit.
5. Tenant `ajax` has a request series limit of 50000 and samples limit of 500. Their request size bytes limit is inherited from the default, 1024 bytes. Their head series are also inherited from default i.e, 1000.

The next sections explain what each configuration value means.

```yaml mdox-exec="cat pkg/receive/testdata/limits_config/good_limits.yaml"
write:
  global:
    max_concurrency: 30
    meta_monitoring_url: "http://localhost:9090"
    meta_monitoring_limit_query: "sum(prometheus_tsdb_head_series) by (tenant)"
  default:
    request:
      size_bytes_limit: 1024
      series_limit: 1000
      samples_limit: 10
    head_series_limit: 1000
  tenants:
    acme:
      request:
        size_bytes_limit: 0
        series_limit: 0
        samples_limit: 0
      head_series_limit: 2000
    ajax:
      request:
        series_limit: 50000
        samples_limit: 500
```

**IMPORTANT**: this feature is experimental and a work-in-progress. It might change in the near future, i.e. configuration might move to a file (to allow easy configuration of different request limits per tenant) or its structure could change.

### Remote write request limits

Thanos Receive supports setting limits on the incoming remote write request sizes. These limits should help you to prevent a single tenant from being able to send big requests and possibly crash the Receive.

These limits are applied per request and can be configured within the `request` key:

- `size_bytes_limit`: the maximum body size.
- `series_limit`: the maximum amount of series in a single remote write request.
- `samples_limit`: the maximum amount of samples in a single remote write request (summed from all series).

Any request above these limits will cause an 413 HTTP response (*Entity Too Large*) and should not be retried without modifications.

Currently a 413 HTTP response will cause data loss at the client, as none of them (Prometheus included) will break down 413 responses into smaller requests. The recommendation is to monitor these errors in the client and contact the owners of your Receive instance for more information on its configured limits.

Future work that can improve this scenario:

- Proper handling of 413 responses in clients, given Receive can somehow communicate which limit was reached.
- Including in the 413 response which are the current limits that apply to the tenant.

By default, all these limits are disabled.

### Remote write request gates

The available request gates in Thanos Receive can be configured within the `global` key:
- `max_concurrency`: the maximum amount of remote write requests that will be concurrently worked on. Any request request that would exceed this limit will be accepted, but wait until the gate allows it to be processed.

## Active Series Limiting (experimental)

Thanos Receive, in Router or RouterIngestor mode, supports limiting tenant active (head) series to maintain the system's stability. It uses any Prometheus Query API compatible meta-monitoring solution that consumes the metrics exposed by all receivers in the Thanos system. Such query endpoint allows getting the scrape time seconds old number of all active series per tenant, which is then compared with a configured limit before ingesting any tenant's remote write request. In case a tenant has gone above the limit, their remote write requests fail fully.

Every Receive Router/RouterIngestor node, queries meta-monitoring for active series of all tenants, every 15 seconds, and caches the results in a map. This cached result is used to limit all incoming remote write requests.

To use the feature, one should specify the following limiting config options:

Under `global`:
- `meta_monitoring_url`: Specifies Prometheus Query API compatible meta-monitoring endpoint.
- `meta_monitoring_limit_query`: Option to specify PromQL query to execute against meta-monitoring. If not specified it is set to `sum(prometheus_tsdb_head_series) by (tenant)` by default.
- `meta_monitoring_http_client`: Optional YAML field specifying HTTP client config for meta-monitoring.

Under `default` and per `tenant`:
- `head_series_limit`: Specifies the total number of active (head) series for any tenant, across all replicas (including data replication), allowed by Thanos Receive.

NOTE:
- It is possible that Receive ingests more active series than the specified limit, as it relies on meta-monitoring, which may not have the latest data for current number of active series of a tenant at all times.
- Thanos Receive performs best-effort limiting. In case meta-monitoring is down/unreachable, Thanos Receive will not impose limits and only log errors for meta-monitoring being unreachable. Similarly to when one receiver cannot be scraped.
- Support for different limit configuration for different tenants is planned for the future.

## Flags

```$ mdox-exec="thanos receive --help"
usage: thanos receive [<flags>]

Accept Prometheus remote write API requests and write to local tsdb.

Flags:
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-grace-period=2m     Time to wait after an interrupt received for
                                 GRPC Server.
      --grpc-server-max-connection-age=60m
                                 The grpc server max connection age.
                                 This controls how often to re-read the tls
                                 certificates and redo the TLS handshake
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no
                                 client CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --hash-func=               Specify which hash function to use when
                                 calculating the hashes of produced files.
                                 If no function has been specified, it does not
                                 happen. This permits avoiding downloading some
                                 files twice albeit at some performance cost.
                                 Possible values are: "", "SHA256".
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --http.config=""           [EXPERIMENTAL] Path to the configuration file
                                 that can enable TLS or authentication for all
                                 HTTP endpoints.
      --label=key="value" ...    External labels to announce. This flag will be
                                 removed in the future when handling multiple
                                 tsdb instances is added.
      --log.format=logfmt        Log format to use. Possible options: logfmt or
                                 json.
      --log.level=info           Log filtering level.
      --objstore.config=<content>
                                 Alternative to 'objstore.config-file'
                                 flag (mutually exclusive). Content of
                                 YAML file that contains object store
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config-file=<file-path>
                                 Path to YAML file that contains object
                                 store configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --receive.default-tenant-id="default-tenant"
                                 Default tenant ID to use when none is provided
                                 via a header.
      --receive.grpc-compression=snappy
                                 Compression algorithm to use for gRPC requests
                                 to other receivers. Must be one of: snappy,
                                 none
      --receive.hashrings=<content>
                                 Alternative to 'receive.hashrings-file' flag
                                 (lower priority). Content of file that contains
                                 the hashring configuration.
      --receive.hashrings-algorithm=hashmod
                                 The algorithm used when distributing series in
                                 the hashrings. Must be one of hashmod, ketama
      --receive.hashrings-file=<path>
                                 Path to file that contains the hashring
                                 configuration. A watcher is initialized
                                 to watch changes and update the hashring
                                 dynamically.
      --receive.hashrings-file-refresh-interval=5m
                                 Refresh interval to re-read the hashring
                                 configuration file. (used as a fallback)
      --receive.local-endpoint=RECEIVE.LOCAL-ENDPOINT
                                 Endpoint of local receive node. Used to
                                 identify the local node in the hashring
                                 configuration. If it's empty AND hashring
                                 configuration was provided, it means that
                                 receive will run in RoutingOnly mode.
      --receive.relabel-config=<content>
                                 Alternative to 'receive.relabel-config-file'
                                 flag (mutually exclusive). Content of YAML file
                                 that contains relabeling configuration.
      --receive.relabel-config-file=<file-path>
                                 Path to YAML file that contains relabeling
                                 configuration.
      --receive.replica-header="THANOS-REPLICA"
                                 HTTP header specifying the replica number of a
                                 write request.
      --receive.replication-factor=1
                                 How many times to replicate incoming write
                                 requests.
      --receive.tenant-certificate-field=
                                 Use TLS client's certificate field to
                                 determine tenant for write requests.
                                 Must be one of organization, organizationalUnit
                                 or commonName. This setting will cause the
                                 receive.tenant-header flag value to be ignored.
      --receive.tenant-header="THANOS-TENANT"
                                 HTTP header to determine tenant for write
                                 requests.
      --receive.tenant-label-name="tenant_id"
                                 Label name through which the tenant will be
                                 announced.
      --remote-write.address="0.0.0.0:19291"
                                 Address to listen on for remote write requests.
      --remote-write.client-server-name=""
                                 Server name to verify the hostname
                                 on the returned TLS certificates. See
                                 https://tools.ietf.org/html/rfc4366#section-3.1
      --remote-write.client-tls-ca=""
                                 TLS CA Certificates to use to verify servers.
      --remote-write.client-tls-cert=""
                                 TLS Certificates to use to identify this client
                                 to the server.
      --remote-write.client-tls-key=""
                                 TLS Key for the client's certificate.
      --remote-write.server-tls-cert=""
                                 TLS Certificate for HTTP server, leave blank to
                                 disable TLS.
      --remote-write.server-tls-client-ca=""
                                 TLS CA to verify clients against. If no
                                 client CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --remote-write.server-tls-key=""
                                 TLS Key for the HTTP server, leave blank to
                                 disable TLS.
      --request.logging-config=<content>
                                 Alternative to 'request.logging-config-file'
                                 flag (mutually exclusive). Content
                                 of YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --request.logging-config-file=<file-path>
                                 Path to YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (mutually exclusive). Content of YAML file
                                 with tracing configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tsdb.allow-overlapping-blocks
                                 Allow overlapping blocks, which in turn enables
                                 vertical compaction and vertical query merge.
                                 Does not do anything, enabled all the time.
      --tsdb.max-exemplars=0     Enables support for ingesting exemplars and
                                 sets the maximum number of exemplars that will
                                 be stored per tenant. In case the exemplar
                                 storage becomes full (number of stored
                                 exemplars becomes equal to max-exemplars),
                                 ingesting a new exemplar will evict the oldest
                                 exemplar from storage. 0 (or less) value of
                                 this flag disables exemplars storage.
      --tsdb.no-lockfile         Do not create lockfile in TSDB data directory.
                                 In any case, the lockfiles will be deleted on
                                 next startup.
      --tsdb.path="./data"       Data directory of TSDB.
      --tsdb.retention=15d       How long to retain raw samples on local
                                 storage. 0d - disables the retention
                                 policy (i.e. infinite retention).
                                 For more details on how retention is
                                 enforced for individual tenants, please
                                 refer to the Tenant lifecycle management
                                 section in the Receive documentation:
                                 https://thanos.io/tip/components/receive.md/#tenant-lifecycle-management
      --tsdb.wal-compression     Compress the tsdb WAL.
      --version                  Show application version.

```
