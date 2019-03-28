# Query

The query component implements the Prometheus HTTP v1 API to query data in a Thanos cluster via PromQL.

It gathers the data needed to evaluate the query from underlying StoreAPIs. See [here](/docs/service_discovery.md)
on how to connect querier with desired StoreAPIs.

Querier currently is fully stateless and horizontally scalable.

```
$ thanos query \
    --http-address     "0.0.0.0:9090" \
    --store            "<store-api>:<grpc-port>" \
    --store            "<store-api2>:<grpc-port>" \
```

## Deduplication

The query layer can deduplicate series that were collected from high-availability pairs of data sources such as Prometheus.
A fixed replica label must be chosen for the entire cluster and can then be passed to query nodes on startup.

Two or more series that have that are only distinguished by the given replica label, will be merged into a single time series.
This also hides gaps in collection of a single data source. For example:

* Prometheus + sidecar "A": `cluster=1,env=2,replica=A`
* Prometheus + sidecar "B": `cluster=1,env=2,replica=B`
* Prometheus + sidecar "A" in different cluster: `cluster=2,env=2,replica=A`

If we configure Querier like this:

```
$ thanos query \
    --http-address        "0.0.0.0:9090" \
    --query.replica-label "replica" \
    --store               "<store-api>:<grpc-port>" \
    --store               "<store-api2>:<grpc-port>" \
```

And we query for metric `up{job="prometheus",env="2"}` with this option we will get 2 results:

  * `up{job="prometheus",env="2",cluster="1"} 1`
  * `up{job="prometheus",env="2",cluster="2"} 1`

WITHOUT this replica flag (so deduplication turned off), we will get 3 results:

  * `up{job="prometheus",env="2",cluster="1",replica="A"} 1`
  * `up{job="prometheus",env="2",cluster="1",replica="B"} 1`
  * `up{job="prometheus",env="2",cluster="2",replica="A"} 1`

This logic can also be controlled via parameter on QueryAPI. More details below.

## Query API

Overall QueryAPI exposed by Thanos is guaranteed to be compatible with Prometheus 2.x.

However, for additional Thanos features, Thanos, on top of Prometheus adds several
additional parameters listed below as well as custom response fields.

### Deduplication Enabled

| HTTP URL/FORM parameter | Type | Default | Example |
|----|----|----|----|
| `dedup` | `Boolean` | True, but effect depends on `query.replica` configuration flag. | `1, t, T, TRUE, true, True` for "True" |
|  |  |  |  |

This controls if query should use `replica` label for deduplication or not.

### Auto downsampling

| HTTP URL/FORM parameter | Type | Default | Example |
|----|----|----|----|
| `max_source_resolution` | `Float64/time.Duration/model.Duration` | `step / 5` or `0` if `query.auto-downsampling` is false (default: False) | `5m` |
|  |  |  |  |

Max source resolution is max resolution in seconds we want to use for data we query for. This means that for value:
* 0 -> we will use only raw data.
* 5m -> we will use max 5m downsampling.
* 1h -> we will use max 1h downsampling.

### Partial Response / Error Enabled

| HTTP URL/FORM parameter | Type | Default | Example |
|----|----|----|----|
| `partial_response` | `Boolean` | `query.partial-response` flag (default: True) | `1, t, T, TRUE, true, True` for "True" |
|  |  |  |  |

If true, then all storeAPIs that will be unavailable (and thus return no data) will not cause query to fail, but instead
return warning.

### Custom Response Fields

Any additional field does not break compatibility, however there is no guarantee that Grafana or any other client will understand those.

Currently Thanos UI exposed by Thanos understands
```go
type queryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`

	// Additional Thanos Response field.
	Warnings   []error          `json:"warnings,omitempty"`
}
```

Additional field is `Warnings` that contains every error that occurred that is assumed non critical. `partial_response`
option controls if storeAPI unavailability is considered critical.


## Expose UI on a sub-path

It is possible to expose thanos-query UI and optionally API on a sub-path.
The sub-path can be defined either statically or dynamically via an HTTP header.
Static path prefix definition follows the pattern used in Prometheus,
where `web.route-prefix` option defines HTTP request path prefix (endpoints prefix)
and `web.external-prefix` prefixes the URLs in HTML code and the HTTP redirect responces.

Additionally, Thanos supports dynamic prefix configuration, which
[is not yet implemented by Prometheus](https://github.com/prometheus/prometheus/issues/3156).
Dynamic prefixing simplifies setup when `thanos query` is exposed on a sub-path behind
a reverse proxy, for example, via a Kubernetes ingress controller
[Traefik](https://docs.traefik.io/basics/#frontends)
or [nginx](https://github.com/kubernetes/ingress-nginx/pull/1805).
If `PathPrefixStrip: /some-path` option or `traefik.frontend.rule.type: PathPrefixStrip`
Kubernetes Ingress annotation is set, then `Traefik` writes the stripped prefix into X-Forwarded-Prefix header.
Then, `thanos query --web.prefix-header=X-Forwarded-Prefix` will serve correct HTTP redirects and links prefixed by the stripped path.


## Flags

[embedmd]:# (flags/query.txt $)
```$
usage: thanos query [<flags>]

query node exposing PromQL enabled Query API with data retrieved from multiple
store nodes

Flags:
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --version                  Show application version.
      --log.level=info           Log filtering level.
      --log.format=logfmt        Log format to use.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT
                                 GCP project to send Google Cloud Trace tracings
                                 to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1
                                 How often we send traces (1/<sample-factor>).
                                 If 0 no trace will be sent periodically, unless
                                 forced by baggage item. See
                                 `pkg/tracing/tracing.go` for details.
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components if you use gossip,
                                 'grpc-advertise-address' is empty and you
                                 require cross-node connection.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no client
                                 CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --grpc-advertise-address=GRPC-ADVERTISE-ADDRESS
                                 Explicit (external) host:port address to
                                 advertise for gRPC StoreAPI in gossip cluster.
                                 If empty, 'grpc-address' will be used.
      --cluster.address="0.0.0.0:10900"
                                 Listen ip:port address for gossip cluster.
      --cluster.advertise-address=CLUSTER.ADVERTISE-ADDRESS
                                 Explicit (external) ip:port address to
                                 advertise for gossip in gossip cluster. Used
                                 internally for membership only.
      --cluster.peers=CLUSTER.PEERS ...
                                 Initial peers to join the cluster. It can be
                                 either <ip:port>, or <domain:port>. A lookup
                                 resolution is done only at the startup.
      --cluster.gossip-interval=<gossip interval>
                                 Interval between sending gossip messages. By
                                 lowering this value (more frequent) gossip
                                 messages are propagated across the cluster more
                                 quickly at the expense of increased bandwidth.
                                 Default is used from a specified network-type.
      --cluster.pushpull-interval=<push-pull interval>
                                 Interval for gossip state syncs. Setting this
                                 interval lower (more frequent) will increase
                                 convergence speeds across larger clusters at
                                 the expense of increased bandwidth usage.
                                 Default is used from a specified network-type.
      --cluster.refresh-interval=1m
                                 Interval for membership to refresh
                                 cluster.peers state, 0 disables refresh.
      --cluster.secret-key=CLUSTER.SECRET-KEY
                                 Initial secret key to encrypt cluster gossip.
                                 Can be one of AES-128, AES-192, or AES-256 in
                                 hexadecimal format.
      --cluster.network-type=lan
                                 Network type with predefined peers
                                 configurations. Sets of configurations
                                 accounting the latency differences between
                                 network types: local, lan, wan.
      --cluster.disable          If true gossip will be disabled and no cluster
                                 related server will be started.
      --http-advertise-address=HTTP-ADVERTISE-ADDRESS
                                 Explicit (external) host:port address to
                                 advertise for HTTP QueryAPI in gossip cluster.
                                 If empty, 'http-address' will be used.
      --grpc-client-tls-secure   Use TLS when talking to the gRPC server
      --grpc-client-tls-cert=""  TLS Certificates to use to identify this client
                                 to the server
      --grpc-client-tls-key=""   TLS Key for the client's certificate
      --grpc-client-tls-ca=""    TLS CA Certificates to use to verify gRPC
                                 servers
      --grpc-client-server-name=""
                                 Server name to verify the hostname on the
                                 returned gRPC certificates. See
                                 https://tools.ietf.org/html/rfc4366#section-3.1
      --web.route-prefix=""      Prefix for API and UI endpoints. This allows
                                 thanos UI to be served on a sub-path. This
                                 option is analogous to --web.route-prefix of
                                 Promethus.
      --web.external-prefix=""   Static prefix for all HTML links and redirect
                                 URLs in the UI query web interface. Actual
                                 endpoints are still served on / or the
                                 web.route-prefix. This allows thanos UI to be
                                 served behind a reverse proxy that strips a URL
                                 sub-path.
      --web.prefix-header=""     Name of HTTP request header used for dynamic
                                 prefixing of UI links and redirects. This
                                 option is ignored if web.external-prefix
                                 argument is set. Security risk: enable this
                                 option only if a reverse proxy in front of
                                 thanos is resetting the header. The
                                 --web.prefix-header=X-Forwarded-Prefix option
                                 can be useful, for example, if Thanos UI is
                                 served via Traefik reverse proxy with
                                 PathPrefixStrip option enabled, which sends the
                                 stripped prefix value in X-Forwarded-Prefix
                                 header. This allows thanos UI to be served on a
                                 sub-path.
      --query.timeout=2m         Maximum time to process query by query node.
      --query.max-concurrent=20  Maximum number of queries processed
                                 concurrently by query node.
      --query.replica-label=QUERY.REPLICA-LABEL
                                 Label to treat as a replica indicator along
                                 which data is deduplicated. Still you will be
                                 able to query without deduplication using
                                 'dedup=false' parameter.
      --selector-label=<name>="<value>" ...
                                 Query selector labels that will be exposed in
                                 info endpoint (repeated).
      --store=<store> ...        Addresses of statically configured store API
                                 servers (repeatable). The scheme may be
                                 prefixed with 'dns+' or 'dnssrv+' to detect
                                 store API servers through respective DNS
                                 lookups.
      --store.sd-files=<path> ...
                                 Path to files that contain addresses of store
                                 API servers. The path can be a glob pattern
                                 (repeatable).
      --store.sd-interval=5m     Refresh interval to re-read file SD files. It
                                 is used as a resync fallback.
      --store.sd-dns-interval=30s
                                 Interval between DNS resolutions.
      --query.auto-downsampling  Enable automatic adjustment (step / 5) to what
                                 source of data should be used in store gateways
                                 if no max_source_resolution param is specified.
      --query.partial-response   Enable partial response for queries if no
                                 partial_response param is specified.
      --store.response-timeout=0ms
                                 If a Store doesn't send any data in this
                                 specified duration then a Store will be ignored
                                 and partial data will be returned if it's
                                 enabled. 0 disables timeout.

```
