# Querier/Query

The `thanos query` command (also known as "Querier") implements the [Prometheus HTTP v1 API](https://prometheus.io/docs/prometheus/latest/querying/api/) to query data in a Thanos cluster via PromQL.

In short, it gathers the data needed to evaluate the query from underlying [StoreAPIs](https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/rpc.proto), evaluates the query and returns the result.

Querier is fully stateless and horizontally scalable.

Example command to run Querier:

```bash
thanos query \
    --http-address     "0.0.0.0:9090" \
    --endpoint         "<store-api>:<grpc-port>" \
    --endpoint         "<store-api2>:<grpc-port>"
```

## Querier use cases, why do I need this component?

Thanos Querier essentially allows to aggregate and optionally deduplicate multiple metrics backends under single Prometheus Query endpoint.

### Global View

Since for Querier "a backend" is anything that implements gRPC StoreAPI we can aggregate data from any number of the different storages like:

* Prometheus (see [Sidecar](sidecar.md))
* Object Storage (see [Store Gateway](store.md))
* Global alerting/recording rules evaluations (see [Ruler](rule.md))
* Metrics received from Prometheus remote write streams (see [Receiver](receive.md))
* Another Querier (you can stack Queriers on top of each other)
* Non-Prometheus systems!
  * e.g [OpenTSDB](../integrations.md#opentsdb-as-storeapi)

Thanks to that, you can run queries (manually, from Grafana or via Alerting rule) that aggregate metrics from mix of those sources.

Some examples:

* `sum(cpu_used{cluster=~"cluster-(eu1|eu2|eu3|us1|us2|us3)", job="service1"})` that will give you sum of CPU used inside all listed clusters for service `service1`. This will work even if those clusters runs multiple Prometheus servers each. Querier will know which data sources to query.

* In single cluster you shard Prometheus functionally or have different Prometheus instances for different tenants. You can spin up Querier to have access to both within single Query evaluation.

### Run-time deduplication of HA groups

Prometheus is stateful and does not allow replicating its database. This means that increasing high availability by running multiple Prometheus replicas is not very easy to use. Simple load balancing will not work as for example after some crash, replica might be up but querying such replica will result in small gap during the period it was down. You have a second replica that maybe was up, but it could be down in other moment (e.g rolling restart), so load balancing on top of those is not working well.

Thanos Querier instead pulls the data from both replicas, and deduplicate those signals, filling the gaps if any, transparently to the Querier consumer.

## Metric Query Flow Overview

<img src="../img/querier.svg" class="img-fluid" alt="querier-steps"/>

Overall QueryAPI exposed by Thanos is guaranteed to be compatible with [Prometheus 2.x. API](https://prometheus.io/docs/prometheus/latest/querying/api/). The above diagram shows what Querier does for each Prometheus query request.

See [here](../service-discovery.md) on how to connect Querier with desired StoreAPIs.

### Deduplication

The query layer can deduplicate series that were collected from high-availability pairs of data sources such as Prometheus. A fixed single or multiple replica labels must be chosen for the entire cluster and can then be passed to query nodes on startup.

Two or more series that are only distinguished by the given replica label, will be merged into a single time series. This also hides gaps in collection of a single data source.

### An example with a single replica labels:

* Prometheus + sidecar "A": `cluster=1,env=2,replica=A`
* Prometheus + sidecar "B": `cluster=1,env=2,replica=B`
* Prometheus + sidecar "A" in different cluster: `cluster=2,env=2,replica=A`

If we configure Querier like this:

```
thanos query \
    --http-address        "0.0.0.0:9090" \
    --query.replica-label "replica" \
    --endpoint            "<store-api>:<grpc-port>" \
    --endpoint            "<store-api2>:<grpc-port>" \
```

And we query for metric `up{job="prometheus",env="2"}` with this option we will get 2 results:

* `up{job="prometheus",env="2",cluster="1"} 1`
* `up{job="prometheus",env="2",cluster="2"} 1`

WITHOUT this replica flag (deduplication turned off), we will get 3 results:

* `up{job="prometheus",env="2",cluster="1",replica="A"} 1`
* `up{job="prometheus",env="2",cluster="1",replica="B"} 1`
* `up{job="prometheus",env="2",cluster="2",replica="A"} 1`

### The same output will be present for this example with multiple replica labels:

* Prometheus + sidecar "A": `cluster=1,env=2,replica=A,replicaX=A`
* Prometheus + sidecar "B": `cluster=1,env=2,replica=B,replicaX=B`
* Prometheus + sidecar "A" in different cluster: `cluster=2,env=2,replica=A,replicaX=A`

```
thanos query \
    --http-address        "0.0.0.0:9090" \
    --query.replica-label "replica" \
    --query.replica-label "replicaX" \
    --endpoint            "<store-api>:<grpc-port>" \
    --endpoint            "<store-api2>:<grpc-port>" \
```

This logic can also be controlled via parameter on QueryAPI. More details below.

### Deduplication Algorithms

Thanos Querier supports different algorithms for deduplicating overlapping series. You can choose the deduplication algorithm using the `--deduplication.func` flag. The available options are:

* `penalty` (default): This is the default deduplication algorithm used by Thanos. It fills gaps only after a certain penalty window. This helps avoid flapping between replicas due to minor differences or delays.
* `chain`: This algorithm performs 1:1 deduplication for samples. It merges all available data points from the replicas without applying any penalty. This is useful in deployments based on receivers, where each replica is populated by the same data. In such cases, using the penalty algorithm may cause gaps even when data is available in other replicas.

Note that deduplication of HA groups is not supported by the `chain` algorithm.

## Thanos PromQL Engine (experimental)

By default, Thanos querier comes with standard Prometheus PromQL engine. However, when `--query.promql-engine=thanos` is specified, Thanos will use [experimental Thanos PromQL engine](http://github.com/thanos-io/promql-engine) which is a drop-in, efficient implementation of PromQL engine with query planner and optimizers.

To learn more, see [the introduction talk](https://youtu.be/pjkWzDVxWk4?t=3609) from [the PromConEU 2022](https://promcon.io/2022-munich/talks/opening-pandoras-box-redesigning/).

This feature is still **experimental** given active development. All queries should be supported due to bulit-in fallback to old PromQL if something is not yet implemented.

For new engine bugs/issues, please use https://github.com/thanos-io/promql-engine GitHub issues.

### Distributed execution mode

When using Thanos PromQL Engine the distributed execution mode can be enabled using `--query.mode=distributed`. When this mode is enabled, the Querier will break down each query into independent fragments and delegate them to components which implement the Query API.

This mode is particularly useful in architectures where multiple independent Queriers are deployed in separate environments (different regions or different Kubernetes clusters) and are federated through a separate central Querier. A Querier running in the distributed mode will only talk to Queriers, or other components which implement the Query API. Endpoints which only act as Stores (e.g. Store Gateways or Rulers), and are directly connected to a distributed Querier, will not be included in the execution of a distributed query. This constraint should help with keeping the distributed query execution simple and efficient, but could be removed in the future if there are good use cases for it.

For further details on the design and use cases of this feature, see the [official design document](https://thanos.io/tip/proposals-done/202301-distributed-query-execution.md/).

## Query API Overview

As mentioned, Query API exposed by Thanos is guaranteed to be compatible with [Prometheus 2.x. API](https://prometheus.io/docs/prometheus/latest/querying/api/). However for additional Thanos features on top of Prometheus, Thanos adds:

* partial response behaviour
* several additional parameters listed below
* custom response fields.

Let's walk through all of those extensions:

### Partial Response

QueryAPI and StoreAPI has additional behaviour controlled via query parameter called [PartialResponseStrategy](../../pkg/store/storepb/rpc.pb.go).

This parameter controls tradeoff between accuracy and availability.

Partial response is a potentially missed result within query against QueryAPI or StoreAPI. This can happen if one of StoreAPIs is returning error or timeout whereas couple of others returns success. It does not mean you are missing data, you might lucky enough that you actually get the correct data as the broken StoreAPI did not have anything for your query.

If partial response happen QueryAPI returns human readable warnings explained [here](#custom-response-fields).

Now it supports two strategies:
* "warn"
* "abort" (default)

NOTE: Having a warning does not necessarily mean partial response (e.g no store matched query warning).

Querier also allows to configure different timeouts:

* `--query.timeout`
* `--store.response-timeout`

If you prefer availability over accuracy you can set tighter timeout to underlying StoreAPI than overall query timeout. If partial response strategy is NOT `abort`, this will "ignore" slower StoreAPIs producing just warning with 200 status code response.

### Deduplication replica labels.

| HTTP URL/FORM parameter | Type       | Default                                      | Example                                         |
|-------------------------|------------|----------------------------------------------|-------------------------------------------------|
| `replicaLabels`         | `[]string` | `query.replica-label` flag (default: empty). | `replicaLabels=replicaA&replicaLabels=replicaB` |
|                         |            |                                              |                                                 |

This overwrites the `query.replica-label` cli flag to allow dynamic replica labels at query time.

### Deduplication Enabled

| HTTP URL/FORM parameter | Type      | Default                                                         | Example                                |
|-------------------------|-----------|-----------------------------------------------------------------|----------------------------------------|
| `dedup`                 | `Boolean` | True, but effect depends on `query.replica` configuration flag. | `1, t, T, TRUE, true, True` for "True" |
|                         |           |                                                                 |                                        |

This controls if query results should be deduplicated using the replica labels.

### Auto downsampling

| HTTP URL/FORM parameter | Type                                   | Default                                                                  | Example |
|-------------------------|----------------------------------------|--------------------------------------------------------------------------|---------|
| `max_source_resolution` | `Float64/time.Duration/model.Duration` | `step / 5` or `0` if `query.auto-downsampling` is false (default: False) | `5m`    |
|                         |                                        |                                                                          |         |

Max source resolution is max resolution in seconds we want to use for data we query for.

Available options:

* `auto` - Select downsample resolution automatically based on the query.
* `0` - Only use raw data.
* `5m` - Use max 5m downsampling.
* `1h` - Use max 1h downsampling.

### Partial Response Strategy

 <!-- TODO(bwplotka): Update. This will change to "strategy" soon as [PartialResponseStrategy enum here](../../pkg/store/storepb/rpc.proto) -->

| HTTP URL/FORM parameter | Type      | Default                                       | Example                                |
|-------------------------|-----------|-----------------------------------------------|----------------------------------------|
| `partial_response`      | `Boolean` | `query.partial-response` flag (default: True) | `1, t, T, TRUE, true, True` for "True" |
|                         |           |                                               |                                        |

If true, then all storeAPIs that will be unavailable (and thus return no data) will not cause query to fail, but instead return warning.

### Custom Response Fields

Any additional field does not break compatibility, however there is no guarantee that Grafana or any other client will understand those.

Currently Thanos UI exposed by Thanos understands

```go
type queryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`

	// Additional Thanos Response field.
	Warnings []error `json:"warnings,omitempty"`
}
```

Additional field is `Warnings` that contains every error that occurred that is assumed non critical. `partial_response` option controls if storeAPI unavailability is considered critical.

### Concurrent Selects

Thanos Querier has the ability to perform concurrent select request per query. It dissects given PromQL statement and executes selectors concurrently against the discovered StoreAPIs. The maximum number of concurrent requests are being made per query is controlled by `query.max-concurrent-select` flag. Keep in mind that the maximum number of concurrent queries that are handled by querier is controlled by `query.max-concurrent`. Please consider implications of combined value while tuning the querier.

### Store filtering

It's possible to provide a set of matchers to the Querier api to select specific stores to be used during the query using the `storeMatch[]` parameter. It is useful when debugging a slow/broken store. It uses the same format as the matcher of [Prometheus' federate api](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers). Note that at the moment the querier only supports the `__address__` which contain the address of the store as it is shown on the `/stores` endpoint of the UI.

Example:

```
- targets:
  - prometheus-foo.thanos-sidecar:10901
  - prometheus-bar.thanos-sidecar:10901
```

```
http://localhost:10901/api/v1/query?query=up&dedup=true&partial_response=true&storeMatch[]={__address__=~"prometheus-foo.*"}
```

Will only return metrics from `prometheus-foo.thanos-sidecar:10901`

## Expose UI on a sub-path

It is possible to expose thanos-query UI and optionally API on a sub-path. The sub-path can be defined either statically or dynamically via an HTTP header. Static path prefix definition follows the pattern used in Prometheus, where `web.route-prefix` option defines HTTP request path prefix (endpoints prefix) and `web.external-prefix` prefixes the URLs in HTML code and the HTTP redirect responses.

Additionally, Thanos supports dynamic prefix configuration, which [is not yet implemented by Prometheus](https://github.com/prometheus/prometheus/issues/3156). Dynamic prefixing simplifies setup when `thanos query` is exposed on a sub-path behind a reverse proxy, for example, via a Kubernetes ingress controller [Traefik](https://docs.traefik.io/routing/routers/) or [nginx](https://github.com/kubernetes/ingress-nginx/pull/1805). If `PathPrefixStrip: /some-path` option or `traefik.frontend.rule.type: PathPrefixStrip` Kubernetes Ingress annotation is set, then `Traefik` writes the stripped prefix into X-Forwarded-Prefix header. Then, `thanos query --web.prefix-header=X-Forwarded-Prefix` will serve correct HTTP redirects and links prefixed by the stripped path.

## File SD

`--store.sd-files` flag provides a path to a JSON or YAML formatted file, which contains a list of targets in [Prometheus target format](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config).

Example file SD file in YAML:

```
- targets:
  - prometheus-0.thanos-sidecar:10901
  - prometheus-1.thanos-sidecar:10901
  - thanos-store:10901
  - thanos-short-store:10901
  - thanos-rule:10901
- targets:
  - prometheus-0.thanos-sidecar.infra:10901
  - prometheus-1.thanos-sidecar.infra:10901
  - thanos-store.infra:10901
```

## Active Query Tracking

`--query.active-query-path` is an option which allows the user to specify a directory which will contain a `queries.active` file to track active queries. To enable this feature, the user has to specify a directory other than "", since that is skipped being the default.

## Tenancy

### Tenant Metrics

Tenant information is captured in relevant Thanos exported metrics in the Querier, Query Frontend and Store. In order make use of this functionality requests to the Query/Query Frontend component should include the tenant-id in the appropriate HTTP request header as configured with `--query.tenant-header`. The tenant information is passed through components (including Query Frontend), down to the Thanos Store, enabling per-tenant metrics in these components also. If no tenant header is set to requests to the query component, the default tenant as defined by `--query.default-tenant-id` will be used.

### Tenant Enforcement

Enforcement of tenancy can be enabled using `--query.enforce-tenancy`. If enabled, queries will only fetch series containing a specific matcher, while evaluating PromQL expressions. The matcher label name is `--query.tenant-label-name` and the matcher value matches the tenant, as sent to the querier in the HTTP header configured with `--query-tenant-header`. This functionality requires that metrics are injected with a tenant label when ingested into Thanos. This can be done for example by enabling tenancy in the Thanos Receive component.

In case of nested Thanos Query components, it's important to note that tenancy enforcement will only occur in the querier which the initial request is sent to, the layered queriers will not perform any enforcement.

Further, note that there are no authentication mechanisms in Thanos, so anyone can set an arbitrary tenant in the HTTP header. It is recommended to use a proxy in front of the querier in case an authentication mechanism is needed. The Query UI also includes an option to set an arbitrary tenant, and should therefore not be exposed to end-users if users should not be able to see each others data.

## Flags

```$ mdox-exec="thanos query --help"
usage: thanos query [<flags>]

Query node exposing PromQL enabled Query API with data retrieved from multiple
store nodes.


Flags:
  -h, --[no-]help                Show context-sensitive help (also try
                                 --help-long and --help-man).
      --[no-]version             Show application version.
      --log.level=info           Log filtering level.
      --log.format=logfmt        Log format to use. Possible options: logfmt,
                                 json or journald.
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (mutually exclusive). Content of YAML file
                                 with tracing configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --[no-]enable-auto-gomemlimit
                                 Enable go runtime to automatically limit memory
                                 consumption.
      --auto-gomemlimit.ratio=0.9
                                 The ratio of reserved GOMEMLIMIT memory to the
                                 detected maximum container or system memory.
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --http.config=""           [EXPERIMENTAL] Path to the configuration file
                                 that can enable TLS or authentication for all
                                 HTTP endpoints.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no
                                 client CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --grpc-server-tls-min-version="1.3"
                                 TLS supported minimum version for gRPC server.
                                 If no version is specified, it'll default to
                                 1.3. Allowed values: ["1.0", "1.1", "1.2",
                                 "1.3"]
      --grpc-server-max-connection-age=60m
                                 The grpc server max connection age. This
                                 controls how often to re-establish connections
                                 and redo TLS handshakes.
      --grpc-grace-period=2m     Time to wait after an interrupt received for
                                 GRPC Server.
      --[no-]grpc-client-tls-secure
                                 Use TLS when talking to the gRPC server
      --[no-]grpc-client-tls-skip-verify
                                 Disable TLS certificate verification i.e self
                                 signed, signed by fake CA
      --grpc-client-tls-cert=""  TLS Certificates to use to identify this client
                                 to the server
      --grpc-client-tls-key=""   TLS Key for the client's certificate
      --grpc-client-tls-ca=""    TLS CA Certificates to use to verify gRPC
                                 servers
      --grpc-client-server-name=""
                                 Server name to verify the hostname on
                                 the returned gRPC certificates. See
                                 https://tools.ietf.org/html/rfc4366#section-3.1
      --grpc-compression=none    Compression algorithm to use for gRPC requests
                                 to other clients. Must be one of: snappy, none
      --web.route-prefix=""      Prefix for API and UI endpoints. This allows
                                 thanos UI to be served on a sub-path.
                                 Defaults to the value of --web.external-prefix.
                                 This option is analogous to --web.route-prefix
                                 of Prometheus.
      --web.external-prefix=""   Static prefix for all HTML links and
                                 redirect URLs in the UI query web interface.
                                 Actual endpoints are still served on / or the
                                 web.route-prefix. This allows thanos UI to be
                                 served behind a reverse proxy that strips a URL
                                 sub-path.
      --web.prefix-header=""     Name of HTTP request header used for dynamic
                                 prefixing of UI links and redirects.
                                 This option is ignored if web.external-prefix
                                 argument is set. Security risk: enable
                                 this option only if a reverse proxy in
                                 front of thanos is resetting the header.
                                 The --web.prefix-header=X-Forwarded-Prefix
                                 option can be useful, for example, if Thanos
                                 UI is served via Traefik reverse proxy with
                                 PathPrefixStrip option enabled, which sends the
                                 stripped prefix value in X-Forwarded-Prefix
                                 header. This allows thanos UI to be served on a
                                 sub-path.
      --[no-]web.disable-cors    Whether to disable CORS headers to be set by
                                 Thanos. By default Thanos sets CORS headers to
                                 be allowed by all.
      --query.timeout=2m         Maximum time to process query by query node.
      --query.promql-engine=prometheus
                                 Default PromQL engine to use.
      --[no-]query.enable-x-functions
                                 Whether to enable extended rate functions
                                 (xrate, xincrease and xdelta). Only has effect
                                 when used with Thanos engine.
      --query.mode=local         PromQL query mode. One of: local, distributed.
      --query.max-concurrent=20  Maximum number of queries processed
                                 concurrently by query node.
      --query.lookback-delta=QUERY.LOOKBACK-DELTA
                                 The maximum lookback duration for retrieving
                                 metrics during expression evaluations.
                                 PromQL always evaluates the query for the
                                 certain timestamp (query range timestamps are
                                 deduced by step). Since scrape intervals might
                                 be different, PromQL looks back for given
                                 amount of time to get latest sample. If it
                                 exceeds the maximum lookback delta it assumes
                                 series is stale and returns none (a gap).
                                 This is why lookback delta should be set to at
                                 least 2 times of the slowest scrape interval.
                                 If unset it will use the promql default of 5m.
      --query.max-concurrent-select=4
                                 Maximum number of select requests made
                                 concurrently per a query.
      --query.conn-metric.label=external_labels... ...
                                 Optional selection of query connection metric
                                 labels to be collected from endpoint set
      --deduplication.func=penalty
                                 Experimental. Deduplication algorithm for
                                 merging overlapping series. Possible values
                                 are: "penalty", "chain". If no value is
                                 specified, penalty based deduplication
                                 algorithm will be used. When set to chain, the
                                 default compact deduplication merger is used,
                                 which performs 1:1 deduplication for samples.
                                 At least one replica label has to be set via
                                 --query.replica-label flag.
      --query.replica-label=QUERY.REPLICA-LABEL ...
                                 Labels to treat as a replica indicator along
                                 which data is deduplicated. Still you will
                                 be able to query without deduplication using
                                 'dedup=false' parameter. Data includes time
                                 series, recording rules, and alerting rules.
                                 Flag may be specified multiple times as well as
                                 a comma separated list of labels.
      --query.partition-label=QUERY.PARTITION-LABEL ...
                                 Labels that partition the leaf queriers. This
                                 is used to scope down the labelsets of leaf
                                 queriers when using the distributed query mode.
                                 If set, these labels must form a partition
                                 of the leaf queriers. Partition labels must
                                 not intersect with replica labels. Every TSDB
                                 of a leaf querier must have these labels.
                                 This is useful when there are multiple external
                                 labels that are irrelevant for the partition as
                                 it allows the distributed engine to ignore them
                                 for some optimizations. If this is empty then
                                 all labels are used as partition labels.
      --query.metadata.default-time-range=0s
                                 The default metadata time range duration for
                                 retrieving labels through Labels and Series API
                                 when the range parameters are not specified.
                                 The zero value means range covers the time
                                 since the beginning.
      --selector-label=<name>="<value>" ...
                                 Query selector labels that will be exposed in
                                 info endpoint (repeated).
      --[no-]query.auto-downsampling
                                 Enable automatic adjustment (step / 5) to what
                                 source of data should be used in store gateways
                                 if no max_source_resolution param is specified.
      --[no-]query.partial-response
                                 Enable partial response for queries if
                                 no partial_response param is specified.
                                 --no-query.partial-response for disabling.
      --query.active-query-path=""
                                 Directory to log currently active queries in
                                 the queries.active file.
      --enable-feature= ...      Comma separated feature names to enable. Valid
                                 options for now: promql-experimental-functions
                                 (enables promql experimental functions in
                                 query)
      --query.default-evaluation-interval=1m
                                 Set default evaluation interval for sub
                                 queries.
      --query.default-step=1s    Set default step for range queries. Default
                                 step is only used when step is not set in UI.
                                 In such cases, Thanos UI will use default
                                 step to calculate resolution (resolution
                                 = max(rangeSeconds / 250, defaultStep)).
                                 This will not work from Grafana, but Grafana
                                 has __step variable which can be used.
      --store.response-timeout=0ms
                                 If a Store doesn't send any data in this
                                 specified duration then a Store will be ignored
                                 and partial data will be returned if it's
                                 enabled. 0 disables timeout.
      --selector.relabel-config-file=<file-path>
                                 Path to YAML file with relabeling
                                 configuration that allows selecting blocks
                                 to query based on their external labels.
                                 It follows the Thanos sharding relabel-config
                                 syntax. For format details see:
                                 https://thanos.io/tip/thanos/sharding.md/#relabelling
      --selector.relabel-config=<content>
                                 Alternative to 'selector.relabel-config-file'
                                 flag (mutually exclusive). Content of YAML
                                 file with relabeling configuration that allows
                                 selecting blocks to query based on their
                                 external labels. It follows the Thanos sharding
                                 relabel-config syntax. For format details see:
                                 https://thanos.io/tip/thanos/sharding.md/#relabelling
      --request.logging-config-file=<file-path>
                                 Path to YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --request.logging-config=<content>
                                 Alternative to 'request.logging-config-file'
                                 flag (mutually exclusive). Content
                                 of YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --alert.query-url=ALERT.QUERY-URL
                                 The external Thanos Query URL that would be set
                                 in all alerts 'Source' field.
      --query.telemetry.request-duration-seconds-quantiles=0.1... ...
                                 The quantiles for exporting metrics about the
                                 request duration quantiles.
      --query.telemetry.request-samples-quantiles=100... ...
                                 The quantiles for exporting metrics about the
                                 samples count quantiles.
      --query.telemetry.request-series-seconds-quantiles=10... ...
                                 The quantiles for exporting metrics about the
                                 series count quantiles.
      --query.tenant-header="THANOS-TENANT"
                                 HTTP header to determine tenant.
      --query.default-tenant-id="default-tenant"
                                 Default tenant ID to use if tenant header is
                                 not present
      --query.tenant-certificate-field=
                                 Use TLS client's certificate field to determine
                                 tenant for write requests. Must be one of
                                 organization, organizationalUnit or commonName.
                                 This setting will cause the query.tenant-header
                                 flag value to be ignored.
      --[no-]query.enforce-tenancy
                                 Enforce tenancy on Query APIs. Responses
                                 are returned only if the label value of the
                                 configured tenant-label-name and the value of
                                 the tenant header matches.
      --query.tenant-label-name="tenant_id"
                                 Label name to use when enforcing tenancy (if
                                 --query.enforce-tenancy is enabled).
      --store.sd-dns-interval=30s
                                 Interval between DNS resolutions.
      --store.unhealthy-timeout=5m
                                 Timeout before an unhealthy store is cleaned
                                 from the store UI page.
      --endpoint.sd-config-file=<file-path>
                                 Path to Config File with endpoint definitions
      --endpoint.sd-config=<content>
                                 Alternative to 'endpoint.sd-config-file' flag
                                 (mutually exclusive). Content of Config File
                                 with endpoint definitions
      --endpoint.sd-config-reload-interval=5m
                                 Interval between endpoint config refreshes
      --store.sd-files=<path> ...
                                 (Deprecated) Path to files that contain
                                 addresses of store API servers. The path can be
                                 a glob pattern (repeatable).
      --store.sd-interval=5m     (Deprecated) Refresh interval to re-read file
                                 SD files. It is used as a resync fallback.
      --endpoint=<endpoint> ...  (Deprecated): Addresses of statically
                                 configured Thanos API servers (repeatable).
                                 The scheme may be prefixed with 'dns+' or
                                 'dnssrv+' to detect Thanos API servers through
                                 respective DNS lookups.
      --endpoint-group=<endpoint-group> ...
                                 (Deprecated, Experimental): DNS name of
                                 statically configured Thanos API server groups
                                 (repeatable). Targets resolved from the DNS
                                 name will be queried in a round-robin, instead
                                 of a fanout manner. This flag should be used
                                 when connecting a Thanos Query to HA groups of
                                 Thanos components.
      --endpoint-strict=<endpoint-strict> ...
                                 (Deprecated): Addresses of only statically
                                 configured Thanos API servers that are always
                                 used, even if the health check fails. Useful if
                                 you have a caching layer on top.
      --endpoint-group-strict=<endpoint-group-strict> ...
                                 (Deprecated, Experimental): DNS name of
                                 statically configured Thanos API server groups
                                 (repeatable) that are always used, even if the
                                 health check fails.
      --store.limits.request-series=0
                                 The maximum series allowed for a single Series
                                 request. The Series call fails if this limit is
                                 exceeded. 0 means no limit.
      --store.limits.request-samples=0
                                 The maximum samples allowed for a single
                                 Series request, The Series call fails if
                                 this limit is exceeded. 0 means no limit.
                                 NOTE: For efficiency the limit is internally
                                 implemented as 'chunks limit' considering each
                                 chunk contains a maximum of 120 samples.

```

## Exported metrics

Thanos Query also exports metrics about its own performance. You can find a list with these metrics below.

**Disclaimer**: this list is incomplete. The remaining metrics will be added over time.

| Name                                    | Type      | Labels                                          | Description                                                                                                       |
|-----------------------------------------|-----------|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| grpc_client_handled_total               | Counter   | grpc_code, grpc_method, grpc_service, grpc_type | Number of gRPC client requests handled by this query instance (including errors)                                  |
| grpc_server_handled_total               | Counter   | grpc_code, grpc_method, grpc_service, grpc_type | Number of gRPC server requests handled by this query instance (including errors)                                  |
| thanos_store_api_query_duration_seconds | Histogram | samples_le, series_le                           | Duration of the Thanos Store API select phase for a query according to the amount of samples and series selected. |
