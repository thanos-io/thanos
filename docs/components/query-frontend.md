# Query Frontend

The `thanos query-frontend` command implements a service that can be put in front of Thanos Queriers to improve the read path. It is based on the [Cortex Query Frontend](https://cortexmetrics.io/docs/architecture/#query-frontend) component so you can find some common features like `Splitting` and `Results Caching`.

Query Frontend is fully stateless and horizontally scalable.

Example command to run Query Frontend:

```bash
thanos query-frontend \
    --http-address     "0.0.0.0:9090" \
    --query-frontend.downstream-url="<thanos-querier>:<querier-http-port>"
```

_**NOTE:** Currently only range queries (`/api/v1/query_range` API call) are actually processed through Query Frontend. All other API calls just directly go to the downstream Querier, which means only range queries are split and cached. But we are planning to support instant queries as well.

For more information please check out [initial design proposal](../proposals-done/202004-embedd-cortex-frontend.md).

## Features

### Splitting

Query Frontend splits a long query into multiple short queries based on the configured `--query-range.split-interval` flag. The default value of `--query-range.split-interval` is `24h`. When caching is enabled it should be greater than `0`.

There are some benefits from query splitting:

1. Safeguard. It prevents large queries from causing OOM issues to Queries.
2. Better parallelization.
3. Better load balancing for Queries.

### Retry

Query Frontend supports a retry mechanism to retry query when HTTP requests are failing. There is a `--query-range.max-retries-per-request` flag to limit the maximum retry times.

### Caching

Query Frontend supports caching query results and reuses them on subsequent queries. If the cached results are incomplete, Query Frontend calculates the required subqueries and executes them in parallel on downstream queriers. Query Frontend can optionally align queries with their step parameter to improve the cacheability of the query results. Currently, in-memory cache (fifo cache), memcached, and redis are supported.

#### Excluded from caching

* Requests that support deduplication and having it disabled with `dedup=false`. Read more about deduplication in [Dedup documentation](query.md#deduplication-enabled).
* Requests that specify Store Matchers.
* Requests where downstream queriers set the header `Cache-Control=no-store` in the response:
  * Requests with a partial **response**.
  * Requests with other warnings.

#### In-memory

```yaml mdox-exec="go run scripts/cfggen/main.go --name=queryfrontend.InMemoryResponseCacheConfig"
type: IN-MEMORY
config:
  max_size: ""
  max_size_items: 0
  validity: 0s
```

`max_size: ` Maximum memory size of the cache in bytes. A unit suffix (KB, MB, GB) may be applied.

**_NOTE:** If both `max_size` and `max_size_items` are not set, then the *cache* would not be created.

If either of `max_size` or `max_size_items` is set, then there is no limit on other field. For example - only set `max_size_item` to 1000, then `max_size` is unlimited. Similarly, if only `max_size` is set, then `max_size_items` is unlimited.

Example configuration: [kube-thanos](https://github.com/thanos-io/kube-thanos/blob/master/examples/all/manifests/thanos-query-frontend-deployment.yaml#L50-L54)

#### Memcached

```yaml mdox-exec="go run scripts/cfggen/main.go --name=queryfrontend.MemcachedResponseCacheConfig"
type: MEMCACHED
config:
  addresses: []
  timeout: 0s
  max_idle_connections: 0
  max_async_concurrency: 0
  max_async_buffer_size: 0
  max_get_multi_concurrency: 0
  max_item_size: 0
  max_get_multi_batch_size: 0
  dns_provider_update_interval: 0s
  auto_discovery: false
  expiration: 0s
```

`expiration` specifies memcached cache valid time. If set to 0s, so using a default of 24 hours expiration time.

If a `set` operation is skipped because of the item size is larger than `max_item_size`, this event is tracked by a counter metric `cortex_memcache_client_set_skip_total`.

Other cache configuration parameters, you can refer to [memcached-index-cache](store.md#memcached-index-cache).

The default memcached config is:

```yaml
type: MEMCACHED
config:
  addresses: [your-memcached-addresses]
  timeout: 500ms
  max_idle_connections: 100
  max_item_size: 1MiB
  max_async_concurrency: 10
  max_async_buffer_size: 10000
  max_get_multi_concurrency: 100
  max_get_multi_batch_size: 0
  dns_provider_update_interval: 10s
  expiration: 24h
```

#### Redis

The default redis config is:

```yaml mdox-exec="go run scripts/cfggen/main.go --name=queryfrontend.RedisResponseCacheConfig"
type: REDIS
config:
  addr: ""
  username: ""
  password: ""
  db: 0
  dial_timeout: 5s
  read_timeout: 3s
  write_timeout: 3s
  max_get_multi_concurrency: 100
  get_multi_batch_size: 100
  max_set_multi_concurrency: 100
  set_multi_batch_size: 100
  tls_enabled: false
  tls_config:
    ca_file: ""
    cert_file: ""
    key_file: ""
    server_name: ""
    insecure_skip_verify: false
  cache_size: 0
  master_name: ""
  max_async_buffer_size: 10000
  max_async_concurrency: 20
  expiration: 24h0m0s
```

`expiration` specifies redis cache valid time. If set to 0s, so using a default of 24 hours expiration time.

Other cache configuration parameters, you can refer to [redis-index-cache](store.md#redis-index-cache).

### Slow Query Log

Query Frontend supports `--query-frontend.log-queries-longer-than` flag to log queries running longer than some duration.

## Naming

Naming is hard :) Please check [here](https://github.com/thanos-io/thanos/pull/2434#discussion_r408300683) to see why we chose `query-frontend` as the name.

## Recommended Downstream Tripper Configuration

You can configure the parameters of the HTTP client that `query-frontend` uses for the downstream URL with parameters `--query-frontend.downstream-tripper-config` and `--query-frontend.downstream-tripper-config-file`. If it is pointing to a single host, most likely a load-balancer, then it is highly recommended to increase `max_idle_conns_per_host` via these parameters to at least 100 because otherwise `query-frontend` will not be able to leverage HTTP keep-alive connections, and the latency will be 10 - 20% higher. By default, the Go HTTP client will only keep two idle connections per each host.

Keys which denote a duration are strings that can end with `s` or `m` to indicate seconds or minutes respectively. All of the other keys are integers. Supported keys are:

* `idle_conn_timeout` - timeout of idle connections (string);
* `response_header_timeout` - maximum duration to wait for a response header (string);
* `tls_handshake_timeout` - maximum duration of a TLS handshake (string);
* `expect_continue_timeout` - [Go source code](https://github.com/golang/go/blob/912f0750472dd4f674b69ca1616bfaf377af1805/src/net/http/transport.go#L220-L226) (string);
* `max_idle_conns` - maximum number of idle connections to all hosts (integer);
* `max_idle_conns_per_host` - maximum number of idle connections to each host (integer);
* `max_conns_per_host` - maximum number of connections to each host (integer);

You can find the default values [here](https://github.com/thanos-io/thanos/blob/55cb8ca38b3539381dc6a781e637df15c694e50a/pkg/exthttp/transport.go#L12-L27).

## Forward Headers to Downstream Queriers

`--query-frontend.forward-header` flag provides list of request headers forwarded by query frontend to downstream queriers.

If downstream queriers need basic authentication to access, we can run query-frontend:

```bash
thanos query-frontend \
    --http-address     "0.0.0.0:9090" \
    --query-frontend.forward-header "Authorization"
    --query-frontend.downstream-url="<thanos-querier>:<querier-http-port>"
```

## Flags

```$ mdox-exec="thanos query-frontend --help"
usage: thanos query-frontend [<flags>]

Query frontend command implements a service deployed in front of queriers to
improve query parallelization and caching.

Flags:
      --cache-compression-type=""
                                 Use compression in results cache.
                                 Supported values are: 'snappy' and ” (disable
                                 compression).
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --http.config=""           [EXPERIMENTAL] Path to the configuration file
                                 that can enable TLS or authentication for all
                                 HTTP endpoints.
      --labels.default-time-range=24h
                                 The default metadata time range duration for
                                 retrieving labels through Labels and Series API
                                 when the range parameters are not specified.
      --labels.max-query-parallelism=14
                                 Maximum number of labels requests will be
                                 scheduled in parallel by the Frontend.
      --labels.max-retries-per-request=5
                                 Maximum number of retries for a single
                                 label/series API request; beyond this,
                                 the downstream error is returned.
      --labels.partial-response  Enable partial response for labels requests
                                 if no partial_response param is specified.
                                 --no-labels.partial-response for disabling.
      --labels.response-cache-config=<content>
                                 Alternative to
                                 'labels.response-cache-config-file' flag
                                 (mutually exclusive). Content of YAML file that
                                 contains response cache configuration.
      --labels.response-cache-config-file=<file-path>
                                 Path to YAML file that contains response cache
                                 configuration.
      --labels.response-cache-max-freshness=1m
                                 Most recent allowed cacheable result for
                                 labels requests, to prevent caching very recent
                                 results that might still be in flux.
      --labels.split-interval=24h
                                 Split labels requests by an interval and
                                 execute in parallel, it should be greater
                                 than 0 when labels.response-cache-config is
                                 configured.
      --log.format=logfmt        Log format to use. Possible options: logfmt or
                                 json.
      --log.level=info           Log filtering level.
      --query-frontend.compress-responses
                                 Compress HTTP responses.
      --query-frontend.downstream-tripper-config=<content>
                                 Alternative to
                                 'query-frontend.downstream-tripper-config-file'
                                 flag (mutually exclusive). Content of YAML file
                                 that contains downstream tripper configuration.
                                 If your downstream URL is localhost or
                                 127.0.0.1 then it is highly recommended to
                                 increase max_idle_conns_per_host to at least
                                 100.
      --query-frontend.downstream-tripper-config-file=<file-path>
                                 Path to YAML file that contains downstream
                                 tripper configuration. If your downstream URL
                                 is localhost or 127.0.0.1 then it is highly
                                 recommended to increase max_idle_conns_per_host
                                 to at least 100.
      --query-frontend.downstream-url="http://localhost:9090"
                                 URL of downstream Prometheus Query compatible
                                 API.
      --query-frontend.forward-header=<http-header-name> ...
                                 List of headers forwarded by the query-frontend
                                 to downstream queriers, default is empty
      --query-frontend.log-queries-longer-than=0
                                 Log queries that are slower than the specified
                                 duration. Set to 0 to disable. Set to < 0 to
                                 enable on all queries.
      --query-frontend.org-id-header=<http-header-name> ...
                                 Request header names used to identify the
                                 source of slow queries (repeated flag).
                                 The values of the header will be added to
                                 the org id field in the slow query log. If
                                 multiple headers match the request, the first
                                 matching arg specified will take precedence.
                                 If no headers match 'anonymous' will be used.
      --query-frontend.vertical-shards=QUERY-FRONTEND.VERTICAL-SHARDS
                                 Number of shards to use when
                                 distributing shardable PromQL queries.
                                 For more details, you can refer to
                                 the Vertical query sharding proposal:
                                 https://thanos.io/tip/proposals-accepted/202205-vertical-query-sharding.md
      --query-range.align-range-with-step
                                 Mutate incoming queries to align their
                                 start and end with their step for better
                                 cache-ability. Note: Grafana dashboards do that
                                 by default.
      --query-range.horizontal-shards=0
                                 Split queries in this many requests
                                 when query duration is below
                                 query-range.max-split-interval.
      --query-range.max-query-length=0
                                 Limit the query time range (end - start time)
                                 in the query-frontend, 0 disables it.
      --query-range.max-query-parallelism=14
                                 Maximum number of query range requests will be
                                 scheduled in parallel by the Frontend.
      --query-range.max-retries-per-request=5
                                 Maximum number of retries for a single query
                                 range request; beyond this, the downstream
                                 error is returned.
      --query-range.max-split-interval=0
                                 Split query range below this interval in
                                 query-range.horizontal-shards. Queries with a
                                 range longer than this value will be split in
                                 multiple requests of this length.
      --query-range.min-split-interval=0
                                 Split query range requests above this
                                 interval in query-range.horizontal-shards
                                 requests of equal range. Using
                                 this parameter is not allowed with
                                 query-range.split-interval. One should also set
                                 query-range.split-min-horizontal-shards to a
                                 value greater than 1 to enable splitting.
      --query-range.partial-response
                                 Enable partial response for query range
                                 requests if no partial_response param is
                                 specified. --no-query-range.partial-response
                                 for disabling.
      --query-range.request-downsampled
                                 Make additional query for downsampled data in
                                 case of empty or incomplete response to range
                                 request.
      --query-range.response-cache-config=<content>
                                 Alternative to
                                 'query-range.response-cache-config-file' flag
                                 (mutually exclusive). Content of YAML file that
                                 contains response cache configuration.
      --query-range.response-cache-config-file=<file-path>
                                 Path to YAML file that contains response cache
                                 configuration.
      --query-range.response-cache-max-freshness=1m
                                 Most recent allowed cacheable result for query
                                 range requests, to prevent caching very recent
                                 results that might still be in flux.
      --query-range.split-interval=24h
                                 Split query range requests by an interval and
                                 execute in parallel, it should be greater than
                                 0 when query-range.response-cache-config is
                                 configured.
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
      --version                  Show application version.
      --web.disable-cors         Whether to disable CORS headers to be set by
                                 Thanos. By default Thanos sets CORS headers to
                                 be allowed by all.

```
