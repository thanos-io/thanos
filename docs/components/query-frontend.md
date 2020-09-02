---
title: Query Frontend
type: docs
menu: components
---

# Query Frontend

The `thanos query-frontend` command implements a service that can be put in front of Thanos Queriers to improve the read path. It is
based on the [Cortex Query Frontend](https://cortexmetrics.io/docs/architecture/#query-frontend) component so you can find some common features like `Splitting` and `Results Caching`.

Query Frontend is fully stateless and horizontally scalable.

Example command to run Query Frontend:

```bash
thanos query-frontend \
    --http-address     "http://0.0.0.0:9090" \
    --query-frontend.downstream-url="<thanos-querier>:<querier-http-port>"
```

_**NOTE:** Currently only range queries (`/api/v1/query_range` API call) are actually processed through Query Frontend. All other
API calls just directly go to the downstream Querier, which means only range queries are split and cached. But we are planning to support instant queries as well.

For more information please check out [initial design proposal](https://thanos.io/tip/proposals/202004_embedd_cortex_frontend.md/).

## Features

### Splitting

Query Frontend splits a long query into multiple short queries based on the configured `--query-range.split-interval` flag. The default value of `--query-range.split-interval`
is `24h`. Set it to `0` disables query splitting, but please note that caching is also disabled in this case.

There are some benefits from query splitting:

1. Safe guard. It prevents large queries from causing OOM issues to Queries.
2. Better parallelization.
3. Better load balancing for Queries.

### Retry

Query Frontend supports a retry mechanism to retry query when HTTP requests are failing. There is a `--query-range.max-retries-per-request` flag to limit the maximum retry times.

### Caching

Query Frontend supports caching query results and reuses them on subsequent queries. If the cached results are incomplete,
Query Frontend calculates the required subqueries and executes them in parallel on downstream queriers. Query Frontend can optionally align queries with their step parameter to improve the cacheability of the query results.
It uses the cortex cache module so supports all that is supported there.

[embedmd]:# (../flags/config_frontend_cache.txt yaml)
```yaml
- cache:
    enable_fifocache: false
    default_validity: 0s
    background:
      writeback_goroutines: 0
      writeback_buffer: 0
    memcached:
      expiration: 0s
      batch_size: 0
      parallelism: 0
    memcached_client:
      host: ""
      service: ""
      addresses: ""
      timeout: 0s
      max_idle_conns: 0
      update_interval: 0s
      consistent_hash: false
    redis:
      endpoint: ""
      timeout: 0s
      expiration: 0s
      max_idle_conns: 0
      max_active_conns: 0
      password: ""
      enable_tls: false
      idle_timeout: 0s
      wait_on_pool_exhaustion: false
      max_conn_lifetime: 0s
    fifocache:
      max_size_bytes: ""
      max_size_items: 0
      validity: 0s
      size: 0
    prefix: ""
  limits:
    ingestion_rate: 0
    ingestion_rate_strategy: ""
    ingestion_burst_size: 0
    accept_ha_samples: false
    ha_cluster_label: ""
    ha_replica_label: ""
    drop_labels: []
    max_label_name_length: 0
    max_label_value_length: 0
    max_label_names_per_series: 0
    max_metadata_length: 0
    reject_old_samples: false
    reject_old_samples_max_age: 0s
    creation_grace_period: 0s
    enforce_metadata_metric_name: false
    enforce_metric_name: false
    user_subring_size: 0
    max_series_per_query: 0
    max_samples_per_query: 0
    max_series_per_user: 0
    max_series_per_metric: 0
    max_global_series_per_user: 0
    max_global_series_per_metric: 0
    min_chunk_length: 0
    max_metadata_per_user: 0
    max_metadata_per_metric: 0
    max_global_metadata_per_user: 0
    max_global_metadata_per_metric: 0
    max_chunks_per_query: 0
    max_query_length: 0s
    max_query_parallelism: 0
    cardinality_limit: 0
    max_cache_freshness: 0s
    per_tenant_override_config: ""
    per_tenant_override_period: 0s
  queryrange:
    split_queries_by_interval: 0s
    split_queries_by_day: false
    align_queries_with_step: false
    results_cache:
      cache:
        enable_fifocache: false
        default_validity: 0s
        background:
          writeback_goroutines: 0
          writeback_buffer: 0
        memcached:
          expiration: 0s
          batch_size: 0
          parallelism: 0
        memcached_client:
          host: ""
          service: ""
          addresses: ""
          timeout: 0s
          max_idle_conns: 0
          update_interval: 0s
          consistent_hash: false
        redis:
          endpoint: ""
          timeout: 0s
          expiration: 0s
          max_idle_conns: 0
          max_active_conns: 0
          password: ""
          enable_tls: false
          idle_timeout: 0s
          wait_on_pool_exhaustion: false
          max_conn_lifetime: 0s
        fifocache:
          max_size_bytes: ""
          max_size_items: 0
          validity: 0s
          size: 0
        prefix: ""
      max_freshness: 0s
    cache_results: false
    max_retries: 0
    parallelise_shardable_queries: false
  frontend:
    max_outstanding_per_tenant: 0
    compress_responses: false
    downstream_url: ""
    log_queries_longer_than: 0s
```

### Slow Query Log

Query Frontend supports `--query-frontend.log_queries_longer_than` flag to log queries running longer then some duration.

## Naming

Naming is hard :) Please check [here](https://github.com/thanos-io/thanos/pull/2434#discussion_r408300683) to see why we chose `query-frontend` as the name.

## Flags

[embedmd]:# (flags/query-frontend.txt $)
```$
usage: thanos query-frontend [<flags>]

query frontend

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
                              https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                              Alternative to 'tracing.config-file' flag (lower
                              priority). Content of YAML file with tracing
                              configuration. See format details:
                              https://thanos.io/tip/tracing.md/#configuration
      --query-range.split-queries-by-interval=24h
                              Split queries by an interval and execute in
                              parallel, 0 disables it.
      --query-range.max-retries-per-request=5
                              Maximum number of retries for a single request;
                              beyond this, the downstream error is returned.
      --query-range.max-query-length=0
                              Limit the query time range (end - start time) in
                              the query-frontend, 0 disables it.
      --query-range.max-query-parallelism=14
                              Maximum number of queries will be scheduled in
                              parallel by the Frontend.
      --query-range.max-cache-freshness=1m
                              Most recent allowed cacheable result, to prevent
                              caching very recent results that might still be in
                              flux.
      --query-range.partial-response
                              Enable partial response for queries if no
                              partial_response param is specified.
                              --no-query-range.partial-response for disabling.
      --query-range.cache-config-file=<file-path>
                              Path to YAML file that contains response cache
                              configuration.
      --query-range.cache-config=<content>
                              Alternative to 'query-range.cache-config-file'
                              flag (lower priority). Content of YAML file that
                              contains response cache configuration.
      --http-address="0.0.0.0:10902"
                              Listen host:port for HTTP endpoints.
      --http-grace-period=2m  Time to wait after an interrupt received for HTTP
                              Server.
      --query-frontend.downstream-url="http://localhost:9090"
                              URL of downstream Prometheus Query compatible API.
      --query-frontend.compress-http-responses
                              Compress HTTP responses.
      --query-frontend.log-queries-longer-than=0
                              Log queries that are slower than the specified
                              duration. Set to 0 to disable. Set to < 0 to
                              enable on all queries.
      --log.request.decision=LogFinishCall
                              Request Logging for logging the start and end of
                              requests. LogFinishCall is enabled by default.
                              LogFinishCall : Logs the finish call of the
                              requests. LogStartAndFinishCall : Logs the start
                              and finish call of the requests. NoLogCall :
                              Disable request logging.

```
