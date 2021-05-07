---
title: Caching
type: docs
menu: thanos
---

# Background

Thanos can implement caching layers on multiple components. In general this should speed-up certain processes. There is no definitive guide on how one should setup an entire Thanos stack as each cluster and use-case is different. Especially what KPI's<sup>1</sup>, SLO's<sup>2</sup> and SLI's<sup>3</sup> matter to you, your customers and/or your stack should have influence on the setup.

However it is possible to give guidance on what each component tries to achieve, what it is used for and how you could configure this.

<sub>
<sup>1</sup> service level agreement
<sup>2</sup> service level objective
<sup>3</sup> service level indicator
</sub>

# Types of cache

| Caching type                                                                                           | Component           | Backends             | Configuration | Extra             |
| -------------------------------------------------------------------------------------------------- | ------------------ | --------------------- | ----------------- | ----------------------- |
| Index cache | [Store gateway](../components/store.md) | `in_memory` and `memcached` | base configuration | Enabled by default `in_memory`
| Bucket cache | [Store gateway](../components/store.md) | `in_memory` and `memcached` | base configuration + specific bucket cache extra's | Has extra metadata and chunk configuration options
| Query frontend cache | [Query-frontend](../components/query-frontend.md)  | `in_memory` and `memcached` | base configuration + extra field `expiration` |

## Index cache

Thanos Store Gateway supports an index cache to speed up postings and series lookups from TSDB blocks indexes

## Bucket cache

Thanos Store Gateway supports a “caching bucket” with chunks and metadata caching to speed up loading of chunks from TSDB blocks.

## Query frontend cache

Query Frontend supports caching query results and label requests. Query results are reuses on subsequent queries. If the cached results are incomplete, Query Frontend calculates the required subqueries and executes them in parallel on downstream queriers. Query Frontend can optionally align queries with their step parameter to improve the cacheability of the query results.

# Configurations for memcached

## [memcached] Base configuration

When using memcached, there is a base configuration that is consistent in every other caching component using memcached. Per component specific there may be additions to the configuration. Such as the `expiration: 0s` for the frontend cache component.

[embedmd]:# (flags/config_index_cache_memcached.txt yaml)
```yaml
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
```

## [memcached] Index cache

The index cache has no exceptions on the [base configuration](caching.md/#memcached-base-configuration).

The `memcached` index cache allows to use [Memcached](https://memcached.org) as cache backend. This cache type is configured using `--index-cache.config-file` to reference to the configuration file or `--index-cache.config` to put yaml config directly

Full example:

[embedmd]:# (flags/config_index_cache_memcached.txt yaml)
```yaml
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
```

The **required** settings are:

- `addresses`: list of memcached addresses, that will get resolved with the [DNS service discovery](../service-discovery.md/#dns-service-discovery) provider.

While the remaining settings are **optional**:

- `timeout`: the socket read/write timeout.
- `max_idle_connections`: maximum number of idle connections that will be maintained per address.
- `max_async_concurrency`: maximum number of concurrent asynchronous operations can occur.
- `max_async_buffer_size`: maximum number of enqueued asynchronous operations allowed.
- `max_get_multi_concurrency`: maximum number of concurrent connections when fetching keys. If set to `0`, the concurrency is unlimited.
- `max_get_multi_batch_size`: maximum number of keys a single underlying operation should fetch. If more keys are specified, internally keys are splitted into multiple batches and fetched concurrently, honoring `max_get_multi_concurrency`. If set to `0`, the batch size is unlimited.
- `max_item_size`: maximum size of an item to be stored in memcached. This option should be set to the same value of memcached `-I` flag (defaults to 1MB) in order to avoid wasting network round trips to store items larger than the max item size allowed in memcached. If set to `0`, the item size is unlimited.
- `dns_provider_update_interval`: the DNS discovery update interval.

This caching type is part of the [Store component](../components/store.md) component. Refer to that page for more information about flags and configuration definitions.

## [memcached] Bucket cache

Uses the [base configuration](caching.md/#memcached-base-configuration).

Additional options to configure various aspects of [chunks](../design.md/#chunk) cache are available:


[embedmd]:# (flags/config_bucket_cache_memcached.txt yaml)
```yaml
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
chunk_subrange_size: 0
max_chunks_get_range_requests: 0
chunk_object_attrs_ttl: 0s
chunk_subrange_ttl: 0s
blocks_iter_ttl: 0s
metafile_exists_ttl: 0s
metafile_doesnt_exist_ttl: 0s
metafile_content_ttl: 0s
metafile_max_size: 0
```

`config` field for memcached supports all the same configuration as memcached for [index cache](#memcached-index-cache). `addresses` in the config field is a **required** setting

Additional options to configure various aspects of [chunks](../design.md/#chunk) cache are available:

- `chunk_subrange_size`: size of segment of [chunks](../design.md/#chunk) object that is stored to the cache. This is the smallest unit that chunks cache is working with.
- `max_chunks_get_range_requests`: how many "get range" sub-requests may cache perform to fetch missing subranges.
- `chunk_object_attrs_ttl`: how long to keep information about [chunk file](../design.md/#chunk-file) attributes (e.g. size) in the cache.
- `chunk_subrange_ttl`: how long to keep individual subranges in the cache.

Following options are used for metadata caching (meta.json files, deletion mark files, iteration result):

- `blocks_iter_ttl`: how long to cache result of iterating blocks.
- `metafile_exists_ttl`: how long to cache information about whether meta.json or deletion mark file exists.
- `metafile_doesnt_exist_ttl`: how long to cache information about whether meta.json or deletion mark file doesn't exist.
- `metafile_content_ttl`: how long to cache content of meta.json and deletion mark files.
- `metafile_max_size`: maximum size of cached meta.json and deletion mark file. Larger files are not cached.

The yml structure for setting the in memory cache configs for caching bucket are the same as the [in-memory index cache](https://thanos.io/tip/components/store.md/#in-memory-index-cache) and all the options to configure Caching Buket mentioned above can be used.

Note that chunks and metadata cache is an experimental feature, and these fields may be renamed or removed completely in the future.

This caching type is part of the [Store component](../components/store.md) component. Refer to that page for more information about flags and configuration definitions.

## [memcached] Query frontend

Uses the [base configuration](caching.md/#memcached-base-configuration) with an extra option:

`expiration` specifies how long memcached itself keeps items. After that time, memcached evicts those items. `0s` means the default duration of `24h`.

[embedmd]:# (flags/config_index_cache_memcached.txt yaml)
```yaml
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
```

`expiration` specifies memcached cache valid time.  If set to 0s, so using a default of 24 hours expiration time.

If a `set` operation is skipped because of the item size is larger than `max_item_size`, this event is tracked by a counter metric `cortex_memcache_client_set_skip_total`.

This caching type is part of the [Memcached Query frontend](../components/query-frontend.md/) component. Refer to that page for more information about flags and configuration definitions.

# Configurations for in-memory

## [in-memory] Base configuration

[embedmd]:# (flags/config_index_cache_in_memory.txt yaml)
```yaml
type: IN-MEMORY
config:
  max_size: 0
  max_item_size: 0
```

## [in-memory] Index cache

The index cache has no exceptions on the [base configuration](caching.md/#in-memory-base-configuration).

[embedmd]:# (flags/config_index_cache_in_memory.txt yaml)
```yaml
type: IN-MEMORY
config:
  max_size: 0
  max_item_size: 0
```

These settings are **optional**:

- `max_size`: overall maximum number of bytes cache can contain. The value should be specified with a bytes unit (ie. `250MB`).
- `max_item_size`: maximum size of single item, in bytes. The value should be specified with a bytes unit (ie. `125MB`).

This caching type is part of the [Store component](../components/store.md) component. Refer to that page for more information about flags and configuration definitions.

## [in-memory] Bucket cache

Uses the [base configuration](caching.md/#in-memory-base-configuration).

Additional options to configure various aspects of [chunks](../design.md/#chunk) cache are available:

[embedmd]:# (flags/config_bucket_cache_in_memory.txt yaml)
```yaml
type: IN-MEMORY
config:
  max_size: 0
  max_item_size: 0
chunk_subrange_size: 0
max_chunks_get_range_requests: 0
chunk_object_attrs_ttl: 0s
chunk_subrange_ttl: 0s
blocks_iter_ttl: 0s
metafile_exists_ttl: 0s
metafile_doesnt_exist_ttl: 0s
metafile_content_ttl: 0s
metafile_max_size: 0
```

`config` field for memcached supports all the same configuration as memcached for [index cache](#memcached-index-cache). `addresses` in the config field is a **required** setting

Additional options to configure various aspects of [chunks](../design.md/#chunk) cache are available:

- `chunk_subrange_size`: size of segment of [chunks](../design.md/#chunk) object that is stored to the cache. This is the smallest unit that chunks cache is working with.
- `max_chunks_get_range_requests`: how many "get range" sub-requests may cache perform to fetch missing subranges.
- `chunk_object_attrs_ttl`: how long to keep information about [chunk file](../design.md/#chunk-file) attributes (e.g. size) in the cache.
- `chunk_subrange_ttl`: how long to keep individual subranges in the cache.

Following options are used for metadata caching (meta.json files, deletion mark files, iteration result):

- `blocks_iter_ttl`: how long to cache result of iterating blocks.
- `metafile_exists_ttl`: how long to cache information about whether meta.json or deletion mark file exists.
- `metafile_doesnt_exist_ttl`: how long to cache information about whether meta.json or deletion mark file doesn't exist.
- `metafile_content_ttl`: how long to cache content of meta.json and deletion mark files.
- `metafile_max_size`: maximum size of cached meta.json and deletion mark file. Larger files are not cached.

The yml structure for setting the in memory cache configs for caching bucket are the same as the [in-memory index cache](https://thanos.io/tip/components/store.md/#in-memory-index-cache) and all the options to configure Caching Buket mentioned above can be used.

Note that chunks and metadata cache is an experimental feature, and these fields may be renamed or removed completely in the future.

This caching type is part of the [Store component](../components/store.md) component. Refer to that page for more information about flags and configuration definitions.

## [in-memory] Query frontend

Uses the [base configuration](caching.md/#in-memory-base-configuration) with an extra option:

`validity` specifies cache valid time , If set to 0s, so using a default of 24 hours expiration time.

[embedmd]:# (flags/config_response_cache_in_memory.txt yaml)
```yaml
type: IN-MEMORY
config:
  max_size: ""
  max_size_items: 0
  validity: 0s
```

`max_size: ` Maximum memory size of the cache in bytes. A unit suffix (KB, MB, GB) may be applied.

**_NOTE:** If both `max_size` and `max_size_items` are not set, then the *cache* would not be created.

If either of `max_size` or `max_size_items` is set, then there is not limit on other field.
For example - only set `max_size_item` to 1000, then `max_size` is unlimited. Similarly, if only `max_size` is set, then `max_size_items` is unlimited.

Example configuration: [kube-thanos](https://github.com/thanos-io/kube-thanos/blob/master/examples/all/manifests/thanos-query-frontend-deployment.yaml#L50-L54)

This caching type is part of the [Memcached Query frontend](../components/query-frontend.md/) component. Refer to that page for more information about flags and configuration definitions.

# Tips & Tricks

When implementing memcached as caching backend, one should be familiar with such setup. There are some key points to understand when configuring for memcached

## How to implement memcached node addresses

The `addresses` is an array configuration option. Such as:

```yaml
  addresses:
    - localhost:11211
```

or

```yaml
  addresses:
    - redis-1:11211
    - redis-2:11211
```

The Thanos memcached client does **not** support autodiscovery of memcached instances. Therefore it is vital to either define each memcached host by itself or via `dnssrv`.

## Do not use a Loadbalancer as address

If a loadbalancer is used, or for example the cluster endpoint of a cloud service, round-robins will happen. This causes that data has to be posted for each node and your cache-hit rate and performance will suffer.

Therefore one should always use `node` endpoints and not `cluster` endpoints for memcached. On Kubernetes one could implement a `headless` service and use `dnssrv` to fetch each node endpoint. For example:

```yaml
  addresses:
    - dnssrv+_grpc._tcp.my-memcached.memcached.cluster.local
```

Syntax:

```yaml
dnssrv+_{service-port-name}._{service-protocol}.{service-name}.{namespace}.cluster.local
```

A Kubernetes headless service would look like this:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: memcached-service
  namespace: caching
spec:
  clusterIP: None
  selector:
    app: memcached
  ports:
    - name: memcached
      protocol: TCP
      port: 11211
      targetPort: 11211
```

Which would result in the following address:

`dnssrv+_memcached._tcp.memcached-service.memcached.cluster.local`

## Max item size

The Thanos configuration option: `max_item_size` such as:

```yaml
 max_item_size: 1MiB
```

should be lower or equal to the max item size that memcached allows.

Memcached can be configured to allow larger items sizes than the default `1MiB` by setting the `-I` flag, such as `-I 16M` to allow up to `16MiB`

It is not required to increase this, as Thanos simply ignores larger objects. This however decreases the cache hitrate. One could use the metric `thanos_memcached_operation_skipped_total` to observe this behaviour.

## One or multiple memcached instances?

There is no technical limit on using just one instance as backend for all the Thanos caching components. However it might be useful to spread the load over seperated instances to lower the impact in case of incidents.

It however is not possible to use one memcached instance for multiple store backends as this provides conflicts on index keys.

## In-memory versus memcached

- In-memory causes each replica to have it's own cache. If multiple replica's are required it could be more logical by cost/benefit to implement memcached.
- Memcached adds extra complexity which should be used in consideration.
