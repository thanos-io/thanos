
---
title: Caching
type: docs
menu: thanos
---

# Background

Thanos can implement caching layers on multiple components. In general this should speed-up certain processes. There is no definitive guide on how one should setup an entire Thanos stack as each cluster and use-case is different. Especially what KPI's<sup>1</sup>, SLO's<sup>2</sup> and SLI's<sup>3</sup> matter to you, your customers and/or your stack should have influence on the setup.

However it is possible to give guidance on what each component tries to achieve, what it is used for and how you could configure this.

<sub>
- service level agreement
- service level objective
- service level indicator
</sub>

# Types of cache

| Caching type                                                                                           | Component           | Backends             | Configuration | Extra             |
| -------------------------------------------------------------------------------------------------- | ------------------ | --------------------- | ----------------- | ----------------------- |
| Index cache | [Store gateway](components/store.md) | `in_memory` and `memcached` | base configuration | Enabled by default `in_memory`
| Bucket cache | [Store gateway](components/store.md) | `memcached` only | base configuration + specific bucket cache extra's | Has extra metadata and chunk configuration options
| Query frontend cache | [Query-frontend](components/query-frontend.md)  | `in_memory` and `memcached` | base configuration + extra field `expiration` | `max_item_size` is currently not supported


## Index cache

Thanos Store Gateway supports an index cache to speed up postings and series lookups from TSDB blocks indexes

## Bucket cache

Thanos Store Gateway supports a “caching bucket” with chunks and metadata caching to speed up loading of chunks from TSDB blocks.

## Query frontend cache

Query Frontend supports caching query results and reuses them on subsequent queries. If the cached results are incomplete, Query Frontend calculates the required subqueries and executes them in parallel on downstream queriers. Query Frontend can optionally align queries with their step parameter to improve the cacheability of the query results.

# Configuration for memcached

## Base configuration

Each component can be configured with the following base configuration.

```yaml
type: MEMCACHED # Case-insensitive
config:
  addresses: []
  timeout: 500ms
  max_idle_connections: 100
  max_async_concurrency: 20
  max_async_buffer_size: 10000
  max_item_size: 1MiB
  max_get_multi_concurrency: 100
  max_get_multi_batch_size: 0
  dns_provider_update_interval: 10s
```

## Index cache specific for memcached

The index cache has no exceptions on the base configuration

## Caching bucket specific for memcached

Additional options to configure various aspects of [chunks](../design.md/#chunk) cache are available:

- `chunk_subrange_size`: size of segment of [chunks](../design.md/#chunk) object that is stored to the cache. This is the smallest unit that chunks cache is working with.
- `max_chunks_get_range_requests`: how many "get range" sub-requests may cache perform to fetch missing subranges.
- `chunk_object_attrs_ttl`: how long to keep information about [chunk file](../design.md/#chunk-file) attributes (e.g. size) in the cache.
- `chunk_subrange_ttl`: how long to keep individual subranges in the cache.

Following options are used for metadata caching (`meta.json` files, deletion mark files, iteration result):

- `blocks_iter_ttl`: how long to cache result of iterating blocks.
- `metafile_exists_ttl`: how long to cache information about whether meta.json or deletion mark file exists.
- `metafile_doesnt_exist_ttl`: how long to cache information about whether `meta.json` or deletion mark file doesn't exist.
- `metafile_content_ttl`: how long to cache content of `meta.json` and deletion mark files.
- `metafile_max_size`: maximum size of cached `meta.json` and deletion mark file. Larger files are not cached.

A full example would be: 


```yaml
type: MEMCACHED # Case-insensitive
config:
  addresses: []
  timeout: 500ms
  max_idle_connections: 100
  max_async_concurrency: 20
  max_async_buffer_size: 10000
  max_item_size: 1MiB
  max_get_multi_concurrency: 100
  max_get_multi_batch_size: 0
  dns_provider_update_interval: 10s
chunk_subrange_size: 16000
max_chunks_get_range_requests: 3
chunk_object_attrs_ttl: 24h
chunk_subrange_ttl: 24h
blocks_iter_ttl: 5m
metafile_exists_ttl: 2h
metafile_doesnt_exist_ttl: 15m
metafile_content_ttl: 24h
metafile_max_size: 1MiB  
```

## Query frontend specific

`expiration` specifies how long memcached itself keeps items. After that time, memcached evicts those items. `0s` means the default duration of `24h`.

Full example:

```yaml
type: MEMCACHED # Case-insensitive
config:
  addresses: []
  timeout: 500ms
  max_idle_connections: 100
  max_async_concurrency: 20
  max_async_buffer_size: 10000
  max_item_size: 1MiB
  max_get_multi_concurrency: 100
  max_get_multi_batch_size: 0
  dns_provider_update_interval: 10s
  expiration: 0s  
 ``` 


# Base configuration for in-memory

```yaml
type: IN-MEMORY
config:
  max_size: 0
  max_item_size: 0
```

## Index cache specific for in-memory

The index cache has no exceptions on the base configuration

## Bucket cache specific for in-memory

`in-memory` bucket cache is not supported. This is only possible with `memcached` as backend.

## Query frontend specific for in-memory

`validity` specifies cache valid time , If set to 0s, so using a default of 24 hours expiration time

Full example:

```yaml
type: IN-MEMORY
config:
  max_size: 0
  max_item_size: 0
  validity: 0s
```


# Notes about Memcached configuration

When implementing memcached as caching backend, one should be familiar with such setup. There are some key points to understand when configuring for memcached

## How to set addresses

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

**Do not use a Loadbalancer as address**

If a loadbalancer is used, or for example the cluster endpoint of a cloud service, round-robins will happen. This causes that data has to be posted for each node and your cache-hit rate and performance will suffer.

Therefore one should always use `node` endpoints and not `cluster` endpoints for memcached. On Kubernetes one could implement a `headless` service and use `dnssrv` to fetch each node endpoint. For example:

```yaml
  addresses:
    - dnssrv+_grpc._tcp.my-memcached.memcached.cluster.local
```

Syntax:

```
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

```
dnssrv+_memcached._tcp.memcached-service.memcached.cluster.local
```

# Max item size

The Thanos configuration option: ` max_item_size` such as:

```yaml
 max_item_size: 1MiB
```
should be lower or equal to the max item size that memcached allows. 

Memcached can be configured to allow larger items sizes than the default `1MiB` by setting the `-I` flag, such as `-I 16M` to allow up to `16MiB`

It is not required to increase this, as Thanos simply ignores larger objects. This however decreases the cache hitrate. One could use the metric `thanos_memcached_operation_skipped_total` to observe this behaviour.
