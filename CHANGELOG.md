# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

NOTE: As semantic versioning states all 0.y.z releases can contain breaking changes in API (flags, grpc API, any backward compatibility)

We use *breaking* word for marking changes that are not backward compatible (relates only to v0.y.z releases.)

## Unreleased

## [v0.4.0-rc.1](https://github.com/improbable-eng/thanos/releases/tag/v0.4.0-rc.1) - 2019.04.26

:warning: **IMPORTANT** :warning: This is the last release that supports gossip. From Thanos v0.5.0, gossip will be completely removed.

This release also disables gossip mode by default for all components.
See [this](docs/proposals/approved/201809_gossip-removal.md) for more details.

### Added

- [#811](https://github.com/improbable-eng/thanos/pull/811) Remote write receiver component :heart: :heart: thanks to RedHat (@brancz) contribution.
- [#910](https://github.com/improbable-eng/thanos/pull/910) Query's stores UI page is now sorted by type and old DNS or File SD stores are removed after 5 minutes (configurable via the new `--store.unhealthy-timeout=5m` flag).
- [#905](https://github.com/improbable-eng/thanos/pull/905) Thanos support for Query API: /api/v1/labels. Notice that the API was added in Prometheus v2.6.
- [#798](https://github.com/improbable-eng/thanos/pull/798) Ability to limit the maximum number of concurrent request to Series() calls in Thanos Store and the maximum amount of samples we handle.
- [#1060](https://github.com/improbable-eng/thanos/pull/1060) Allow specifying region attribute in S3 storage configuration

:warning: **WARNING** :warning: #798 adds a new default limit to Thanos Store: `--store.grpc.series-max-concurrency`. Most likely you will want to make it the same as `--query.max-concurrent` on Thanos Query.

New options:

  New Store flags:
  
    * `--store.grpc.series-sample-limit` limits the amount of samples that might be retrieved on a single Series() call. By default it is 0. Consider enabling it by setting it to more than 0 if you are running on limited resources.
    * `--store.grpc.series-max-concurrency` limits the number of concurrent Series() calls in Thanos Store. By default it is 20. Considering making it lower or bigger depending on the scale of your deployment.

  New Store metrics:
  
    * `thanos_bucket_store_queries_dropped_total` shows how many queries were dropped due to the samples limit;
    * `thanos_bucket_store_queries_concurrent_max` is a constant metric which shows how many Series() calls can concurrently be executed by Thanos Store;
    * `thanos_bucket_store_queries_in_flight` shows how many queries are currently "in flight" i.e. they are being executed;
    * `thanos_bucket_store_gate_duration_seconds` shows how many seconds it took for queries to pass through the gate in both cases - when that fails and when it does not.
    
  New Store tracing span:
    * `store_query_gate_ismyturn` shows how long it took for a query to pass (or not) through the gate.
    
- [#1016](https://github.com/improbable-eng/thanos/pull/1016) Added option for another DNS resolver (miekg/dns client). 
Note that this is required to have SRV resolution working on [Golang 1.11+ with KubeDNS below v1.14](https://github.com/golang/go/issues/27546)

   New Querier and Ruler flag: `-- store.sd-dns-resolver` which allows to specify resolver to use. Either `golang` or `miekgdns`
   
- [#986](https://github.com/improbable-eng/thanos/pull/986) Allow to save some startup & sync time in store gateway as it is no longer needed to compute index-cache from block index on its own for larger blocks.
  The store Gateway still can do it, but it first checks bucket if there is index-cached uploaded already. 
  In the same time, compactor precomputes the index cache file on every compaction. 

  New Compactor flag: `--index.generate-missing-cache-file` was added to allow quicker addition of index cache files. If enabled it precomputes missing files on compactor startup. Note that it will take time and it's only one-off step per bucket.

- [#887](https://github.com/improbable-eng/thanos/pull/887) Compact: Added new `--block-sync-concurrency` flag, which allows you to configure number of goroutines to use when syncing block metadata from object storage.
- [#928](https://github.com/improbable-eng/thanos/pull/928) Query: Added `--store.response-timeout` flag. If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.
- [#893](https://github.com/improbable-eng/thanos/pull/893) S3 storage backend has graduated to `stable` maturity level.
- [#936](https://github.com/improbable-eng/thanos/pull/936) Azure storage backend has graduated to `stable` maturity level.
- [#937](https://github.com/improbable-eng/thanos/pull/937) S3: added trace functionality. You can add `trace.enable: true` to enable the minio client's verbose logging.
- [#953](https://github.com/improbable-eng/thanos/pull/953) Compact: now has a hidden flag `--debug.accept-malformed-index`. Compaction index verification will ignore out of order label names.
- [#963](https://github.com/improbable-eng/thanos/pull/963) GCS: added possibility to inline ServiceAccount into GCS config.
- [#1010](https://github.com/improbable-eng/thanos/pull/1010) Compact: added new flag `--compact.concurrency`. Number of goroutines to use when compacting groups.
- [#1028](https://github.com/improbable-eng/thanos/pull/1028) Query: added `--query.default-evaluation-interval`, which sets default evaluation interval for sub queries.
- [#980](https://github.com/improbable-eng/thanos/pull/980) Ability to override Azure storage endpoint for other regions (China)
- [#1021](https://github.com/improbable-eng/thanos/pull/1021) Query API `series` now supports POST method.
- [#939](https://github.com/improbable-eng/thanos/pull/939) Query API `query_range` now supports POST method.

### Changed 

- [#970](https://github.com/improbable-eng/thanos/pull/970) Deprecated `partial_response_disabled` proto field. Added `partial_response_strategy` instead. Both in gRPC and Query API.
  No `PartialResponseStrategy` field for `RuleGroups` by default means `abort` strategy (old PartialResponse disabled) as this is recommended option for Rules and alerts.

  Metrics:
    
    * Added `thanos_rule_evaluation_with_warnings_total` to Ruler.
    * DNS `thanos_ruler_query_apis*` are now `thanos_ruler_query_apis_*` for consistency.
    * DNS `thanos_querier_store_apis*` are now `thanos_querier_store_apis__*` for consistency.
    * Query Gate `thanos_bucket_store_series*` are now `thanos_bucket_store_series___*` for consistency.
    * Most of thanos ruler metris related to rule manager has `strategy` label.
  
  Ruler tracing spans:
  
    * `/rule_instant_query HTTP[client]` is now `/rule_instant_query_part_resp_abort HTTP[client]"` if request is for abort strategy.
    
- [#1009](https://github.com/improbable-eng/thanos/pull/1009): Upgraded Prometheus (~v2.7.0-rc.0 to v2.8.1)  and TSDB (`v0.4.0` to `v0.6.1`) deps.
  
  Changes that affects Thanos:
   * query: 
     * [ENHANCEMENT] In histogram_quantile merge buckets with equivalent le values. #5158.   
     * [ENHANCEMENT] Show list of offending labels in the error message in many-to-many scenarios. #5189   
     * [BUGFIX] Fix panic when aggregator param is not a literal. #5290
   * ruler: 
     * [ENHANCEMENT] Reduce time that Alertmanagers are in flux when reloaded. #5126
     * [BUGFIX] prometheus_rule_group_last_evaluation_timestamp_seconds is now a unix timestamp. #5186
     * [BUGFIX] prometheus_rule_group_last_duration_seconds now reports seconds instead of nanoseconds. Fixes our [issue #1027](https://github.com/improbable-eng/thanos/issues/1027)
     * [BUGFIX] Fix sorting of rule groups. #5260
   * store: [ENHANCEMENT] Fast path for EmptyPostings cases in Merge, Intersect and Without.
   * tooling: [FEATURE] New dump command to tsdb tool to dump all samples.
   * compactor: [ENHANCEMENT] When closing the db any running compaction will be cancelled so it doesn't block.
  
  For ruler essentially whole TSDB CHANGELOG applies beween v0.4.0-v0.6.1: https://github.com/prometheus/tsdb/blob/master/CHANGELOG.md
  
  Note that this was added on TSDB and Prometheus: [FEATURE] Time-ovelapping blocks are now allowed. #370
  Whoever due to nature of Thanos compaction (distributed systems), for safety reason this is disabled for Thanos compactor for now.
 
- [#868](https://github.com/improbable-eng/thanos/pull/868) Go has been updated to 1.12.
- [#1055](https://github.com/improbable-eng/thanos/pull/1055) Gossip flags are now disabled by default and deprecated. 
- [#964](https://github.com/improbable-eng/thanos/pull/964) repair: Repair process now sorts the series and labels within block.
- [#1073](https://github.com/improbable-eng/thanos/pull/1073) Store: index cache for requests. It now calculates the size properly (includes slice header), has anti-deadlock safeguard and reports more metrics.

### Fixed

- [#921](https://github.com/improbable-eng/thanos/pull/921) `thanos_objstore_bucket_last_successful_upload_time` now does not appear when no blocks have been uploaded so far.
- [#966](https://github.com/improbable-eng/thanos/pull/966) Bucket: verify no longer warns about overlapping blocks, that overlap `0s` 
- [#848](https://github.com/improbable-eng/thanos/pull/848) Compact: now correctly works with time series with duplicate labels.
- [#894](https://github.com/improbable-eng/thanos/pull/894) Thanos Rule: UI now correctly shows evaluation time.
- [#865](https://github.com/improbable-eng/thanos/pull/865) Query: now properly parses DNS SRV Service Discovery.
- [#889](https://github.com/improbable-eng/thanos/pull/889) Store: added safeguard against merging posting groups segfault
- [#941](https://github.com/improbable-eng/thanos/pull/941) Sidecar: added better handling of intermediate restarts.
- [#933](https://github.com/improbable-eng/thanos/pull/933) Query: Fixed 30 seconds lag of adding new store to query.
- [#962](https://github.com/improbable-eng/thanos/pull/962) Sidecar: Make config reloader file writes atomic. 
- [#982](https://github.com/improbable-eng/thanos/pull/982) Query: now advertises Min & Max Time accordingly to the nodes.
- [#1041](https://github.com/improbable-eng/thanos/issues/1038) Ruler is now able to return long time range queries.
- [#904](https://github.com/improbable-eng/thanos/pull/904) Compact: Skip compaction for blocks with no samples.
- [#1070](https://github.com/improbable-eng/thanos/pull/1070) Downsampling works back again. Deferred closer errors are now properly captured.

## [v0.3.2](https://github.com/improbable-eng/thanos/releases/tag/v0.3.2) - 2019.03.04

### Added

- [#851](https://github.com/improbable-eng/thanos/pull/851) New read API endpoint for api/v1/rules and api/v1/alerts.
- [#873](https://github.com/improbable-eng/thanos/pull/873) Store: fix set index cache LRU

:warning: **WARNING** :warning: #873 fix fixes actual handling of `index-cache-size`. Handling of limit for this cache was
broken so it was unbounded all the time. From this release actual value matters and is extremely low by default. To "revert"
the old behaviour (no boundary), use a large enough value.

### Fixed

- [#833](https://github.com/improbable-eng/thanos/issues/833) Store Gateway matcher regression for intersecting with empty posting.
- [#867](https://github.com/improbable-eng/thanos/pull/867) Fixed race condition in sidecare between reloader and shipper.

## [v0.3.1](https://github.com/improbable-eng/thanos/releases/tag/v0.3.1) - 2019.02.18

### Fixed

- [#829](https://github.com/improbable-eng/thanos/issues/829) Store Gateway crashing due to `slice bounds out of range`.
- [#834](https://github.com/improbable-eng/thanos/issues/834) Store Gateway matcher regression for `<>` `!=`.


## [v0.3.0](https://github.com/improbable-eng/thanos/releases/tag/v0.3.0) - 2019.02.08

### Added

- Support for gzip compressed configuration files before envvar substitution for reloader package.
- `bucket inspect` command for better insights on blocks in object storage.
- Support for [Tencent COS](docs/storage.md#tencent-cos-configuration) object storage.
- Partial Response disable option for StoreAPI and QueryAPI.
- Partial Response disable button on Thanos UI
- We have initial docs for goDoc documentation!
- Flags for Querier and Ruler UIs: `--web.route-prefix`, `--web.external-prefix`, `--web.prefix-header`. Details [here](docs/components/query.md#expose-ui-on-a-sub-path)

### Fixed

- [#649](https://github.com/improbable-eng/thanos/issues/649) - Fixed store label values api to add also external label values.
- [#396](https://github.com/improbable-eng/thanos/issues/396) - Fixed sidecar logic for proxying series that has more than 2^16 samples from Prometheus.
- [#732](https://github.com/improbable-eng/thanos/pull/732) - Fixed S3 authentication sequence. You can see new sequence enumerated [here](https://github.com/improbable-eng/thanos/blob/master/docs/storage.md#aws-s3-configuration)
- [#745](https://github.com/improbable-eng/thanos/pull/745) - Fixed race conditions and edge cases for Thanos Querier fanout logic.
- [#651](https://github.com/improbable-eng/thanos/issues/651) - Fixed index cache when asked buffer size is bigger than cache max size.

### Changed

- [#529](https://github.com/improbable-eng/thanos/pull/529) Massive improvement for compactor. Downsampling memory consumption was reduce to only store labels and single chunks per each series.
- Qurerier UI: Store page now shows the store APIs per component type.
- Prometheus and TSDB deps are now up to date with ~2.7.0 Prometheus version. Lot's of things has changed. See details [here #704](https://github.com/improbable-eng/thanos/pull/704) Known changes that affects us:
    - prometheus/prometheus/discovery/file
      - [ENHANCEMENT] Discovery: Improve performance of previously slow updates of changes of targets. #4526
      - [BUGFIX] Wait for service discovery to stop before exiting #4508 ??
    - prometheus/prometheus/promql:
      - **[ENHANCEMENT] Subqueries support. #4831**
      - [BUGFIX] PromQL: Fix a goroutine leak in the lexer/parser. #4858
      - [BUGFIX] Change max/min over_time to handle NaNs properly. #438
      - [BUGFIX] Check label name for `count_values` PromQL function. #4585
      - [BUGFIX] Ensure that vectors and matrices do not contain identical label-sets. #4589
      - [ENHANCEMENT] Optimize PromQL aggregations #4248
      - [BUGFIX] Only add LookbackDelta to vector selectors #4399
      - [BUGFIX] Reduce floating point errors in stddev and related functions #4533
    - prometheus/prometheus/rules:
      - New metrics exposed! (prometheus evaluation!)
      - [ENHANCEMENT] Rules: Error out at load time for invalid templates, rather than at evaluation time. #4537
    - prometheus/tsdb/index: Index reader optimizations.
- Thanos store gateway flag for sync concurrency (`block-sync-concurrency` with `20` default, so no change by default)
- S3 provider:
  - Added `put_user_metadata` option to config.
  - Added `insecure_skip_verify` option to config.

### Deprecated

- Tests against Prometheus below v2.2.1. This does not mean *lack* of support for those. Only that we don't tests the compatibility anymore. See [#758](https://github.com/improbable-eng/thanos/issues/758) for details.

## [v0.2.1](https://github.com/improbable-eng/thanos/releases/tag/v0.2.1) - 2018.12.27

### Added

- Relabel drop for Thanos Ruler to enable replica label drop and alert deduplication on AM side.
- Query: Stores UI page available at `/stores`.

![](./docs/img/query_ui_stores.png)

### Fixed

- Thanos Rule Alertmanager DNS SD bug.
- DNS SD bug when having SRV results with different ports.
- Move handling of HA alertmanagers to be the same as Prometheus.
- Azure iteration implementation flaw.

## [v0.2.0](https://github.com/improbable-eng/thanos/releases/tag/v0.2.0) - 2018.12.10

Next Thanos release adding support to new discovery method, gRPC mTLS and two new object store providers (Swift and Azure).

Note lots of necessary breaking changes in flags that relates to bucket configuration.

### Deprecated

- *breaking*: Removed all bucket specific flags as we moved to config files:
    - --gcs-bucket=\<bucket\>
    - --s3.bucket=\<bucket\>
    - --s3.endpoint=\<api-url\>
    - --s3.access-key=\<key\>
    - --s3.insecure
    - --s3.signature-version2
    - --s3.encrypt-sse
    - --gcs-backup-bucket=\<bucket\>
    - --s3-backup-bucket=\<bucket\>
- *breaking*: Removed support of those environment variables for bucket:
    * S3_BUCKET
    * S3_ENDPOINT
    * S3_ACCESS_KEY
    * S3_INSECURE
    * S3_SIGNATURE_VERSION2
- *breaking*: Removed provider specific bucket metrics e.g `thanos_objstore_gcs_bucket_operations_total` in favor of of generic bucket operation metrics.

### Changed

- *breaking*: Added `thanos_` prefix to memberlist (gossip) metrics. Make sure to update your dashboards and rules.
- S3 provider:
  - Set `"X-Amz-Acl": "bucket-owner-full-control"` metadata for s3 upload operation.

### Added

- Support for heterogeneous secure gRPC on StoreAPI.
- Handling of scalar result in rule node evaluating rules.
- Flag `--objstore.config-file` to reference to the bucket configuration file in yaml format. Detailed information can be found in document [storage](docs/storage.md).
- File service discovery for StoreAPIs:
- In `thanos rule`, static configuration of query nodes via `--query`
- In `thanos rule`, file based discovery of query nodes using `--query.file-sd-config.files`
- In `thanos query`, file based discovery of store nodes using `--store.file-sd-config.files`
- `/-/healthy` endpoint to Querier.
- DNS service discovery to static and file based configurations using the `dns+` and `dnssrv+` prefixes for the respective lookup. Details [here](docs/service-discovery.md)
- `--cluster.disable` flag to disable gossip functionality completely.
- Hidden flag to configure max compaction level.
- Azure Storage.
- OpenStack Swift support.
- Thanos Ruler `thanos_rule_loaded_rules` metric.
- Option for JSON logger format.

### Fixed

- Issue whereby the Proxy Store could end up in a deadlock if there were more than 9 stores being queried and all returned an error.
- Ruler tracing causing panics.
- GatherIndexStats panics on duplicated chunks check.
- Clean up of old compact blocks on compact restart.
- Sidecar too frequent Prometheus reload.
- `thanos_compactor_retries_total` metric not being registered.

## [v0.1.0](https://github.com/improbable-eng/thanos/releases/tag/v0.1.0) - 2018.09.14

Initial version to have a stable reference before [gossip protocol removal](https://github.com/improbable-eng/thanos/blob/master/docs/proposals/gossip-removal.md).

### Added

- Gossip layer for all components.
- StoreAPI gRPC proto.
- TSDB block upload logic for Sidecar.
- StoreAPI logic for Sidecar.
- Config and rule reloader logic for Sidecar.
- On-the fly result merge and deduplication logic for Querier.
- Custom Thanos UI (based mainly on Prometheus UI) for Querier.
- Optimized object storage fetch logic for Store.
- Index cache and chunk pool for Store for better memory usage.
- Stable support for Google Cloud Storage object storage.
- StoreAPI logic for Querier to support Thanos federation (experimental).
- Support for S3 minio-based AWS object storage (experimental).
- Compaction logic of blocks from multiple sources for Compactor.
- Optional Compaction fixed retention.
- Optional downsampling logic for Compactor (experimental).
- Rule (including alerts) evaluation logic for Ruler.
- Rule UI with hot rules reload.
- StoreAPI logic for Ruler.
- Basic metric orchestration for all components.
- Verify commands with potential fixes (experimental).
- Compact / Downsample offline commands.
- Bucket commands.
- Downsampling support for UI.
- Grafana dashboards for Thanos components.
