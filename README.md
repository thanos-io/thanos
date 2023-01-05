## Overview

This repository is a fork of the [thanos-io/thanos](https://github.com/thanos-io/thanos) project. It is used to deploy the metrics stack for Shopify Observe.

## Differences from upstream

This project has several improvements which have been made. Most of them are raised as upstream PRs in order to avoid divergent codebases over time.

#### Parallel compaction

The compactor has been updated to be able to do parallel compaction tasks inside a single compaction group. The change is submitted to the upstream repository: https://github.com/thanos-io/thanos/pull/5936.

#### Latest PromQL engine

The project uses the latest version of the https://github.com/thanos-community/promql-engine module. This helps us deliver the latest query improvements without getting blocked by upstream approvals.

#### Non-blocking deduplication

The Thanos Querier does deduplication in a blocking manner. It first retrieves all series, sorts them without replica labels, and only then starts to deduplicate. This fork has an improvement where deduplication can be done on-the-fly as series are being retrieved from storage.

There are two upstream PRs which address this issue:
* https://github.com/thanos-io/thanos/pull/5796
* https://github.com/thanos-io/thanos/pull/5988

#### Sharded compactor

Large tenants (Kubernetes clusters) produce TSDB blocks which reach the index size limit after a few compaction iterations. As a result, we could not produce blocks of sufficient range for downsampling. The `sharded-compactor` branch contains a version of the Thanos Compactor that is capable of splitting blocks vertically into N independent shards.

The reason why the change is kept in a branch is because it depends on the [mimir-prometheus](https://github.com/grafana/mimir-prometheus) fork of Prometheus. Merging the change in main would lead to the Mimir Prometheus fork being used throughout the entire codebase.
