# Sharding

# Background

By default, all components that read from object store assume that all the operations and functionality should be done based on **all** the available blocks that are present in the given bucket's root directory.

This is in most cases totally fine, however when storing blocks from many `Sources` over long durations in the same bucket the number of objects in a bucket can grow drastically.

This means that with time you might want to scale out certain components e.g:

## Compactor

Larger number of objects does not matter much, however compactor has to scale (CPU, network, disk, memory) with number of Sources pushing blocks to the object storage.

## Store Gateway

Queries against store gateway which are touching large number of blocks (no matter from what Sources) might be expensive, so it can scale with number of blocks.

# Relabelling

Similar to [promtail](https://grafana.com/docs/loki/latest/send-data/promtail/configuration/#relabel_configs) this config follows native [Prometheus relabel-config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config) syntax.

Currently, thanos only supports the following relabel actions:

* keep

* drop

* hashmod
  * on `external labels` for all components

  * on `__block_id` for store gateway, see this [example](https://github.com/observatorium/configuration/blob/bf1304b0d7bce2ae3fefa80412bb358f9aa176fb/environments/openshift/manifests/observatorium-template.yaml#L1514-L1521)

The relabel config is used to filter the relevant blocks every time a Thanos component synchronizes with object storage. If a block is dropped, this means it will be ignored by the component.

Currently, only external labels are allowed as sources. The block will be dropped if the output is an empty label set.

If no relabel-config is given, all external labels will be kept.

Example usages:

* Ignore blocks which contain the external labels `cluster=A`

```yaml
- action: drop
  regex: "A"
  source_labels:
  - cluster
```

* Keep only blocks which contain the external labels `cluster=A`

```yaml
- action: keep
  regex: "A"
  source_labels:
  - cluster
```

We can shard by adjusting which labels should be included in the blocks.

# Time Partitioning

For store gateway, we can specify `--min-time` and `--max-time` flags to filter for what blocks store gateway should be responsible for.

More details can refer to "Time based partitioning" chapter in [Store gateway](components/store.md).
