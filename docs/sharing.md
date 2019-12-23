---
title: Sharing
type: docs
menu: thanos
slug: /Sharing.md
---

# Store and Compactor Sharding

Store and Compactor Sharding is for Long Term Retention Storage.

## Background

Currently all components that read from object store assume that all the operations and functionality should be done based
on **all** the available blocks that are present in the certain bucket's root directory.

This is in most cases totally fine, however with time and allowance of storing blocks from multiple `Sources` into the same bucket,
the number of objects in a bucket can grow drastically.

This means that with time you might want to scale out certain components e.g:

* Compactor: Larger number of objects does not matter much, however compactor has to scale (CPU, network) with number of Sources pushing blocks to the object storage.

### Relabelling

Similar to [promtail](https://github.com/grafana/loki/blob/master/docs/promtail.md#scrape-configs) this config will follow native
[Prometheus relabel-config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config) syntax.

The relabel config defines filtering process done on **every** synchronization with object storage.

We will allow potentially manipulating with several of inputs:

* External labels:
  * `<name>`

Output:

* If output is empty, drop block.

By default, on empty relabel-config, all external labels are assumed.

Example usages would be:

* Drop blocks which contains external labels cluster=A

```yaml
- action: drop
  regex: "A"
  source_labels:
  - cluster
```

* Keep only blocks which contains external labels cluster=A
```yaml
- action: keep
  regex: "A"
  source_labels:
  - cluster
```

By controling which labels should be included in the blocks, we can do sharing.
