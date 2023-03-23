# Quick Tutorial

Check out the free, in-browser interactive tutorial [Killercoda Thanos course](https://killercoda.com/thanos). We will be progressively updating our Killercoda course with more scenarios.

On top of this, find our quick tutorial below.

## Prometheus

Thanos is based on Prometheus. With Thanos, Prometheus always remains as an integral foundation for collecting metrics and alerting using local data.

Thanos bases itself on vanilla [Prometheus](https://prometheus.io/). We plan to support *all* Prometheus versions beyond v2.2.1.

NOTE: It is highly recommended to use Prometheus v2.13.0+ due to its remote read improvements.

Always make sure to run Prometheus as recommended by the Prometheus team:

* Put Prometheus in the same failure domain. This means in the same network and in the same geographic location as the monitored services.
* Use a persistent disk to persist data across Prometheus restarts.
* Use local compaction for longer retentions.
* Do not change the minimum TSDB block durations.
* Do not scale out Prometheus unless necessary. A single Prometheus instance is already efficient.

We recommend using Thanos when you need to scale out your Prometheus instance.

## Components

Following the [KISS](https://en.wikipedia.org/wiki/KISS_principle) and Unix philosophies, Thanos is comprised of a set of components where each fulfills a specific role.

* Sidecar: connects to Prometheus, reads its data for query and/or uploads it to cloud storage.
* Store Gateway: serves metrics inside of a cloud storage bucket.
* Compactor: compacts, downsamples and applies retention on the data stored in the cloud storage bucket.
* Receiver: receives data from Prometheus's remote write write-ahead log, exposes it, and/or uploads it to cloud storage.
* Ruler/Rule: evaluates recording and alerting rules against data in Thanos for exposition and/or upload.
* Querier/Query: implements Prometheus's v1 API to aggregate data from the underlying components.
* Query Frontend: implements Prometheus's v1 API to proxy it to Querier while caching the response and optionally splitting it by queries per day.

Deployment with Thanos Sidecar for Kubernetes:

<!---
Source file to copy and edit: https://docs.google.com/drawings/d/1AiMc1qAjASMbtqL6PNs0r9-ynGoZ9LIAtf0b9PjILxw/edit?usp=sharing
-->

![Sidecar](https://docs.google.com/drawings/d/e/2PACX-1vSJd32gPh8-MC5Ko0-P-v1KQ0Xnxa0qmsVXowtkwVGlczGfVW-Vd415Y6F129zvh3y0vHLBZcJeZEoz/pub?w=960&h=720)

Deployment via Receive in order to scale out or integrate with other remote write-compatible sources:

<!---
Source file to copy and edit: https://docs.google.com/drawings/d/1iimTbcicKXqz0FYtSfz04JmmVFLVO9BjAjEzBm5538w/edit?usp=sharing
-->

![Receive](https://docs.google.com/drawings/d/e/2PACX-1vRdYP__uDuygGR5ym1dxBzU6LEx5v7Rs1cAUKPsl5BZrRGVl5YIj5lsD_FOljeIVOGWatdAI9pazbCP/pub?w=960&h=720)

### Sidecar

Thanos integrates with existing Prometheus servers as a [sidecar process](https://docs.microsoft.com/en-us/azure/architecture/patterns/sidecar#solution), which runs on the same machine or in the same pod as the Prometheus server.

The purpose of Thanos Sidecar is to back up Prometheus's data into an object storage bucket, and give other Thanos components access to the Prometheus metrics via a gRPC API.

Sidecar makes use of Prometheus's `reload` endpoint. Make sure it's enabled with the flag `--web.enable-lifecycle`.

[Sidecar component documentation](components/sidecar.md)

### External Storage

The following configures Sidecar to write Prometheus's data into a configured object storage bucket:

```bash
thanos sidecar \
    --tsdb.path            /var/prometheus \          # TSDB data directory of Prometheus
    --prometheus.url       "http://localhost:9090" \  # Be sure that Sidecar can use this URL!
    --objstore.config-file bucket_config.yaml \       # Storage configuration for uploading data
```

The exact format of the YAML file depends on the provider you choose. Configuration examples and an up-to-date list of the storage types that Thanos supports are available [here](storage.md).

Rolling this out has little to no impact on the running Prometheus instance. This allows you to ensure you are backing up your data while figuring out the other pieces of Thanos.

If you are not interested in backing up any data, the `--objstore.config-file` flag can simply be omitted.

* *[Example Kubernetes manifests using Prometheus operator](https://github.com/coreos/prometheus-operator/tree/master/example/thanos)*
* *[Example Deploying Sidecar using official Prometheus Helm Chart](../tutorials/kubernetes-helm/README.md)*
* *[Details & Config for other object stores](storage.md)*

### Store API

The Sidecar component implements and exposes a gRPC *[Store API](https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/rpc.proto#L27)*. This implementation allows you to query the metric data stored in Prometheus.

Let's extend the Sidecar from the previous section to connect to a Prometheus server, and expose the Store API:

```bash
thanos sidecar \
    --tsdb.path                 /var/prometheus \
    --objstore.config-file      bucket_config.yaml \       # Bucket config file to send data to
    --prometheus.url            http://localhost:9090 \    # Location of the Prometheus HTTP server
    --http-address              0.0.0.0:19191 \            # HTTP endpoint for collecting metrics on Sidecar
    --grpc-address              0.0.0.0:19090              # GRPC endpoint for StoreAPI
```

* *[Example Kubernetes manifests using Prometheus operator](https://github.com/coreos/prometheus-operator/tree/master/example/thanos)*

### Uploading Old Metrics

When Sidecar is run with the `--shipper.upload-compacted` flag, it will sync all older existing blocks from Prometheus local storage on startup.

NOTE: This assumes you never run the Sidecar with block uploading against this bucket. Otherwise, you must manually remove overlapping blocks from the bucket. Those mitigations will be suggested in the sidecar verification process.

### External Labels

Prometheus allows the configuration of "external labels" of a given Prometheus instance. These are meant to globally identify the role of that instance. As Thanos aims to aggregate data across all instances, providing a consistent set of external labels becomes crucial!

Every Prometheus instance must have a globally unique set of identifying labels. For example, in Prometheus's configuration file:

```yaml
global:
  external_labels:
    region: eu-west
    monitor: infrastructure
    replica: A
```

## Querier/Query

Now that we have setup Sidecar for one or more Prometheus instances, we want to use Thanos's global [Query Layer](components/query.md) to evaluate PromQL queries against all instances at once.

The Querier component is stateless and horizontally scalable, and can be deployed with any number of replicas. Once connected to Thanos Sidecar, it automatically detects which Prometheus servers need to be contacted for a given PromQL query.

Thanos Querier also implements Prometheus's official HTTP API and can thus be used with external tools such as Grafana. It also serves a derivative of Prometheus's UI for ad-hoc querying and checking the status of the Thanos stores.

Below, we will set up a Thanos Querier to connect to our Sidecars, and expose its HTTP UI:

```bash
thanos query \
    --http-address 0.0.0.0:19192 \                                # HTTP Endpoint for Thanos Querier UI
    --store        1.2.3.4:19090 \                                # Static gRPC Store API Address for the query node to query
    --store        1.2.3.5:19090 \                                # Also repeatable
    --store        dnssrv+_grpc._tcp.thanos-store.monitoring.svc  # Supports DNS A & SRV records
```

Go to the configured HTTP address, which should now show a UI similar to that of Prometheus. You can now query across all Prometheus instances within the cluster. You can also check out the Stores page, which shows all of your stores.

[Query documentation](components/query.md)

### Deduplicating Data from Prometheus HA Pairs

The Querier component is also capable of deduplicating data collected from Prometheus HA pairs. This requires configuring Prometheus's `global.external_labels` configuration block to identify the role of a given Prometheus instance.

A typical configuration uses the label name "replica" with whatever value you choose. For example, you might set up the following in Prometheus's configuration file:

```yaml
global:
  external_labels:
    region: eu-west
    monitor: infrastructure
    replica: A
# ...
```

In a Kubernetes stateful deployment, the replica label can also be the pod name.

Ensure your Prometheus instances have been reloaded with the configuration you defined above. Then, in Thanos Querier, we will define `replica` as the label we want to enable deduplication on:

```bash
thanos query \
    --http-address        0.0.0.0:19192 \
    --store               1.2.3.4:19090 \
    --store               1.2.3.5:19090 \
    --query.replica-label replica          # Replica label for deduplication
    --query.replica-label replicaX         # Supports multiple replica labels for deduplication
```

Go to the configured HTTP address, and you should now be able to query across all Prometheus instances and receive deduplicated data.

* *[Example Kubernetes manifest](https://github.com/thanos-io/kube-thanos/blob/master/manifests/thanos-query-deployment.yaml)*

### Communication Between Components

The only required communication between nodes is for a Thanos Querier to be able to reach the gRPC Store APIs that you provide. Thanos Querier periodically calls the info endpoint to collect up-to-date metadata as well as check the health of a given Store API. That metadata includes the information about time windows and external labels for each node.

There are various ways to tell Thanos Querier about the Store APIs it should query data from. The simplest way is to use a static list of well known addresses to query. These are repeatable, so you can add as many endpoints as you need. You can also put a DNS domain prefixed by `dns+` or `dnssrv+` to have a Thanos Querier do an `A` or `SRV` lookup to get all the required IPs it should communicate with.

```bash
thanos query \
    --http-address 0.0.0.0:19192 \              # Endpoint for Thanos Querier UI
    --grpc-address 0.0.0.0:19092 \              # gRPC endpoint for Store API
    --store        1.2.3.4:19090 \              # Static gRPC Store API Address for the query node to query
    --store        1.2.3.5:19090 \              # Also repeatable
    --store        dns+rest.thanos.peers:19092  # Use DNS lookup for getting all registered IPs as separate Store APIs
```

Read more details [here](service-discovery.md).

* *[Example Kubernetes manifests using Prometheus operator](https://github.com/coreos/prometheus-operator/tree/master/example/thanos)*

## Store Gateway

As Thanos Sidecar backs up data into the object storage bucket of your choice, you can decrease Prometheus's retention in order to store less data locally. However, we need a way to query all that historical data again. Store Gateway does just that, by implementing the same gRPC data API as Sidecar, but backing it with data it can find in your object storage bucket. Just like sidecars and query nodes, Store Gateway exposes a Store API and needs to be discovered by Thanos Querier.

```bash
thanos store \
    --data-dir             /var/thanos/store \   # Disk space for local caches
    --objstore.config-file bucket_config.yaml \  # Bucket to fetch data from
    --http-address         0.0.0.0:19191 \       # HTTP endpoint for collecting metrics on the Store Gateway
    --grpc-address         0.0.0.0:19090         # GRPC endpoint for StoreAPI
```

Store Gateway uses a small amount of disk space for caching basic information about data in the object storage bucket. This will rarely exceed more than a few gigabytes and is used to improve restart times. It is useful but not required to preserve it across restarts.

* *[Example Kubernetes manifest](https://github.com/thanos-io/kube-thanos/blob/master/manifests/thanos-store-statefulSet.yaml)*

[Store Gateway documentation](components/store.md)

## Compactor

A local Prometheus installation periodically compacts older data to improve query efficiency. Since Sidecar backs up data into an object storage bucket as soon as possible, we need a way to apply the same process to data in the bucket.

Thanos Compactor simply scans the object storage bucket and performs compaction where required. At the same time, it is responsible for creating downsampled copies of data in order to speed up queries.

```bash
thanos compact \
    --data-dir             /var/thanos/compact \  # Temporary workspace for data processing
    --objstore.config-file bucket_config.yaml \   # Bucket where compacting will be performed
    --http-address         0.0.0.0:19191          # HTTP endpoint for collecting metrics on the compactor
```

Compactor is not in the critical path of querying or data backup. It can either be run as a periodic batch job or be left running to always compact data as soon as possible. It is recommended to provide 100-300GB of local disk space for data processing.

*NOTE: Compactor must be run as a **singleton** and must not run when manually modifying data in the bucket.*

* *[Example Kubernetes manifest](https://github.com/thanos-io/kube-thanos/blob/master/examples/all/manifests/thanos-compact-statefulSet.yaml)*

[Compactor documentation](components/compact.md)

## Ruler/Rule

In case Prometheus running with Thanos Sidecar does not have enough retention, or if you want to have alerts or recording rules that require a global view, Thanos has just the component for that: the [Ruler](components/rule.md), which does rule and alert evaluation on top of a given Thanos Querier.

[Rule documentation](components/rule.md)
