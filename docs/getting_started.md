# Getting started

At its heart Thanos provides a global query view, data backup, and access historical data as core features. All three are well separated and Thanos deployments can be adapted to make use of each of them individually. This supports using just a subset of its features as well as a gradual rollout that immediately provides some of its benefits.

The following examples configure Thanos to work against a Google Cloud Storage bucket. However, any object storage (S3, HDFS, DigitalOcean Spaces, ...) can be used by using the equivalent flags to connect to the S3 API.

See [this](storage.md) for up-to-date list of available object stores for Thanos.

## Requirements

* One or more [Prometheus](https://prometheus.io) v2.2.1+ installations (v2.0.0 works too but is not recommended)
* golang 1.10+
* An object storage bucket (optional)

## Get Thanos!

Thanos has no official releases yet. With a working installation of the Go [toolchain](https://github.com/golang/tools) (`GOPATH`, `PATH=${GOPATH}/bin:${PATH}`), Thanos can be downloaded and built by running:

```
go get -d github.com/improbable-eng/thanos/...
make
```

The `thanos` binary should now be in your `$PATH` and is the only thing required to deploy any of its components.

## Sidecars

Thanos integrates with existing Prometheus servers through a sidecar process, which runs on same machine/in the same pod as the Prometheus server itself. It only works with Prometheus instances of version 2.0.

The sidecar is responsible for backing up data into an object storage bucket and providing querying access to the underlying Prometheus instance for other Thanos components.

### Backups 

The following configures the sidecar to backup data into a Google Cloud Storage bucket.

```
thanos sidecar \
    --prometheus.url    http://localhost:9090 \     # Prometheus's HTTP address
    --tsdb.path         /var/prometheus \           # Data directory of Prometheus
    --gcs.bucket        example-bucket \            # Bucket to upload data to
```

Rolling this out has little to zero impact on the running Prometheus instance. It is a good start to ensure you are backing up your data while figuring out the other pieces of Thanos.

If you are not interested in backing up any data, the `--gcs.bucket` flag can simply be omitted.

* _[Example Kubernetes manifest](../kube/manifests/prometheus.yaml)_
* _[Example Kubernetes manifest with GCS upload](../kube/manifests/prometheus-gcs.yaml)_
* _[Details & Config for other object stores](./storage.md)_

### Query Access

Thanos comes with a highly efficient gRPC-based Store API for metric data access across all its components. The sidecar implements it in front of its connected Prometheus server. While it is ready to use with the above example, we must additionally configure the sidecar to join a Thanos cluster.

Let's extend the above sidecar to expose their gRPC Store API so that we can query metrics.

```
thanos sidecar \
    --prometheus.url            http://localhost:9090 \
    --tsdb.path                 /var/prometheus \
    --gcs.bucket                example-bucket \
    --grpc-address              0.0.0.0:19091 \            # gRPC endpoint for Store API (will be used to perform PromQL queries)
    --http-address              0.0.0.0:19191 \            # HTTP endpoint for collecting metrics on Thanos sidecar
    --cluster.address           0.0.0.0:19391 \            # Endpoint used to meta data about the current node
    --cluster.advertise-address 127.0.0.1:19391 \          # Location at which the node advertise itself at to other members of the cluster
    --cluster.peers             127.0.0.1:19391 \          # Static cluster peer where the node will get info about the cluster
```

* _[Example Kubernetes manifest](../kube/manifests/prometheus.yaml)_
* _[Example Kubernetes manifest with GCS upload](../kube/manifests/prometheus-gcs.yaml)_

### External Labels

Prometheus allows to configure "external labels" for a Prometheus instance. Those are meant to globally identify the role of a given Prometheus instance. As Thanos aims to aggregate data across all Prometheus servers, providing a consistent of external labels for all Prometheus server becomes crucial!

Every Prometheus instance _must_ have a globally unique set of identifying labels. For example, in Prometheus's configuration file:

```
global:
  external_labels:
    region: eu-west
    monitor: infrastructure
    replica: A
# ...
```

## Query Layer

Now that we have setup the sidecar for one or more Prometheus servers, we want to use Thanos' global query layer to evaluate PromQL queries against all of them at once.

The query component is stateless and horizontally scalable and thus can be deployed with any required amount of replicas. Just like sidecars, it connects to the cluster via gossip protocol and automatically detects which Prometheus servers need to be contacted for a given PromQL query.

It implements Prometheus's official HTTP API and can thus seamlessly be used with external tools such as Grafana. Additionaly it servers a derviative of Prometheus's UI for ad-hoc querying.

```
thanos query \
    --http-address              0.0.0.0:19192 \         # Endpoint for Query UI
    --grpc-address              0.0.0.0:19092 \         # gRPC endpoint for Store API
    --cluster.address           0.0.0.0:19591 \
    --cluster.advertise-address 127.0.0.1:19591 \
    --cluster.peers             127.0.0.1:19391 \       # Static cluster peer where the node will get info about the cluster
    --cluster.peers             127.0.0.1:19392 \       # Another cluster peer (many can be added to discover nodes)
    --store                     0.0.0.0:18091   \       # Static gRPC Store API Address for the query node to query
    --store                     0.0.0.0:18092   \       # Also repeatable
```

The query component is also capable of merging data collected from Prometheus HA pairs. This requires a consistent choice of an external label for Prometheus servers that identifies replicas. Other external labels must be identical. A typical choice is simply the label name "replica" while its value is freely chosable.

Providing the label name to the query component will enable the deduplication.

```
thanos query \
    --http-address              0.0.0.0:19092 \
    --cluster.address           0.0.0.0:19591 \
    --cluster.advertise-address 127.0.0.1:19591 \
    --cluster.peers             127.0.0.1:19391 \
    --query.replica-label       replica \               # Replica label for de-duplication
```

Go to the configured HTTP address that should now show a UI similar to that of Prometheus itself. If the cluster formed correctly you can now query data across all Prometheus servers within the cluster.

* _[Example Kubernetes manifest](../kube/manifests/thanos-query.yaml)_

## Communication Between Components

Components in a Thanos cluster can be connected through a gossip protocol to advertise membership and propagate metadata about other known nodes or by setting static store flags of known components. We added gossip to efficiently and dynamically discover other nodes in the cluster and the metrics information they can access.

This is especially useful for the Query node to know all endpoints to query, time windows and external labels for each node, thus reducing the overhead of querying all nodes in the cluster.

Given a sidecar we can have it join a gossip cluster by advertising itself to other peers within the network.

```
thanos sidecar \
    --prometheus.url            http://localhost:9090 \
    --tsdb.path                 /var/prometheus \
    --gcs.bucket                example-bucket \
    --grpc-address              0.0.0.0:19091 \            # gRPC endpoint for Store API (will be used to perform PromQL queries)
    --http-address              0.0.0.0:19191 \            # HTTP endpoint for collecting metrics on Thanos sidecar
    --cluster.address           0.0.0.0:19391 \            # Endpoint used to meta data about the current node
    --cluster.advertise-address 127.0.0.1:19391 \          # Location at which the node advertise itself at to other members of the cluster
    --cluster.peers             127.0.0.1:19391 \          # Static cluster peer where the node will get info about the cluster (repeatable)
```

With the above configuration a single node will advertise itself in the cluster and query for other members of the cluster (from itself) when you add more sidecars / components you will probably want to sent `cluset.peers` to a well known peer that will allow you to discover other peers within the cluster.

When a peer advertises itself / joins a gossip cluster it sends information about all the peers it currently knows about (including itself). This information for each peer allows you to see what type of component a peer is (Source, Store, Query), the peers Store API address (used for querying) and meta data about the external labels and time window the peer holds information about.

Once the Peer joins the cluster it will periodically update the information it sends out with new / updated information about other peers and the time windows for the metrics that it can access.

```
thanos query \
    --http-address              0.0.0.0:19192 \         # Endpoint for Query UI
    --grpc-address              0.0.0.0:19092 \         # gRPC endpoint for Store API
    --cluster.address           0.0.0.0:19591 \
    --cluster.advertise-address 127.0.0.1:19591 \
    --cluster.peers             127.0.0.1:19391 \       # Static cluster peer where the node will get info about the cluster
```

The Query component however does not have to utilize gossip to discover other nodes and instead can be setup to use a static list of well known addresses to query. These are repeatable so can add as many endpoint as needed. However, if you only use `store` you will automatically discover nodes added to the cluster.

```
thanos query \
    --http-address              0.0.0.0:19192 \         # Endpoint for Query UI
    --grpc-address              0.0.0.0:19092 \         # gRPC endpoint for Store API
    --store                     0.0.0.0:19091   \       # Static gRPC Store API Address for the query node to query
    --store                     0.0.0.0:19092   \       # Also repeatable
```

You can mix both static `store` and `cluster` based approaches:

```
thanos query \
    --http-address              0.0.0.0:19192 \         # Endpoint for Query UI
    --grpc-address              0.0.0.0:19092 \         # gRPC endpoint for Store API
    --cluster.address           0.0.0.0:19591 \
    --cluster.advertise-address 127.0.0.1:19591 \
    --cluster.peers             127.0.0.1:19391 \       # Static cluster peer where the node will get info about the cluster
    --cluster.peers             127.0.0.1:19392 \       # Another cluster peer (many can be added to discover nodes)
    --store                     0.0.0.0:18091   \       # Static gRPC Store API Address for the query node to query
    --store                     0.0.0.0:18092   \       # Also repeatable
```

When to use gossip vs store flags?
- Use gossip if you want to maintain single gossip cluster that is able to dynamically join and remove components.
- Use static store when you want to have full control of which components are connected. It is also easier to user static store options when setting up communication with remote (cross-cluster) components e.g (sidecar in different network through some proxy)

Configuration of initial peers is flexible and the argument can be repeated for Thanos to try different approaches.
Additional flags for cluster configuration exist but are typically not needed. Check the `--help` output for further information.

* _[Example Kubernetes manifest](../kube/manifests/prometheus.yaml)_
* _[Example Kubernetes manifest with GCS upload](../kube/manifests/prometheus-gcs.yaml)_

## Store Gateway

As the sidecar backs up data into the object storage of your choice, you can decrease Prometheus retention and store less locally. However we need a way to query all that historical data again.
The store gateway does just that by implementing the same gRPC data API as the sidecars but backing it with data it can find in your object storage bucket.
Just like sidecars and query nodes, the store gateway joins the gossip cluster and is automatically picked up by running query nodes as yet another data provider.

```
thanos store \
    --tsdb.path                 /var/thanos/store \     # Disk space for local caches
    --gcs.bucket                example-bucket \        # Bucket to fetch data from
    --cluster.address           0.0.0.0:19891 \
    --cluster.advertise-address 127.0.0.1:19891 \
    --cluster.peers             127.0.0.1:19391 \
```

The store gateway occupies small amounts of disk space for caching basic information about data in the object storage. This will rarely exceed more than a few gigabytes and is used to improve restart times. It is not useful but not required to preserve it across restarts.

* _[Example Kubernetes manifest](../kube/manifests/thanos-store.yaml)_

## Compactor

A local Prometheus installation periodically compacts older data to improve query efficieny. Since the sidecar backs up data as soon as possible, we need a way to apply the same process to data in the object storage.

The compactor component simple scans the object storage and processes compaction where required. At the same time it is responsible for creating downsampled copies of data to speed up queries.

```
thanos compact \
    --data-dir /var/thanos/compact \  # Temporary workspace for data processing
    --gcs-bucket example-bucket
```

The compactor is not in the critical path of querying or data backup. It can either be run as a periodic batch job or be left running to always compact data as soon as possible. It is recommended to provide 100-300GB of local disk space for data processing.

_NOTE: The compactor must be run as a **singleton** and must not run when manually modifying data in the bucket._

# All-in-one example

You can find one-box example with minikube [here](../kube/README.md).

# Dashboards

You can find example Grafana dashboards [here](../examples/grafana/monitoring.md)

# Alerts

You can find example Alert configuration [here](../examples/alerts/alerts.md)
