---
type: proposal
title: Thanos Remote Write
status: completed
owner: brancz
menu: proposals-done
---

## Summary

This document describes the motivation and design of the Thanos receiver component, as well as how it fits into the rest of the Thanos ecosystem and components.

## Motivation

The Thanos receiver is the missing piece within Thanos in order to use it to build a Prometheus as a Service offering, either as an internal service to the rest of an organization or as an actual pay-as-you-go off the shelf service. It builds on top of existing Prometheus servers and retains their usefulness, while extending their functionality with long-term-storage, horizontal scalability and downsampling. The normal Thanos sidecar is not sufficient for this, as the system would always lag the block length behind (typically 2 hours), which would prevent the most common query pattern of Prometheus: real time queries of very recent data. With the Thanos receiver on the service provider side, users can simply deploy vanilla Prometheus servers and configure Prometheus with built-in functionality to send its data to the service provider.

This component is only recommended for uses for whom pushing is the only viable solution, for example, analytics use cases or cases where the data ingestion must be client initiated, such as software as a service type environments.

This component is not recommended in order to achieve a global view of data of a single tenant, for those cases the sidecar based approach with layered Thanos queriers is recommended. Also, users are asked to note the [various pros and cons of pushing metrics](https://docs.google.com/document/d/1H47v7WfyKkSLMrR8_iku6u9VB73WrVzBHb2SB6dL9_g/edit#heading=h.2v27snv0lsur). Multi tenancy may also be achievable if ingestion is not user controlled, as then enforcing of labels, for example using the [prom-label-proxy](https://github.com/openshift/prom-label-proxy) (please thoroughly understand the mechanism if intending to employ this mechanism, as wrong configuration could leak data).

## Technical summary of Thanos receiver component

To solve the above mentioned problems, a new Thanos component is proposed: the Thanos receiver.

Prometheus defines a remote write API to send samples collected by a Prometheus server to a remote location. This is essentially replicating the Prometheus write-ahead-log to that remote location. The Thanos receiver represents the remote location that accepts the Prometheus remote write API. This brings some interesting characteristics with it. It retains the metadata aspects that has contributed to a large part to Prometheus' success, while the remote location can add additional features to the stack that are not present (and arguably shouldn't be) in Prometheus, such as long term storage and horizontal scalability, while keeping the local Prometheus useful for reliable alerting on short term data.

## Architecture

The Thanos receiver component seamlessly integrates into the rest of the Thanos components. It acts similarly to what is referred to in Thanos as a "source", in the current set of components, this is typically represented by the Thanos sidecar that is put next to Prometheus to ship TSDB blocks into object storage and reply to store API requests, however in the case of the Thanos receiver, the Thanos sidecar is not necessary anymore, as the data is replicated from the original Prometheus server to the Thanos receiver, and the Thanos receiver participates in the Thanos gossip mesh. The Prometheus server on a tenant's infrastructure can therefore be completely vanilla and is just configured to replicate its time-series to the Thanos receiver.

Instead of directly scraping metrics, however, the Thanos receiver accepts Prometheus remote-write requests and writes these into a local instance of the Prometheus TSDB. Once successfully committed to the tenant's TSDB, the requests return successfully. To prevent data leaking at the database level, each tenant has an individual TSDB instance, meaning a single Thanos receiver may manage multiple TSDB instances. The receiver answers Thanos store API requests and uploads built blocks of the Prometheus TSDB. Implementation-wise, this just requires wiring up existing components. As tenant's data within object storage are separate objects, it may be enough separation to have a single bucket for all tenants, however, this architecture supports any setup of tenant to object storage bucket combination.

In a minimal setup the system would look like the following:

```
                 +
Tenant's Premise | Provider Premise
                 |
                 |            +------------------------+
                 |            |                        |
                 |  +-------->+     Object Storage     |
                 |  |         |                        |
                 |  |         +-----------+------------+
                 |  |                     ^
                 |  | S3 API              | S3 API
                 |  |                     |
                 |  |         +-----------+------------+
                 |  |         |                        |       Store API
                 |  |         |  Thanos Store Gateway  +<-----------------------+
                 |  |         |                        |                        |
                 |  |         +------------------------+                        |
                 |  |                                                           |
                 |  +---------------------+                                     |
                 |                        |                                     |
+--------------+ |            +-----------+------------+              +---------+--------+
|              | | Remote     |                        |  Store API   |                  |
|  Prometheus  +------------->+     Thanos Receiver    +<-------------+  Thanos Querier  |
|              | | Write      |                        |              |                  |
+--------------+ |            +------------------------+              +---------+--------+
                 |                                                              ^
                 |                                                              |
+--------------+ |                                                              |
|              | |                PromQL                                        |
|    User      +----------------------------------------------------------------+
|              | |
+--------------+ |
                 +
```

For the Thanos receiver to work at scale there are some areas that need further discussion:

1) Load distribution.
2) Ingestion hard tenancy.
3) Rollout/scaling/failure of receiver nodes.

### Load distribution

In order to scale beyond a single machine, time-series are distributed among all receivers. In order to do this consistently, the Thanos receivers build a hashring and use consistent hashing to distribute the time-series. Receivers are configured with their identity, and thus their position in the hashring, by an external system (such as the configuration management system). The position in the hashring decides which time-series are accepted and stored by a Thanos receiver.

As the Thanos receivers route the requests to the responsible node, an edge load balancer is configured to randomly distribute load to all Thanos receivers available.

Time-series hashes are calculated from the entire label set of that time-series. To ensure that potentially large time-series with common labels do not all end up being ingested by the same node, the tenant’s ID should be included in the hash. The tenant's ID is passed to the receiver via an HTTP header. The header defining a tenant must be pre-configured. For example, the receiver is configured with the following flag:

```
--receive.tenant-header=THANOS-TENANT
```

A valid request could have the `THANOS-TENANT` header set to `tenant-a`. If the header is not present in a request, then the request is interpreted as belonging to the empty string tenant ``.

Using the tenant's ID in the hash will help to distribute the load across receivers. The hash is roughly calculated as follows:

```
hash(string(tenant_id) + sort(timeseries.labelset).join())
```

The hashing function used is the same one used by Prometheus: [xxHash](http://cyan4973.github.io/xxHash/). Sorting of labels is necessary in order to ensure that a unique time-series always has the same hash.

While the routing functionality could be a separate component, we choose to have it in the receiver to allow for a simpler setup.

### Ingestion hard tenancy

In attempts to build similar systems a common fallacy has been to distribute all load from all tenants of the system onto a single set of ingestion nodes. This makes reasoning and management rather simple, however, has turned out to have stronger downsides than upsides. Companies using Cortex in production and offering it to clients have setup entirely separate clusters for different customers because their load has caused incidents affecting other clients. In order to allow customers to send large amounts of data at irregular intervals, the ingestion infrastructure needs to scale without impacting durability. Scaling the ingestion infrastructure can cause endpoints to not accept data temporarily, however, the write-ahead-log based replication can cope with this, as it backs off and continues sending its data once the ingestion infrastructure successfully processes those requests again.

Hard tenants in the Thanos receiver are configured in a configuration file. Changes to this configuration must be orchestrated by a configuration management tool. When a remote write request is received by a Thanos receiver, it goes through the list of configured hard tenants. For each hard tenant, there is a separate hashring respective to their ingestion nodes as described in the "Load distribution" section. A hard tenant also has the number of associated receive endpoints belonging to it. A remote write request can be initially received by any receiver node, however, will only be dispatched to receiver endpoints that correspond to that hard tenant.

A sample of the configuration of tenants and their respective infrastructure:

```json
[
    {
        "hashring": "tenant-a",
        "endpoints": ["tenant-a-1.metrics.local:19291/api/v1/receive", "tenant-a-2.metrics.local:19291/api/v1/receive"],
        "tenants": ["tenant-a"]
    },
    {
        "hashring": "tenants-b-c",
        "endpoints": ["tenant-b-c-1.metrics.local:19291/api/v1/receive", "tenant-b-c-2.metrics.local:19291/api/v1/receive"],
        "tenants": ["tenant-b", "tenant-c"]
    },
    {
        "hashring": "soft-tenants",
        "endpoints": ["http://soft-tenants-1.metrics.local:19291/api/v1/receive"]
    }
]
```

To start, exact matches of tenant IDs will be used to distribute requests to receive endpoints. Should it be necessary, more sophisticated mechanisms can be added later. When a request is received, the tenant specified in the request is tested against the configured allowed tenants for each hashring until an exact match is found. If a hashring specifies no explicit tenants, then any tenant is considered a valid match; this allows for a cluster to provide soft-tenancy. Requests whose tenant ID matches no other hashring explicitly, will automatically land in this soft tenancy hashring. If no matching hashring is found and no soft tenancy is configured, the receiver responds with an error.

```
                                  Soft tenant hashring
                                 +-----------------------+
                                 |                       |
+-----------------+              |  +-----------------+  |
|                 |              |  |                 |  |
|  Load Balancer  +-------+      |  | Thanos receiver |  |
|                 |       |      |  |                 |  |
+-----------------+       |      |  +-----------------+  |
                          |      |                       |
                          |      |                       |
                          |      |  +-----------------+  |
                          |      |  |                 |  |
                          +-------->+ Thanos receiver +-----------+
                                 |  |                 |  |        |
                                 |  +-----------------+  |        |
                                 |                       |        |
                                 +-----------------------+        |
                                                                  |
                                   Hard Tenant A hashring         |
                                 +-----------------------+        |
                                 |                       |        |
                                 |  +-----------------+  |        |
                                 |  |                 |  |        |
                                 |  | Thanos receiver +<----------+
                                 |  |                 |  |        |
                                 |  +-----------------+  |        |
                                 |                       |        |
                                 |                       |        |
                                 |  +-----------------+  |        |
                                 |  |                 |  |        |
                                 |  | Thanos receiver +<----------+
                                 |  |                 |  |
                                 |  +-----------------+  |
                                 |                       |
                                 +-----------------------+
```

The intention is that the load balancer can distribute requests randomly to all Thanos receivers independent of the tenant. Should this turn out to cause problems, a distribution layer can be created with additional Thanos receivers whose names do not appear in the configuration file. These receivers will forward all requests to the correct receiver in each hashring without storing any data themselves. Alternatively, a load balancer per hard tenant hashring can be introduced. The distribution layer allows users to maintain a single endpoint when configuring Prometheus servers for remote write, which may be convenient.

### Replication

The Thanos receiver supports replication of received time-series to other receivers in the same hashring. The replication factor is controlled by setting a flag on the receivers and indicates the maximum number of copies of any time-series that should be stored in the hashring. If any time-series in a write request received by a Thanos receiver is not successfully written to at least `(REPLICATION_FACTOR + 1)/2` nodes, the receiver responds with an error. For example, to attempt to store 3 copies of every time-series and ensure that every time-series is successfully written to at least 2 Thanos receivers in the target hashring, all receivers should be configured with the following flag:

```
--receive.replication-factor=3
```

Thanos receivers identify the replica number of a write request via a 0-indexed uint64 contained in an HTTP header. The header name can be configured via a flag:

```
--receive.replica-header=THANOS-REPLICA
```

If the header is present in a request, the receiver will look for the replica-th node of the hashring that should handle the request. If it is the receiver itself, then the request is stored locally, else it is forwarded to the correct endpoint. If the replica number of the request exceeds the configured replication factor or the total number of nodes in the target hashring, the receiver responds with an error. If the header is not present in a request, then the receiver will replicate the request to `REPLICATION_FACTOR` nodes, setting the replica header on each request to ensure it is not replicated further.

Note that replicating write requests may require additional compaction and deduplication of object storage as well as significantly increase infrastructure cost.

### Rollout/scaling/failure of receiver nodes

Prometheus remote write will retry whenever the remote write backend is not available, thus intermediate downtime is tolerable and expected for receivers. Prometheus remote write treats 503s as temporary failures and continues to retry until the remote write endpoint responds again. If this ingestion downtime is not acceptable, then a replication factor of 3 or more should be specified, ensuring that a write request is accepted in its entirety by at least 2 replicas. This way we can ensure there is no ingestion downtime.

Receivers do not need to re-shard data on rollouts; instead, they must flush the write-ahead-log to a Prometheus TSDB block and upload it to object storage during shutdown in the case of rollouts and scaling events. Rollouts that include a soft tenant being promoted to a hard tenant do require all nodes of a hashring to upload their content as the hashring changes. When the nodes come back and accept remote write requests again, the tenant's local Prometheus server will continue where it left off. When scaling, all nodes need to perform the above operation as the hashring is resized, meaning all nodes will have a new distribution. In the case of a node failure where the hashring is not resized, the node will load the write-ahead-log and assume where it left off. Partially succeeding requests return a 503 causing Prometheus to retry the full request. This works as identically existing timestamp-value matches are ignored by TSDB. Prometheus relies on this to de-duplicate federation request results, therefore it is safe to rely on this here as well.

This may produce small, and therefore unoptimized, TSDB blocks in object storage, however these are optimized away by the Thanos compactor by merging the small blocks into bigger blocks. The compaction process is done concurrently in a separate deployment to the receivers. Timestamps involved are produced by the sending Prometheus, therefore no clock synchronization is necessary.

When changing a soft tenant to a hard tenant (or vice versa), all blocks on all nodes in hashrings in which the tenant is present must be flushed.

## Open questions

Decisions of the design have consequences some of which will show themselves in practice. These include (but are not limited to):

* Bursting remote write API requests (after a rollout or easing of rate limiting), as Prometheuses may attempt to push their data simultaneously.
  - This could be solved with a combination of rate limiting and back-off on Prometheus’ side, plus alerts if replication lag gets too large due to rate limiting (or other factors).
* Additional safeguards may need to be put in place to ensure that hashring resizes do not occur on failed nodes, only once they have recovered and have successfully uploaded their blocks.
