## Thanos Remote Write

Status: draft | in-review | rejected | *accepted* | complete

Implementation Owner: @brancz

## Summary

This document describes the motivation and design of the Thanos receiver component, as well as how it fits into the rest of the Thanos ecosystem and components.

## Motivation

The Thanos receiver is the missing piece within Thanos in order to use it to build Prometheus as a Service offering, either as an internal service to the rest of an organization or as an actual pay-as-you-go off the shelf service. It builds on top of existing Prometheus servers and retains their usefulness, while extending their functionality with long-term-storage, horizontal scalability and downsampling. The normal Thanos sidecar is not sufficient for this, as the system would always lack the block length behind (typically 2 hours), which would prevent the most common query pattern of Prometheus: real time queries of very recent data. With the Thanos receiver on the service provider side users can simply deploy vanilla Prometheus servers and configure Prometheus with built-in functionality to send its data to the service provider.

This component is only recommended for uses, where pushing is the only viable solution, for example analytics use cases or cases where the data ingestion must be client initiated, for example a software as a service type environments.

This component is not recommended in order to achieve a global view of data of a single tenant, for those cases the sidecar based approach with layered Thanos queriers is recommended. Also users are asked to note the [various pros and cons of pushing metrics](https://docs.google.com/document/d/1H47v7WfyKkSLMrR8_iku6u9VB73WrVzBHb2SB6dL9_g/edit#heading=h.2v27snv0lsur). Multi tenancy may also be achievable if ingestion is not user controlled, as then enforcing of labels, for example using the [prom-label-proxy](https://github.com/openshift/prom-label-proxy) (please thoroughly understand the mechanism if intending to employ this mechanism, as wrong configuration could leak data).

## Technical summary of Thanos receiver component

To solve the above mentioned problems, a new Thanos component is proposed: the Thanos receiver.

Prometheus has the remote write API to send samples collected by a Prometheus server to a remote location. This is essentially replicating the Prometheus write-ahead-log to that remote location. The Thanos receiver represents the remote location that accepts the Prometheus remote write API. This brings some interesting characteristics with it. It retains the metadata aspects that has contributed to a large part to Prometheus' success, while the remote location can add additional features to the stack that are not present (and arguably shouldn't be) in Prometheus, such as long term storage and horizontal scalability, while keeping the local Prometheus useful for reliable alerting on short term data.

## Architecture

The Thanos receiver component seamlessly integrates into the rest of the Thanos components. It acts similarly to what is referred to in Thanos as a "source", in the current set of components, this is typically represented by the Thanos sidecar that is put next to Prometheus to ship tsdb blocks into object storage and reply to store API requests, however in the case of the Thanos receiver, the Thanos sidecar is not necessary anymore, as the data is replicated from the original Prometheus server to the Thanos receiver, and the Thanos receiver participates in the Thanos gossip mesh. The Prometheus server on a tenant’s infrastructure can therefore be completely vanilla and is just configured to replicate its time-series to the Thanos receiver.

Instead of directly scraping metrics, however, the Thanos receiver accepts Prometheus remote-write requests, and writes these into a local instance of the Prometheus tsdb. Once successfully committed to the tenant's tsdbs, the requests returns successfully. To prevent data leaking at the database level, each tenant has an individual tsdb instance, meaning a single Thanos receiver may manage multiple tsdb instances. The receiver answers Thanos store API requests and uploads built blocks of the Prometheus tsdb. Implementation-wise, this just requires wiring up existing components. As tenant's data within object storage are separate objects, it may be enough separation to have a single bucket for all tenants, however, this architecture supports any setup of tenant to object storage bucket combination.

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

In order to scale beyond a single machine, time-series are distributed among all receivers. In order to do this consistently, the Thanos receivers build a hash ring and use consistent hashing to distribute the time-series. Receivers are configured with their identity, and thus their position in the hash ring, by an external system (such as the configuration management system). The position in the hashring decides which time-series are accepted and stored by a Thanos receiver.

As the Thanos receivers route the requests to the responsible node, an edge load balancer is configured to randomly distribute load to all Thanos receivers available.

Time-series hashes are calculated from the entire label set of that time-series. To ensure that potentially large time-series with common labels do not all end up being ingested by the same node, the tenant’s ID should be included in the hash. The tenant's ID is obtained through a set of labels passed to the receiver via an HTTP header. The label keys defining a tenant must be pre-configured. For example, the receiver is configured with the following flag:

```
--tenant-labels=tenant
--tenant-header=X-THANOS-TENANT
```

And a valid request would have the `X-THANOS-TENANT` header set to `{tenant="A"}`. When a tenant label is configured, it must be included in the header sent.

Using the tenant's ID in the hash will help to distribute the load across receivers. The hash is roughly calculated as follows.

```
hash(string(tenant_id) + sort(timeseries.labelset).join())
```

The hashing function used is the same one as used by Prometheus’: [xxHash][xxHash]. Sorting of labels is necessary, in order to ensure that a unique time-series always has the same hash.

While the routing functionality could be a separate component, we choose to have it in the receiver to allow for a simpler setup.

### Ingestion hard tenancy

In attempts to build similar systems a common fallacy has been to distribute all load from all tenants of the system onto a single set of ingestion nodes. This makes reasoning and management rather simple, however, has turned out to have stronger downsides than upsides. Companies using Cortex in production and offering it to clients have setup entirely separate clusters for different customers, because their load has caused incidents affecting other clients. In order to allow customers to send large amounts of data at irregular intervals the ingestion infrastructure needs to scale without impacting durability. Scaling the ingestion infrastructure can cause endpoints to not accept data temporarily, however, the write-ahead-log based replication can cope with this, as it backs off and continues sending its data once the ingestion infrastructure successfully processes those requests again.

Hard tenants in the Thanos receiver are configured in a configuration file, changes to this configuration must be orchestrated by a configuration management tool. When a remote write request is received by a Thanos receiver it goes through the list of configured hard tenants. For each hard tenant there is a separate hash ring respective to their ingestion nodes as described in the "Load distribution" section. A hard tenant also has the number of associated receive nodes belonging to it. A remote write request can be initially received by any receiver node, however, will only be dispatched to receiver nodes that correspond to that hard tenant.

A sample of the configuration of tenants and their respective infrastructure:

```
tenants:
- match: tenant-a
  nodes:
  - tenant-a-1.metrics.local
- hashmod: 0
  nodes:
  - soft-tenants-1.metrics.local
```

To start, exact matches of tenant IDs will used to distribute requests to receive nodes. Additionally a sharding mechanism performing `hashmod` on the tenant ID, in order to shard the tenants among pools of receivers. Should it be necessary, more sophisticated mechanisms can be added later. When a request is received, the specified tenant is tested against the configured tenant ID until an exact match is found. If the specified tenant is the empty string, then any tenant is considered a valid match. If no hard tenancy is configured, a tenant will automatically land in a soft tenancy hashring.

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

The intention is that the load balancer can just distribute requests randomly to all Thanos receivers independent of the tenant. Should this turn out to cause problems, either an additional “distribution” layer can be inserted that performs this routing or a load balancer per hard tenant hashring can be introduced. The distribution layer would have the advantage of being able to keep a single endpoint for configuration of Prometheus servers for remote write, which could be convenient for users.

### Rollout/scaling/failure of receiver nodes

As replication is based on the Prometheus write-ahead-log and retries when the remote write backend is not available, intermediate downtime is tolerable and expected for receivers. Prometheus remote write treats 503 as temporary failures and continues do retry until the remote write receiving end responds again.

On rollouts receivers do not need to re-shard data, but instead at shutdown in case of rollout or scaling flush the write-ahead-log to a Prometheus tsdb block and upload it to object storage. Rollouts that include a soft tenant being promoted to a hard tenant, does require all nodes of a hash-ring to upload its content as the hash-ring changes. When the nodes comes back and accepts remote write requests again, the tenant local Prometheus server will continue where it left off. When scaling, all nodes need to perform the above operation as the hashring is resized meaning all nodes will have a new distribution. In the case of a failure, and the hashring is not resized, it will load the write-ahead-log and assume where it left off. Partially succeeding requests return a 503 causing Prometheus to retry the full request. This works as identically existing timestamp-value matches are ignored by tsdb. Prometheus relies on this to de-duplicate federation request results, therefore it is safe to rely on this here as well.

This may produce small, and therefore unoptimized, blocks of TSDB in object storage, however, these are optimized away, by the Thanos compactor by merging the small blocks into bigger blocks. The compaction process is done concurrently in a separate deployment to the receivers. Timestamps involved are produced by the sending Prometheus, therefore no clock synchronization is necessary.

Changing of a soft to a hard tenant (or vise versa) we need to flush all blocks in all nodes in the effected rings that tenant is present in.

## Open questions

Decisions of the design have consequences some of which will show themselves in practice. These include (but are not limited to):

* Bursting remote write API requests (after a rollout or easing of rate limiting), as Prometheuses may attempt to push their data simultaneously.
  - This could be solved with a combination of rate limiting and back-off on Prometheus’ side, plus alerts if replication lag gets too large due to rate limiting (or other factors).
* This proposal describes the write-ahead-log based remote write, which is not (yet) merged in Prometheus: https://github.com/prometheus/prometheus/pull/4588. This landing may impact durability characteristics.
  - While there is work left the pull request seems to be close to completion
* For compaction to work as described in this proposal, vertical compaction in tsdb needs to be possible. Implemented but not merged yet: https://github.com/prometheus/tsdb/pull/370
* If downtime of ingestion, as in the described `503` for intermediate downtime, and Prometheus resuming when the backend becomes healthy again, turns out not to be an option, we could attempt to duplicate all write requests to 3 (or configurable amount) replicas, where a write request needs to be accepted by at least 2 replicas, this way we can ensure no downtime of ingestion. This may require additional compaction and deduplication of object storage as well as significantly increase infrastructure cost.
* Additional safeguards may need to be put in place to ensure that hashring resizes do not occur on failed nodes, only once they have recovered and have successfully uploaded their blocks.

[xxhash]: http://cyan4973.github.io/xxHash/
[prom-label-proxy]: https://github.com/openshift/prom-label-proxy

