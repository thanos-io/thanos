# High-availability for store instances

Status: draft | in-review | **rejected** | accepted | complete

Proposal author: [@mattbostock](https://github.com/mattbostock)
Implementation owner: [@mattbostock](https://github.com/mattbostock)

## Status: Rejected

This proposal makes total sense and solves our goals when using gossip. However there exists a very easy solution
to this problem in form of using just static entry with any loadbalancer like Kubernetes Service to load balance
through different Store Gateways. Those are technically stateless, so request can fetch the data independently.

## Motivation

Thanos store instances currently have no explicit support for
high-availability; query instances treat all store instances equally. If
multiple store instances are used as gateways to a single bucket in an object
store, Thanos query instances will wait for all instances to respond (subject
to timeouts) before returning a response.

## Goals

- Explicitly support and document high availability for store instances.

- Reduce the query latency incurred by failing store instances when other store
  instances could return the same response faster.

## Proposal

Thanos supports deduplication of metrics retrieved from multiple Prometheus
servers to avoid gaps in query responses where a single Prometheus server
failed but similar data was recorded by another Prometheus server in the same
failure domain. To support deduplication, Thanos must wait for all Thanos
sidecar servers to return their data (subject to timeouts) before returning a
response to a client.

When retrieving data from Thanos bucket store instances, however, the desired
behaviour is different; we want Thanos use the first successful response it
receives, on the assumption that all bucket store instances that communicate
with the same bucket have access to the same data.

To support the desired behaviour for bucket store instances while still
allowing for deduplication, we propose to expand the [InfoResponse
Protobuf](https://github.com/improbable-eng/thanos/blob/b67aa3a709062be97215045f7488df67a9af2c66/pkg/store/storepb/rpc.proto#L28-L32)
used by the Store API by adding two fields:

- a string identifier that can be used to group store instances

- an enum representing the [peer type as defined in the cluster
  package](https://github.com/improbable-eng/thanos/blob/673614d9310f3f90fdb4585ca6201496ff92c697/pkg/cluster/cluster.go#L51-L64)

For example;

```diff
--- before	2018-07-02 15:49:09.000000000 +0100
+++ after	2018-07-02 15:49:13.000000000 +0100
@@ -1,5 +1,6 @@
 message InfoResponse {
   repeated Label labels = 1 [(gogoproto.nullable) = false];
   int64 min_time        = 2;
   int64 max_time        = 3;
+  string store_group_id = 4;
+  enum PeerType {
+    STORE  = 0;
+    SOURCE = 1;
+    QUERY  = 2;
+  }
+  PeerType peer_type    = 5;
 }
```

For the purpose of querying data from store instances, stores instance will be
grouped by:

- labels, as returned as part of `InfoResponse`
- the new `store_group_id` string identifier

Therefore, stores having identical sets of labels and identical values for
`store_group_id` will belong in the same group for the purpose of querying
data. Stores having an empty `store_group_id` field and matching labels will be
considered to be part of the same group. Stores having an empty
`store_group_id` field and empty label sets will also be considered part of the
same group.

If a service implementing the store API (a 'store instance') has a `STORE` or
`QUERY` peer type, query instances will treat each store instance in the same
group as having access to the same data. Query instances will randomly pick any
two store instances[1][] from the same group and use the first response
returned.

[1]: https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf

Otherwise, for the `SOURCE` peer type, query instances will wait for all
instances within the same group to respond (subject to existing timeouts)
before returning a response, consistent with the current behaviour. This is
necessary to collect all data available for the purposes of deduplication and
to fill gaps in data where an individual Prometheus server failed to ingest
data for a period of time.

Each service implementing the store API must determine what value the
`store_group_id` should return. For bucket stores, `store_group_id` should
contain the concatenation of the object store URL and bucket name. For all
other existing services implementing the store API, we will use an empty string
for `store_group_id` until a reason exists to use it.

Multiple buckets or object stores will be supported by setting the
`store_group_id`.

Existing instances running older versions of Thanos will be assumed to have
an empty string for `store_group_id` and a `SOURCE` peer type, which will
retain existing behaviour when awaiting responses.

### Scope

Horizontal scaling should be handled separately and is out of scope for this
proposal.

## User experience

From a user's point of view, query responses should be faster and more reliable:

- Running multiple bucket store instances will allow the query to be served even
  if a single store instance fails.

- Query latency should be lower since the response will be served from the
  first bucket store instance to reply.

The user experience for query responses involving only Thanos sidecars will be
unaffected.

## Alternatives considered

### Implicitly relying on store labels

Rather than expanding the `InfoResponse` Protobuf, we had originally considered
relying on an empty set of store labels to determine that a store instance was
acting as a gateway.

We decided against this approach as it would make debugging harder due to its
implicit nature, and is likely to cause bugs in future.

### Using boolean fields to determine query behaviour

We rejected the idea of adding a `gateway` or `deduplicated` boolean field to
`InfoResponse` in the store RPC API. The value of these fields would have had
the same effect on query behaviour as returning the peer type field as proposed
above and would be more explicit, but were specific to this use case.

The peer type field in `InfoResponse` proposed above could be used for other
use cases aside from determining query behaviour.

## Related future work

### Sharing data between store instances

Thanos bucket stores download index and metadata from the object store on
start-up. If multiple instances of a bucket store are used to provide high
availability, each instance will download the same files for its own use. These
file sizes can be in the order of gigabytes.

Ideally, the overhead of each store instance downloading its own data would be
avoided. We decided that it would be more appropriate to tackle sharing data as
part of future work to support the horizontal scaling of store instances.
