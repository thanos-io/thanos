# High-availability for store instances

Status: draft | **in-review** | rejected | accepted | complete

Proposal author: [@mattbostock](https://github.com/mattbostock)
Implementation owner: [@mattbostock](https://github.com/mattbostock)

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
used by the Store API by adding a single boolean field to indicate whether the
store instance in question is acting as a 'gateway':

```diff
--- before	2018-07-02 15:49:09.000000000 +0100
+++ after	2018-07-02 15:49:13.000000000 +0100
@@ -1,5 +1,6 @@
 message InfoResponse {
   repeated Label labels = 1 [(gogoproto.nullable) = false];
   int64 min_time        = 2;
   int64 max_time        = 3;
+  bool gateway          = 4;
 }
```

Thanos bucket store instances (i.e. store instances that act as 'gateways' to
AWS S3 or Google Cloud Storage) will set `gateway` to `true`. A `bool` type in
Protobuf defaults to false, so the behaviour of other existing store instances
that do not explicitly set a value for `gateway` will not be affected.

If a store instance is a gateway, query instances will treat each store
instance in a label group as having access to the same data. Query instances
will randomly pick any two store instances[1][] from the same gateway group and
use the first response returned.

[1]: https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf

Otherwise, query instances will wait for all replicas within the same
label group to respond (subject to existing timeouts) before returning a
response, consistent with the current behaviour.

### Scope

Horizontal scaling should be handled separately and is out of scope for this proposal.

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

Rather than expanding the `InfoResponse` Protobuf, we had originally considered relying on
an empty set of store labels to determine that a store instance was acting as a gateway.

We decided against this approach as it would make debugging harder due to its
implicit nature, and is likely to cause bugs in future.

## Open issues

### Querying from multiple buckets or object stores

Thanos users may wish to use multiple buckets or multiple object stores
concurrently.

Bucket store instances expose a empty set of labels, so there is no way for the
query instance to distinguish between buckets or object stores.

We may wish to solve this by exposing a distinguishing identifier for the
bucket store in the `InfoResponse`. This identifier field might supplement or
replace the `gateway` boolean field (i.e. a null identifier would mean that the
store instance is not a gateway).

## Glossary

### Label group

A 'label group' is a group of store instances having an identical set of labels,
including the same label names and label values.

## Related future work

### Sharing data between store instances

Thanos bucket stores download index and metadata from the object store on
start-up. If multiple instances of a bucket store are used to provide high
availability, each instance will download the same files for its own use. These
file sizes can be in the order of gigabytes.

Ideally, the overhead of each store instance downloading its own data would be
avoided. We decided that it would be more appropriate to tackle sharing data as
part of future work to support the horizontal scaling of store instances.
