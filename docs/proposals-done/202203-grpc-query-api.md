## Introduce a Query gRPC API

* **Owners:**
  * `@fpetkovski`
  * `@mzardab`

> TL;DR: Introducing a new gRPC API for `/query` and `/query_range`

## Why

We want to be able to distinguish between gRPC Store APIs and other Queriers in the query path. Currently, Thanos Query implements the gRPC Store API and the root Querier does not distinguish between Store targets and other Queriers that are capable of processing a PromQL expression before returning the result. The new gRPC Query API will allow a querier to fan out query execution, in addition to Store API selects.

This is useful for a few reasons:

* When Queriers register disjoint Store targets, they should be able to deduplicate series and then execute the query without concerns of duplicate data from other queriers. This new API would enable users to effectively partition by Querier, and avoid shipping raw series back from each disjointed Querier to the root Querier.
* If Queriers register Store targets with overlapping series, users would be able to express a query sharding strategy between Queriers to more effectively distribute query load amongst a fleet of homogenous Queriers.
* The proposed Query API utilizes gRPC instead of HTTP, which would enable gRPC streaming from root Querier all the way to the underlying Store targets (Query API -> Store API) and unlock the performance benefits of gRPC over HTTP.
* When there is only one StoreAPI connected to Thanos Query which completely covers the requested range of the original user's query, then it is more optimal to execute the query directly in the store, instead of sending raw samples to the querier. This scenario is not unlikely given query-frontend's sharding capabilities.

### Pitfalls of the current solution

Thanos Query currently allows for `query` and `query_range` operations through HTTP only. Various query strategies can be implemented using the HTTP API, an analogous gRPC API would allow for a more resource efficient and expressive query execution path. The two main reasons are the streaming capabilities that come out of the box with gRPC, statically typed API spec, as well as the lower bandwidth utilization which protobuf enables.

## Goals
* Introduce a gRPC Query API implementation equivalent to the current Querier HTTP API (`query` for instant queries, `query_range` for range queries)

## Non-Goals

* Implementation of potential query sharding strategies described in this proposal.
* Streaming implementations for `query` and `query_range` rpc's, these will be introduced as additional `QueryStream` and `QueryRangeStream` rpc's subsequently.
* Response series ordering equivalent to the current Prometheus Query HTTP API behaviour

### Audience
* Thanos Maintainers
* Thanos Users

## How

We propose defining the following gRPC API:

```protobuf
service Query {
  rpc Query(QueryRequest) returns (stream QueryResponse);

  rpc QueryRange(QueryRangeRequest) returns (stream QueryRangeResponse);
}
```

Where the `QueryRequest`, `QueryResponse`, `QueryRangeRequest` and `Query RangeResponse` are defined as follows:

```protobuf
message QueryRequest {
  string query = 1;

  int64 time_seconds = 2;
  int64 timeout_seconds = 3;
  int64 max_resolution_seconds = 4;

  repeated string replica_labels = 5;

  repeated StoreMatchers storeMatchers = 6 [(gogoproto.nullable) = false];

  bool enableDedup = 7;
  bool enablePartialResponse = 8;
  bool enableQueryPushdown = 9;
  bool skipChunks = 10;
}

message QueryResponse {
  oneof result {
    /// warnings are additional messages coming from the PromQL engine.
    string warnings = 1;

    /// timeseries is one series from the result of the executed query.
    prometheus_copy.TimeSeries timeseries = 2;
  }
}

message QueryRangeRequest {
  string query = 1;

  int64 start_time_seconds = 2;
  int64 end_time_seconds = 3;
  int64 interval_seconds = 4;

  int64 timeout_seconds = 5;
  int64 max_resolution_seconds = 6;

  repeated string replica_labels = 7;

  repeated StoreMatchers storeMatchers = 8 [(gogoproto.nullable) = false];

  bool enableDedup = 9;
  bool enablePartialResponse = 10;
  bool enableQueryPushdown = 11;
  bool skipChunks = 12;
}

message QueryRangeResponse {
  oneof result {
    /// warnings are additional messages coming from the PromQL engine.
    string warnings = 1;

    /// timeseries is one series from the result of the executed query.
    prometheus_copy.TimeSeries timeseries = 2;
  }
}
```

The `Query` Service will be implemented by the gRPC server which is started via the `thanos query` command.

## Alternatives

The alternative to expressing a gRPC Query API would be to use the HTTP APIs and distinguish Queriers via configuration on startup. This would be suboptimal for the following reasons:
* No statically typed API definition, we would need to rely on HTTP API versioning to manage changes to the API that is intended to enable advanced query execution strategies.
* HTTP not as performant as gRPC/HTTP2, gRPC/HTTP2 allows us to use streaming(less connection overhead) and protobuf(smaller response sizes), the current HTTP API does not.
* Ergonomics, gRPC allows us to express a functional API with parameters, HTTP requires request parameter marshalling/unmarshalling which is very error-prone.

## Action Plan

* [X] Define the QueryServer gRPC Service
* [X] Implement the QueryServer gRPC Service in the Thanos Query
