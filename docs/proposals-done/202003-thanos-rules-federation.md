---
type: proposal
title: Global / Federated Rules API
status: accepted
owner: s-urbaniak
menu: proposals-done
---

### Related Tickets

https://github.com/thanos-io/thanos/issues/1375

## Summary

Thanos allows a global query view for the Prometheus series. This means discovering, connecting various (often remote) "leafs" components and aggregating series data from them.

Since we work with metrics, evaluating recording rules and alerts is a very important part of our system. Some of our "StoreAPIs" like Prometheus and Thanos Ruler are designed for that. However, we currently don’t have a way to present those resources in a federated way e.g Alerts and Recording Rules. This document explores the potential way of solving this in the Prometheus Ecosystem.

## Motivation

Deployment scenarios with various levels of federated setups are not unusual. Some deployments leverage Thanos Ruler to query multiple data sources (i.e. via Thanos Querier) for rule and alert evaluation. This topology sacrifices availability. At the same time in the same deployment topology, a dedicated Prometheus instance is deployed for certain critical alerting rules implying high availability. Here, we are missing a federation middleware exposing a consolidated view of evaluated rules and alerts in the underlying Prometheus and Thanos Ruler instances.

Pitfalls of current solutions:
* It’s tedious to manually visit each leaf and we are lazy.
* Visiting leaf instances (be it Thanos Ruler or dedicated Prometheus endpoints) have to be implemented manually.

## Goals

* Present other Prometheus/Thanos Resources in a global view.
  * This also means an up-to-date view (e.g statuses)
* Simple to run if you already run Thanos.
  * Use consistent protocols (if we suddenly switch to pure HTTP from gRPC it will break user’s proxies for auth, routing, monitoring, rate limiting, etc)
  * Potentially use the same connections, discovered targets by Querier.

## Verification

* Unit tests which would fire up a dummy Rules API and check different scenarios.
* Ad-hoc testing.

## Proposal

### Rules API

We propose to add following proto service and propagate all using existing components that have rules, so:
* Sidecar
* Querier (for federation)
* Ruler

The newly introduced `Rules` service is designed to reflect [Prometheus' Rules API](https://github.com/prometheus/prometheus/blob/bc703b64568ebfaecf27b9b70be737ad318e217a/web/api/v1/api.go) allowing to retrieve recording and alerting rules.

```proto
service Rules {
  /// Rules has info for all rules.
  rpc Rules(RulesRequest) returns (stream RulesResponse);
}
```

Sidecar proxies the Rules request to its local Prometheus instance and synthesizes the response. Similarly, Thanos Ruler constructs the response based on its local state.

Thanos Querier fans-out to all know Rules endpoints configured via `--rule` command line flag, merges and deduplicates the result. This new setting is meant to configure rules endpoints as a strict subset of store endpoints as specified with `--store` and `--store.sd-files`. If a user specifies a `--rule` endpoint not matching `--store`/`--store.sd-files` endpoints the initial implementation would log out that fact. In the future (see below) it is planned to have this a more separate setting.

Generally the deduplication logic is less complex than with time series, specifically:

* Deduplication happens first at the rule group level. The identifier is the group name and the group file.
* Then, per group name deduplication happens on the rule level, where:

1. the rule type (recording rule vs. alerting rule)
2. the rule name
3. the rule label names
4. the rule expression `expr` field
5. the alerting rule `for` field

are being used as the deduplication identifier. Disjunct entries are simply merged by adding them to the result set.

Thanos Querier presents the result of the fan-out on a `/api/v1/rules` endpoint which is compatible with the Prometheus' rules API endpoint. Additionally Thanos Querier gains a new `--rule.replica-label` command line argument which falls back to `--query.replica-label` if unset. The replica labels refer to the same labels as specified in the `external_labels` section of Prometheus and the `--label` command line argument of Thanos Ruler.

#### Examples

A stream of alerting rules is merged from different Thanos Ruler API instances. These may be remote Thanos Ruler, Prometheus Sidecar, or even Thanos Querier instances. The merging Thanos Querier has `--rule.replica-label=replica`

Scenario 1:

As specified, the rule type and then rule name is used for deduplication.

Given the following stream of incoming rule groups and containing recording/alerting rules:

```text
group: a
   recording:<name:"r1" last_evaluation:<seconds:1 > >
   alert:    <name:"a1" last_evaluation:<seconds:1 > >
group: b
   recording:<name:"r1" last_evaluation:<seconds:1 > >
group: a
   recording:<name:"r2" last_evaluation:<seconds:1 > >
```

The output becomes:

```text
group: a
   alert:    <name:"a1" last_evaluation:<seconds:1 > >
   recording:<name:"r1" last_evaluation:<seconds:1 > >
   recording:<name:"r2" last_evaluation:<seconds:1 > >
group: b
   recording:<name:"r1" last_evaluation:<seconds:1 > >
```

Note in the example above how the recording rule `r1` is not deduplicated as it is contained in two different groups.

Scenario 2:

The next level of deduplication is governed by the label/value set of the underlying recording/alerting rule while respecting the replica label. For a given conflict, the youngest is preferred. For alerting rules, the youngest firing rule is preferred.

Given the following stream of incoming recording rules:

```text
group: a
   recording:<name:"r1" labels:<labels:<name:"replica" value:"thanos-ruler-1" > > last_evaluation:<2006-01-02T10:00:00> >
group: a
   recording:<name:"r1" labels:<labels:<name:"replica" value:"thanos-ruler-2" > > last_evaluation:<2006-01-02T10:01:00> >
```

The output becomes:

```text
group: a
   recording:<name:"r1" labels:<labels:<name:"replica" value:"thanos-ruler-2" > > last_evaluation:<2006-01-02T10:01:00> >
```

Given the following stream of incoming alerting rules:

```text
group: a
   alert:<state:FIRING name:"a1" labels:<labels:<name:"replica" value:"thanos-ruler-1" > > last_evaluation:<2006-01-02T10:00:00> >
group: a
   alert:<state:PENDING name:"a1" labels:<labels:<name:"replica" value:"thanos-ruler-2" > > last_evaluation:<2006-01-02T10:01:00> >
```

The output becomes:

```text
group: a
   alert:<state:FIRING name:"a1" labels:<labels:<name:"replica" value:"thanos-ruler-1" > > last_evaluation:<2006-01-02T10:00:00> >
```

Note how in the above output the firing alerting rule was preferred despite being older.

Scenario 3:

If, under the above conditions a rule is a candidate for deduplication, finally the rule `expr` and `for` fields are being considered for deduplication.

Given the following stream of incoming alerting rules will also result in two independent alerting rules as both the `expr` and `for` fields differ:

```text
  - alert: KubeAPIErrorBudgetBurn
    annotations:
      message: The API server is burning too much error budget
    expr: |
      sum(apiserver_request:burnrate1h) > (14.40 * 0.01000)
      and
      sum(apiserver_request:burnrate5m) > (14.40 * 0.01000)
    for: 2m
    labels:
      severity: critical
  - alert: KubeAPIErrorBudgetBurn
    annotations:
      message: The API server is burning too much error budget
    expr: |
      sum(apiserver_request:burnrate6h) > (6.00 * 0.01000)
      and
      sum(apiserver_request:burnrate30m) > (6.00 * 0.01000)
    for: 15m
    labels:
      severity: critical
```

Scenario 4:

As specified, the group name and file fields are used for deduplication.

Given the following stream of incoming rule groups:

```text
group: a/file1
group: b/file1
group: a/file2
```

The output becomes:

```text
group: a/file1
group: a/file2
group: b/file1
```

Deduplication of included alerting/recording rules inside groups is described in the previous scenarios.

## Alternatives

* Cortex contains a sharded Ruler. Assigning rules to shards is done via Consul, though a gossip implementation is under development. Shards do not communicate with other shards. Rules come from a store (e.g. a Postgres database).

## Work Plan

* Implement a new flag `--rule` in Thanos Querier which registers RulesAPI endpoints.
* Implement a new flag `--rule.replica-label` in Thanos Querier.
* Implement RulesAPI backends in sidecar, query, rule.
* Feature branch: https://github.com/thanos-io/thanos/pull/2200

## Future

These changes are suggestions which we will need to be discussed in future and are not part of the proposal implementation.

### Type and Info

Currently, `Info` is shared between the `RulesAPI` proposed here and the existing `StoreAPI` services. To accommodate for future additional APIs the following changes of the protobuf `Info` and `Type` structures are suggested.

The current `StoreType` enum is renamed to `Type`. This retains binary compatibility with older clients:

```diff
-enum StoreType {
+enum Type {
  UNKNOWN = 0;
  QUERY = 1;
  RULE = 2;
```

The current fields in `InfoResponse` connected to Store APIs are deprecated and dedicated new API sub types are proposed:

```diff
message InfoResponse {
  // Deprecated. Use label_sets instead.
  repeated Label labels = 1 [(gogoproto.nullable) = false];
+  // Deprecated. Will be removed in favor of StoreInfoResponse in the future.
  int64 min_time = 2;
+  // Deprecated. Will be removed in favor of StoreInfoResponse in the future.
  int64 max_time = 3;
-  StoreType storeType  = 4;
+  Type type  = 4;
  // label_sets is an unsorted list of `LabelSet`s.
+  // Deprecated. Will be removed in favor of StoreInfoResponse in the future.
  repeated LabelSet label_sets = 5 [(gogoproto.nullable) = false];
+
+  StoreInfoResponse store = 6;
+  RulesInfoResponse rules = 7;
}
```

### Independent `--rule` and `--store` endpoints configuration

To ease the current implementation, rules endpoints are a strict subset of store endpoints. In the future these settings should be separate, i.e. the user could specify different endpoints for rules and different endpoints for store.
