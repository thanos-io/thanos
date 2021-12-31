---
type: proposal
title: Scalable Rule Storage
status: complete
menu: proposals-done
---

## Summary

There is no way to scale rule evaluation and storage today except functionally sharding rules onto multiple instances of the `thanos rule` component. However, we have already solved scaling storage of time-series across multiple processes: `thanos receive`.

To scale rule evaluations and storage this proposal proposes to allow the `thanos rule` component to have a stateless mode, storing results of queries by sending it to a `thanos receive` hashring instead of storing them locally.

## Motivation

A few large rules can create a significant amount of resulting time-series, which limits the scalability of Thanos Rule, as it uses a single embedded TSDB.

Additionally, scaling out the rule component in terms of rule evaluations causes further defragmentation of TSDB blocks, as multiple rule instances produce hard to deduplicate samples. While doable with vertical compaction, it might cause some operational complexity and unnecessary load on the system.

## Goals

Allow scaling storage and execution of rule evaluations.

## Verification

* Run all rule component e2e tests with new mode as well.

## Proposal

Allow specifying one of the following flags:

* `--remote-write`
* `--remote-write.config` or `--remote-write.config-file` flag following the same scheme as [`--query.config`, and `--query.config-file`](../components/rule.md#query-api)
* `--remote-write.tenant-label-name` which label-value to use to set the tenant to be communicated to the receive component

If any of these are specified the ruler would run a stateless mode, without local storage, and instead writing samples to the configured remote server, which must implement the `storepb.WritableStore` gRPC service.

## Alternatives

Continue to allow spreading load only by functionally sharding rules.

## Work Plan

Implement functionality alongside the existing architecture of the rule component.

## Open questions

### Multi tenancy model

This it stands this proposal does not cover any multi tenancy aspects of the receive component there are two strategies that we could go with:

* Have a configurable label that determines the tenant in requests.
* Change the receive component to instead of using a header to determine the tenant use a label of the series being written.

As the first exists, this proposal will continue with this approach and potentially reevaluate in the future.

### Removal of embedded TSDB

For a start this functionality will be implemented alongside the current embedded TSDB. Once experience with this new mode has been gathered, it may be reevaluated to remove the embedded TSDB, but no changes planned for now. Alternatively the receive component could also be embedded into the rule component in an attempt to minimize code paths, but retain functionality.
