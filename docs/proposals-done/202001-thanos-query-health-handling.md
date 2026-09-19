---
type: proposal
title: Thanos Query store nodes healthiness handling
status: complete
owner: GiedriusS
menu: proposals-done
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/1651 (Response caching for Thanos)
* https://github.com/thanos-io/thanos/pull/2072 (query: add 'sticky' nodes)
* https://github.com/cortexproject/cortex/pull/1974 (frontend: implement cache control)
* https://github.com/thanos-io/thanos/pull/1984 (api/v1: add cache-control header)

## Summary

This proposal document describes how currently the healthiness of store nodes is handled and why it should be changed (e.g. to improve response caching and avoid surprises).

It explores a few options that are available - weighs their pros and cons, and finally proposes one final variant which we believe is the best out of the possible ones.

## Motivation

Currently Thanos Query updates the list of healthy store nodes every 5 seconds. It does that by sending the `Info()` call via gRPC. The last successful check is noted in the `LastCheck` field. At this point if it fails then we note the error and remove it from the set of active store nodes. If that succeeds then it becomes a part of the active store set.

After `--store.unhealthy-timeout` passes since `LastCheck` then it also gets removed from the UI. At this point we would forget about it completely.

Every time a query is executed, we consult the active store set and send the query to all of the nodes which match according to the external labels and the min/max times that they advertise. However, if a node goes down according to our previous definition then in 5 seconds we will not send anything to it anymore. This means that we won't even be able to control this via the partial response options since no query is sent to those nodes in the first place.

This is problematic in the cache of end-response caching. If we know that certain StoreAPI nodes should always be up then we always want to have not just an error (if partial response is disabled) but also the appropriate `Cache-Control` header in the response. But right now we would only get it for maximum 5 seconds after a store node would go down.

Thus, this logic needs to be changed somehow. There are a few possible options:

1. `--store.unhealthy-timeout` could be made to apply to this case as well - we could still consider it a part of the active store set while it is still visible in the UI.
2. Another option could be introduced such as `--store.hold-timeout` which would be `--store.unhealthy-timeout`'s brother and we would hold the StoreAPI nodes for `max(hold_timeout, unhealthy_timeout)`.
3. Another option such as `--store.strict-mode` could be introduced which means that we would always retain the last information of the StoreAPI nodes of the last successful check.
4. The StoreAPI node specification format that is used in `--store` could be extended to include another flag which would let specify the previous option per-specific node.
5. Instead of extending the specification format, we could move the same information to the command line options themselves. This would increase the explicitness of this new mode i.e. that it only applies to statically defined nodes.

Lets look through their pros and cons:

* In general it would be nice to avoid adding new options so the first option seems the most attractive in this regard but it is also the most invasive one.
* Second option increases the configuration complexity the most since you would have to think about the other option `--store.unhealthy-timeout` as well while setting it.
* The last option is the most complex from code's perspective but it is also least invasive; however it has some downsides like the syntax for specifying store nodes becomes really ugly and hard to understand because we would have not only the DNS query type in there but also a special marker to enable that mode.

If we were to graph these choices in terms of their incisiveness and complexity it would look something like this:

```text
Most incisive / Least Complex ------------ Least incisive / Most Complex
#1           #2                                      #4
           #3    #5
```

After careful consideration and with the rationale in this proposal, we have decided to go with the fifth option. It should provide a sweet spot between being too invasive and providing our users the ability to fall-back to the old behavior.

## Goals

* Update the health check logic to properly handle the case when a node disappears in terms of a caching layer.

## No Goals

* Fixing the cache handling in the cases where a **new** StoreAPI gets added to the active store set.

This deserves a separate discussion and/or proposal. The issue when adding a completely new store node via service discovery is that a new node may suddenly provide new information in the past. In this paragraph when we are saying "new" it means new in terms of the data that it provides. Generally over time only a limited number of Prometheus instances will be providing data that can only change in the future (relatively to the current time).

When this happens, we will need to most likely somehow signal the caching layer that it needs to drop (some of the) results cache that it has depending on:

* Time ranges of a new node.
* Its external labels.
* The data that the node itself has by using some kind of hashing mechanism.

The way this will need to be done should be as generic as possible so the design and solution of this is still an open question that this proposal does not solve.

## Verification

* Unit tests which would fire up a dummy StoreAPI and check different scenarios.
* Ad-hoc testing.

## Proposal

* Add a new flag to Thanos Query `--store-strict` which will only accept statically specified nodes and Thanos Query will always retain the last successfully retrieved information of them via the `Info()` gRPC method. Thus, they will always be considered as part of the active store set.

## Risk

* Users might have problems removing the store nodes from the active store set since they will be there forever with this option set. However, one might argue that if the nodes go down then something like DNS service discovery needs to be used which would dynamically add and remove those nodes.

## Work Plan

* Implement the new flag `--store-strict` in Thanos Query which will only accept statically defined nodes that will be permanently kept around. It is optional to use so there will be no surprises when upgrading.
* Implement tests with dummy store nodes.
* Document the new behavior.

## Future Work

* Handle the case when a new node appears in terms of the end-result cache i.e. when using SD:
  1. Need to somehow signal the upper layer to clear the end-result cache. Ideally we would only clear the relevant parts.
  2. Perhaps some kind of hashing can be used for this.
