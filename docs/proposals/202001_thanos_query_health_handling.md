---
title: Thanos Query store nodes healthiness handling
type: proposal
menu: proposals
status: proposed
owner: GiedriusS
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/1651 (Response caching for Thanos)
* https://github.com/thanos-io/thanos/pull/2072 (query: add 'sticky' nodes)
* https://github.com/cortexproject/cortex/pull/1974 (frontend: implement cache control)
* https://github.com/thanos-io/thanos/pull/1984 (api/v1: add cache-control header)

## Summary

This proposal document describes how currently the healthiness of store nodes is handled and why it should be changed to work better in terms of end response caching.

It explores a few options that are available - weighs their pros and cons, and finally proposes one final variant which seems to be the best out of the possible ones.

## Motivation

Currently Thanos Query updates the list of healthy store nodes every 5 seconds. It does that by sending the `Info()` call via gRPC. The last successful check is noted in the `LastCheck` field. At this point if it fails then we note the error and remove it from the set of active store nodes. If that succeeds then it becomes a part of the active store set.

After `--store.unhealthy-timeout` passes since `LastCheck` then it also gets removed from the UI. At this point we would forget about it completely.

Every time a query is executed, we consult the active store set and send the query to all of the nodes which match according to the external labels and the min/max times that they advertise. However, if a node goes down according to our previous definition then in 5 seconds we will not send anything to it anymore. This means that we won't even be able to control this via the partial response options since no query is sent to those nodes in the first place.

This is problematic in the cache of end-response caching. If we know that certain StoreAPI nodes should always be up then we always want to have not just an error (if partial response is disabled) but also the appropriate `Cache-Control` header in the response. But right now we would only get it for maximum 5 seconds after a store node would go down.

Thus, this logic needs to be changed somehow. There are a few possible options:

* `--store.unhealthy-timeout` could be made to apply to this case as well - we could still consider it a part of the active store set while it is still visible in the UI
* Another option could be introduced such as `--store.hold-timeout` which would be `--store.unhealthy-timeout`'s brother and we would hold the StoreAPI nodes for `max(hold_timeout, unhealthy_timeout)`.
* Another option such as `--store.strict-mode` could be introduced which means that we would always retain the last information of the StoreAPI nodes after the first successful check
* The StoreAPI node specification format that is used in `--store` could be extended to include another flag which would let specify the previous option per-specific node

Lets look through their pros and cons:

* In general it would be nice to avoid adding new options so the first option seems the most attractive in this regard but it is also the most invasive one;
* Second option increases the configuration complexity the most since you would have to think about the other option `--store.unhealthy-timeout` as well while setting it;
* The last option is the most complex from code's perspective but it is also least invasive; however it has some downsides like the syntax for specifying store nodes becomes really ugly and hard to understand because we would have not only the DNS query type in there but also a special marker to enable that mode

If we were to graph these choices in terms of theirs incisiveness and complexity it would look something like this:

```text
Most incisive / Least Complex ------------ Least incisive / Most Complex
#1           #2                                      #4
           #3
```

At first there was an initial attempt to add the last option but it was met with some discussion if this is really the way to go forward. After careful consideration and with the rationale in this proposal we could go with the most invasive choice. But it also probably makes the most sense since it would logically follow that as long as a node is visible in the UI then we send a query to it.

## Goals

* Update the health check logic to properly handle the case when a node disappears in terms of a caching layer

## No Goals

* Fixing the cache handling in the cases where a **new** StoreAPI gets added to the active store set

## Verification

* Unit tests which would fire up a dummy StoreAPI and check different scenarios
* Ad-hoc testing

## Proposal

* Keep store nodes around in the active store set in Thanos Query until `--store.unhealthy-timeout` passes

## Risk

* Users might start having partial responses with this implemented if they have certain nodes configured that are always down. However, one might argue that if the nodes go down then something like DNS service discovery needs to be used which would dynamically add and remove those nodes

## Work Plan

* Implement the change in logic not to remove nodes from the active store set immediately after one `Info()` gRPC call failure but until `--store.unhealthy-timeout` passes
* Implement tests with dummy store nodes
* Document the new behavior

## Future Work

* Handle the case when a new node appears in terms of the end-result cache
  1. Need to somehow signal the upper layer to clear the end-result cache
  1. Perhaps some kind of hashing can be used for this
