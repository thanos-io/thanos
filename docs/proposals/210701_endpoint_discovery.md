---
title: Unified Endpoint Discovery
type: proposal
menu: proposals
status: XXX
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/2600

### Summary

We want to propose a new flag called `--endpoint=<address>` that will be passed to Thanos query and replace the current `--store=<address>` and `--rule=<address>` flags, and add a discovery mechanism which will be used to determine what types of services this endpoint can serve.

### Motivation

Currently, in Thanos Query, the discovery of rules APIs happens via Store API's Info method. This makes it harder if we ever want to not have a coupling to the Store API (which is already planned for [scalable ruler proposal](https://github.com/thanos-io/thanos/blob/master/docs/proposals/202005_scalable-rule-storage.md)).

We also require passing two different flags to the Thanos Query component `--store=<address>` and `--rule=<address>`. If users use both flags, the Query component performs DNS discovery on often the same address multiple times, this can cause occasional DNS issues as this resolving happens so frequently. This is especially confusing when one works and the other doesn't.

Adding new APIs in the future, like

### Goals

Unite discovery of endpoints, which would make code cleaner as well as provide building blocks to provide discover and support for new gRPC services, for example, the Targets API/Exemplar API/Metadata API.

### Solution

Add a new flag called `--endpoint` to Thanos query, and auto-discover what services that endpoint is serving based on metadata each gRPC server exposes.

Each component will expose an Info API, that includes various metadata listed below. Discovery of endpoints will happen via this Info API, there might be a case that the discovery of gRPC will also have to happen via [gRPC reflection](https://github.com/grpc/grpc/blob/master/doc/server-reflection.md), this will be shown when implemention starts.

Info API metadata would include the following fields regardless of the type:
```
info:
  external_labels: blah_1, blah_2, ...
  store:
    minTime: 2
    maxTime: 4
  rule:
    blah: ...
```

Right now this info API would be added to existing components Ruler, Store, Sidecar, Compact, Downsample, Receive. But this would be added in the future for Targets, Exemplers, etc.

The upgrade path would be possible in this case, as it's strictly additive for endpoints, and there will be a migration path to fallback to current discovery methods which could be removed in the future.

#### Alternatives

Split Info method out of the Store API into a separate gRPC service called `info`. Each component must implement and expose this. The discovery would happen via this info gRPC service, which each component would need to explicitly state which API it supports. Besides that information, we would also need to include here various info, e.g. LabelSets, Max/Min time, etc. The drawback for this solution is harder upgrades as this would be more complex to implement as we would need to fall back to older APIs.

### Upgrade plan

Regardless of the solution we will be not removing any flags for a given period of time and have fallback code in place to be able to discover in the same way as we do right now. This will ensure smooth upgrade path for the users that make use of the current flags for discovery. We will mark the current flags as depracted and after two releases have passed we can remove the existing store and rule flag from query component and remove any migration code.

### Work plan

1. We would be adding the new info APIs and flags needed for the above solution, so this part would be entirely additive.
2. Second step would be the integration of this APIs and flags and migration of existing code.
3. We would not be removing the existing flags (`--store` and `--rule`) for some grace period of time, after that passes we would also have to remove it and any migration code.
