---
title: Thanos Rate Limiting mechanism for GRPC
type: proposal
menu: proposals
status: WIP
owner: MalloZup (Dario Maiocchi)
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/3525 (main)

# Index:
- Motivation (1)
- Actual state of situation (2)
- Implementation abstracts (3)
- Alternatives evaluations (4)
- Pragmatic approach of implementations (planning) (5)

## 1) Summary and motivation

We want to implement a way to "rate limiting" concept in Thanos, which should prevent overloading CPU and other resources on the server-side due to high concurrent requests in GRPC.

At the point of this writing, gRPC has no standard library way to implement a rate-limiter. Although there is an interceptor from gRPC ecosystem.

* The goal of the mechanism should be a unified, safe way for rate limiting, both per component but also potentially per tenant.
* Another goal is to reuse similar rate limit technique for HTTP as for gRPC

The rate-limit mechanism should coexist also with the `gate` mechanism, which is already implemented and in place in thanos and will be described later below.
( both mechanisms can coexist and offer better protection)

## 2) Actual state of situation:

Currently, the prometheus upstream constructur `gate` is used for preventing an elavate number of requests.

This are used in;

- `store`: https://github.com/thanos-io/thanos/blob/ca8be00eaf6e2e7fe341de460ea73e753f764004/cmd/thanos/store.go#L294 queries

- `query` component: https://github.com/thanos-io/thanos/blob/058e22b5b5ed13b44856d8c4fe355a40091f6a97/cmd/thanos/query.go#L459

- `memcached_client`: https://github.com/thanos-io/thanos/blob/1fff9a7157d1f3b7d5e0ce36f999b10bb3ae7ec4/pkg/cacheutil/memcached_client.go#L232

The gate mechanism however is not a enough safe mechanism, even if it is a semaphore like mechanism, is not global, but it is a burden of maintainer and developer to maintain and put it in place
into "critical" part of code.

The Grpc rate-limiting mechanism, as global unique interceptor, can coexist with the gate, and can prevent that we forget to put semaphores where they should be.

## 3) Technical Implementation (Abstract):

Rate-limiting interceptor consist mainly of 2 parts

a) An interceptor (middleware) need to be registered. We use the `go-grpc` ecosystem middleware.

b) After the registration of the middleware, we implement the rate-limit algorithm, "token bucket" which is called by the function, registered by middleware.

In our context, having a interceptor registered by grpc server, would have a global mechanism, and another safer "gate". 

After everything is initialized correctly, pass the right configuration data globals to the functions, so that the `limits` are configurable.

For a code Proof of concept have a look on this pr.. https://github.com/thanos-io/thanos/pull/3636/files#diff-4174394f0daacbfb9452e54401606c7ef135a179c8bdd14a6c56ab4a18f40517R46

## 4) Alternatives: 

- status: `could be improved`. Gate mechanism is not implemented in all part of codebase. We could improve adopation of the `gate` construct and investigate 
- status:`discarded`. Cortex into the `v2` query-frontend( thanos uses `v1`), implement a scheduler. However is not applicable.
            more details avail at: https://cortexmetrics.io/docs/proposals/scalable-query-frontend/

## 5) Planning of the implementation:

- [ ] discuss this proposal

- [ ] Improve the grpc-handlers ecosystems. Branch: `V2` 

      * [ ] Change the interface to return `error`. In order to provide better context to Client who reaches rate-limiting.
      * [ ] Improve example.
      * [ ] Investigate  if needed, to add the rate-limiter library token algorithm, if importing it or implementing from scratch. to be discussed with team.

- [ ] Send a PR to thanos upstream with:

      * [ ] rate-limiting configuration/init on grpc server.
      * [ ] read global configuration options, in order to get the `Token Capacity` and `Token rate` from thanos conf.
      * [ ] evaluate if we need to do some `e2e` test or similar to test the functionality. 
