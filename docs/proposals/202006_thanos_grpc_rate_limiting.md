---
title: Thanos Rate Limiting for GRPC
type: proposal
menu: proposals
status: WIP
owner: MalloZup
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/3525 (main)

## Summary and motivation

We want to implement a way to  "rate limiting" concept in thanos, which should prevent to overlaoad CPU and other resource server side which can result with high concurrent request in GRPC.

At the point of this writing GRPC has no standard library way to implement a rate-limiter. Allthough there is a interceptor from Grpc ecosystem ( see later)

The goal of the  mechanism should be a unified, safe way for rate limiting, both per component but also potentially per tenant.

There are basically 2 ways of solving the problem:
We should avoid such saturation with a rate-limiting concept or investigate if client queuing is better.


## Actual situation:

Right now in thanos we do have already some rate-limiting mechanism implemented with `gate`. ( more details on gate see later).

This are used in;

- `store`: https://github.com/thanos-io/thanos/blob/ca8be00eaf6e2e7fe341de460ea73e753f764004/cmd/thanos/store.go#L294 queries

- `query` component: https://github.com/thanos-io/thanos/blob/058e22b5b5ed13b44856d8c4fe355a40091f6a97/cmd/thanos/query.go#L459

- `memcached_client`: https://github.com/thanos-io/thanos/blob/1fff9a7157d1f3b7d5e0ce36f999b10bb3ae7ec4/pkg/cacheutil/memcached_client.go#L232






## Technical Implementation(s):


1) Rate-limiting via interceptor (global mechanism at Grpc init)

For implementing a rate-limiting in Grpc we need following resources:

a) an interceptor (middleware) to be registered 
b) a function which is called by the interceptor for limiting

In our context, having a interceptor registered by grpc server, would have a global mechanism we would  need to modify the actual-situation ( see previous point).



a) 
Currently github.com/grpc-ecosystem/go-grpc-middleware/ratelimit offer a way to implement and register a "limiter" for Grpc.

See  example: https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/ratelimit/examples_test.go.

b)
Once the interceptor is easy initiated, we can create a semaphore mechanism for limiting N Requests.

For implementing a semaphore we can use the `gate` constructuctor which is indeed a sempaphore with some prometheus metrics on top and context aware.

So in our interceptor limiter function we can have the gate with `RateLimitingReq` requests

```golang
   g := gate.New(RateLimitingReq)

   if err := g.Start(ctx); err != nil {
      return
   }
   defer g.Done()

  // do critical
```

The `RateLimitingReq` should be a config parameter which by default is X number and user can modify/tune if needed.
Using https://github.com/thanos-io/thanos/blob/058e22b5b5ed13b44856d8c4fe355a40091f6a97/cmd/thanos/query.go#L461 should be ok.


Open questions:

