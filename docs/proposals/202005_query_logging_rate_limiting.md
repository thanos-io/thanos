---
title: Query Logging and Rate Limiting
type: proposal
menu: proposals
status: unknown
owner: yashrsharma44, kakkoyun, bwplotka
---

# Todo


### Related Tickets

* Add Query Logging : https://github.com/thanos-io/thanos/issues/1706
* Add generic layer of limits : https://github.com/thanos-io/thanos/issues/2527

## Summary

This proposal describes configuring a new internal feature : Query logging and rate limiting, for internal APIs of Thanos. 

We will go through the problem statement, use-cases and potential solution for the same.

## Problem Statement / Motivation

Conceptually and at a very high level, Thanos is a durable and cheap database, storing a very large amount of metric data in external object storage. This has the possible implication of having expensive data queries if the amount of data required to be queried is large. 

Even after local optimizations and efficient indexing, still querying a large amount of data is very resource-intensive, and unless the user knows what he/she is doing, it can be very expensive. A possible solution apart from the existing ones is limiting the queries based on the time_range and data consumption. 

It does not make sense if the user requests years of data each containing millions of series, and it does not fit in the specification of the local Thanos Store gateway. Getting such a huge amount of data is not pragmatic, and some sort of checks for the user in getting estimates about the kind of requests he/she is making really helps in avoiding such blockhole.

Another feature lacking from Thanos, is tracking/logging of active queries run in Thanos. This is really helpful when a Thanos component goes down, and it would help the administrator/developer in reading the last run queries before the component gets OOM killed. A similar feature exists in Prometheus which was introduced in version 2.16.0. The same feature in Thanos would help in debugging the potential queries that caused the issue.

## Use Cases

* Controlling and limiting the amount of requests made by the internal APIs. 
* Query logging part would help in debugging the internal requests made.
* This provides ample of insights about possible bottle-neck of individual components of Thanos.


## Goals of this design

* Implement Query Logging for internal APIs of Thanos, StoreAPI in particular
* The Query Logging to implement a seperate logger derived from the base logger. 
* Implement Rate limiting and tracking of queries of the internal APIs


## Proposal

### Query Logging in Thanos

#### Description of the Problem

The Query Logging is a feature which helps in tracking all the active queries made in a database, and helps the users in knowing/debugging what queries made were resource-intensive, and how they can use the details to further optimize the database, or from the user’s perspective, to draw out the line from making such a request.

A similar feature exists in Prometheus, here is a description of an active Query Log [[1]](https://www.robustperception.io/what-queries-were-running-when-prometheus-died) -

```powershell
level=info ts=2019-08-28T14:30:09.142Z caller=main.go:329 msg="Starting Prometheus" version="(version=2.12.0, branch=HEAD, revision=43acd0e2e93f9f70c49b2267efa0124f1e759e86)"
level=info ts=2019-08-28T14:30:09.142Z caller=main.go:330 build_context="(go=go1.12.8, user=root@7a9dbdbe0cc7, date=20190818-13:53:16)"
level=info ts=2019-08-28T14:30:09.142Z caller=main.go:331 host_details="(Linux 4.15.0-55-generic #60-Ubuntu SMP Tue Jul 2 18:22:20 UTC 2019 x86_64 mari (none))"
level=info ts=2019-08-28T14:30:09.142Z caller=main.go:332 fd_limits="(soft=1000000, hard=1000000)"
level=info ts=2019-08-28T14:30:09.142Z caller=main.go:333 vm_limits="(soft=unlimited, hard=unlimited)"
level=info ts=2019-08-28T14:30:09.143Z caller=query_logger.go:74 component=activeQueryTracker msg="These queries didn't finish in prometheus' last run:" queries="[{\"query\":\"changes(changes(prometheus_http_request_duration_seconds_bucket[1h:1s])[1h:1s])\",\"timestamp_sec\":1567002604}]"
level=info ts=2019-08-28T14:30:09.144Z caller=main.go:654 msg="Starting TSDB ..."level=info
```

Here, we can see that this log was taken before the prometheus instance died unnaturally, and the query logged under the component activeQueryTracker, logs the respective query that did the damage. Even though it does not pin-points the exact issue that created the problem, still it is quite helpful in considering this issue as one of the potential queries that led to the unnatural death of the Prometheus instance.

#### Possible Approach

A possible solution would be to add a Logger to track all the queries made in all of the Thanos components. Since the different components of Thanos are interconnected with the StoreAPI, it would be helpful in tracking all the queries in the global state. The diagram here represents the storeAPI and how it is connected to all the components of Thanos - 

![](../img/thanos_log_limit.png)

Here we can track all the active queries made using a middleware between all the requests of the StoreAPI. Here, the QueryLogger would log all the active queries, thus keeping a track of all the query requests made until now. The middleware, or rather the interceptors, would intercept all the queries and log them to a file that would contain the active logs. This logging would certainly be different from the general logging that has been mentioned, and would have the following interface - 

```go
type QueryLogging struct {
	// mmapped file to store the queries.
     mmappedFile   []byte
     // channel to generate the next available index, much like Python’s generator indexing
	getNextIndex chan int
     // logger, different from the usual one
     logger       log.Logger
}
```

This interface is designed with some cues on a similar interface designed for Prometheus Query Logger[[2]](https://prometheus.io/docs/guides/query-log/). Here, the query would be intercepted by a gRPC middleware, as the StoreAPI implements a gRPC protocol.

Here is a rough algorithm that would implement the Query Logging in Thanos - 

```txt
1) Thanos receives a query. 
2) It calls the index_generator, which is a Python-style generator that will generate natural numbers from 0(or 1) indicating the byte index at which to put the information about the query in the log file. This would ensure that the key remains unique for each of the queries logged.
3) The log file would be a memory-mapped file, which would help in accessing a random position for logging the query in constant time.
```

This algorithm has been heavily derived from this pull request[[3]](https://github.com/prometheus/prometheus/pull/5794) and would suggest referring this for the implementation of the same.

### Rate Limiting

#### Description of the Problem

The rate-limiting aspect of Thanos currently refers to rate and query limiting of the Store API. This is a kind of a preflight check, where we do not download data and get our process OOM killed.

#### Possible Approach

Currently, there seems to have been a Pull Request made, not with respect to Query Limiting, but with respect to getting a [disk space](https://github.com/thanos-io/thanos/pull/1550) check. Another Pull Request was made to [estimate the size of the blocks](https://github.com/thanos-io/thanos/pull/1792) in the object storage. While the latter has been merged, the former seems to be open at the time of writing this document, and it can be configured along the StoreAPI to provide another check for the preflight measures of the amount of Query that would be made.

We can reuse the grpc-interceptor that would be built for Query Logging, and have the rate limiting logic encoded for possible implementation.

### Alternatives

#### Don't add anything

This proposal adds in a new feature for Thanos, so there isn't any alternative uptil now that would solve our problem.


## Work Plan

1. Roll out grpc-middlewarev2. 
2. Implement a grpc-interceptor for Store API currently(this might be extended for other APIs as well).
3. Write up the Query Logger derived from the base logger.
4. Configure the Query Logger along with the grpc-interceptor.
5. Discuss out the possible implementation of the rate limiting and what are the objectives that we want from it.
6. Configure the rate limiting logic in the grpc-interceptor for Store API.
