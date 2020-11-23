---
title: Thanos Remote Write
type: proposal
menu: proposals
status: accepted
---

### Related Tickets

* Hashring improvements: https://github.com/thanos-io/thanos/issues/3141

### Summary

This document describes the motivation and design of splitting the receive component into receive-route and receive.
Additionally, we touch possibility for adding a consistent hashing mechanism and buffering the incoming requests.

### Motivation

1. Splitting functionality can be used to make resharding events easier.
2. Right now the receiver handles ingestion, routing and writing, thus leading to too many responsibilities in a single process.
This also makes the receiver more difficult to operate and understand for Thanos users.
Splitting the receiver component into two different components could potentially have the following benefits:
    1. Resharding events become faster and cause no downtime in ingestion.
    2. Deployment becomes easier to understand for Thanos users.
    3. Each component consists of less code.
    4. The new architecture enables further performance improvements.

### Goals
#### Main Goals
1. Split the receive component into receive-route and receive (and ensure ease of resharding events).
2. Evaluate any effects on performance by simulating scenarios and collecting and analyzing metrics.
3. Use consistent hashing to avoid reshuffling time series after resharding events. This would also avoid extra overhead of maintaining more active time series.
#### Secondary Goals (To be implemented on completion of main goals)
1. Explore buffering requests from routers to ingesters as this could minimize the number of requests inside of the hashring.
2. Benefits:
    1. Avoid downtime of ingestion when reconfiguring a receiver.
    2. Scale more quickly: don’t have to wait for all the nodes to flush their TSDB and apply the new configurations.
    3. Avoid instability of failing requests during hashring instability because none of the ingesters are down/offline or have inconsistent views of the hashring.

### Drawbacks of the project
There is no possible way to have a single-process receiver. The user must have a router + a receiver running.
##### Solution
We document how the new architecture can be set up to have the same general deployment of the old architecture. (We run router and receiver on the same node).
