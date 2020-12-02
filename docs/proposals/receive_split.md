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

1. Split the receive component into receive-route and receive (and ensure ease of resharding events).
2. Evaluate any effects on performance by simulating scenarios and collecting and analyzing metrics.
3. Use consistent hashing to avoid reshuffling time series after resharding events. The exact consistent hashing mechanism to be used needs some further research.

### Drawbacks of the project
There is no possible way to have a single-process receiver. The user must have a router + a receiver running.
##### Solution
We document how the new architecture can be set up to have the same general deployment of the old architecture. (We run router and receiver on the same node).
