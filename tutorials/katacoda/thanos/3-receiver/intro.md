# Intermediate: Streaming metrics from remote source with Thanos Receive

The [Thanos](thanos.io) project defines a set of components that can be composed together into a highly available metric system with **unlimited storage capacity** that **seamlessly** integrates into your existing Prometheus deployments.

In this course you get first-hand experience building and deploying this infrastructure yourself.

In this tutorial, you will learn:

* How to ingest metrics data from Prometheus instances without ingress traffic and need to store data locally for longer time.
* How to setup a Thanos Querier to access this data.
* How Thanos Receive is different from Thanos Sidecar, and when is the right time to use each of them.

> NOTE: This course uses docker containers with pre-built Thanos, Prometheus, and Minio Docker images available publicly.

### Prerequisites

Please complete tutorial #1 first: [Global View and seamless HA for Prometheus](https://www.katacoda.com/thanos/courses/thanos/1-globalview) 🤗

### Feedback

Do you see any bug, typo in the tutorial or you have some feedback for us?
Let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io

### Contributed by:

* Ian Billett [@bill3tt](http://github.com/bill3tt)
