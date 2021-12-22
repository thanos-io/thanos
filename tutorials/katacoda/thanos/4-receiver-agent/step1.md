## Problem Statement

Let's get back to our example from [Tutorial 3](https://www.katacoda.com/thanos/courses/thanos/3-receiver). Imagine you run a company called `Wayne Enterprises`. In tutorial 3, we established monitoring of two special clusters: `Batcave` & `Batcomputer`.  These are special because they do not expose public endpoints to the Prometheus instances running there for security reasons, so we used the Remote Write protocol to stream all metrics to Thanos Receive in our centralized space.

Let's imagine we want to expand our `Wayne Enterprises` by adding metrics collection to applications running on smaller devices inside more mobile Batman tools: `Batmobile` and `Batcopter`.

Each of these vehicles has a smaller computer that runs applications from which we want to scrape Prometheus-like metrics using OpenMetrics format.

As the person responsible for implementing monitoring in these environments, you have three requirements to meet:

1. Implement a global view of this data. `Wayne Enterprises` needs to know what is happening in all company parts - including secret ones!
2. `Batmobile` and `Batcopter` can be out of network for some duration of time. You don't want to lose precious data.
3. `Batmobile` and `Batcopter` environments are very **resource constrained**, so you want an efficient solution that avoids extra computations and storage.

Firstly, let us set up Thanos as we explained in the previous tutorial.

## Setup Central Platform

As you might remember from the previous tutorial, in the simplest form for streaming use cases, we need to deploy Thanos Receive and Thanos Querier.

Let's run `Thanos Receive`:

```
docker run -d --rm \
    -v $(pwd)/receive-data:/receive/data \
    --net=host \
    --name receive \
    quay.io/thanos/thanos:v0.21.0 \
    receive \
    --tsdb.path "/receive/data" \
    --grpc-address 127.0.0.1:10907 \
    --http-address 127.0.0.1:10909 \
    --label "receive_replica=\"0\"" \
    --label "receive_cluster=\"wayne-enterprises\"" \
    --remote-write.address 127.0.0.1:10908
```{{execute}}

This starts Thanos Receive that listens on `http://127.0.0.1:10908/api/v1/receive` endpoint for Remote Write and on `127.0.0.1:10907` for Thanos StoreAPI.

Next, let us run a `Thanos Query` instance connected to Thanos Receive:

```
docker run -d --rm \
--net=host \
--name query \
quay.io/thanos/thanos:v0.21.0 \
query \
--http-address "0.0.0.0:39090" \
--store "127.0.0.1:10907"
```{{execute}}

Verify that `Thanos Query` is working and configured correctly by looking at the 'stores' tab [here](https://[[HOST_SUBDOMAIN]]-39090-[[KATACODA_HOST]].environments.katacoda.com/stores) (try refreshing the Thanos UI if it does not show up straight away).

We should see Receive store on this page and, as expected, no metric data since we did not connect any Remote Write sender yet.

With our "central" platform that is ready to ingest metrics, we can now start to architect our collection pipeline that will stream all metrics to our `Wayne Enterprises`.
