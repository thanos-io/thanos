---
title: Life of a sample in Thanos, and how to configure it
date: "2023-11-20"
author: Thibault Mangé (https://github.com/thibaultmg)
---

## Life of a sample in thanos, and how to configure it

### Introduction

You would like to use Thanos but you are feeling intimidated by its apparent complexity? Thanos is indeed a sophisticated distributed system with a broad range of capabilities, and with that comes a certain level of configuration intricacies. In this article, we'll take a deep dive into the lifecycle of a sample within Thanos, tracking its journey from initial ingestion to final retrieval. Our focus will be to explain Thanos' critical internal mechanisms and pinpoint the essential configurations for each component, guiding you to achieve the operational results you're aiming for.

Our goal with this article is to make Thanos more accessible to new users, helping to alleviate any initial apprehensions. Given the extensive ground to cover, we'll aim to maintain conciseness throughout our exploration.

Before diving deeper, please check annexes to clarify some essential terminology. If you're already familiar with these concepts, feel free to skip ahead.

### The sample origin: do you have close integration capabilities?

The sample originates typically from a Prometheus instance that is scraping targets in a cluster. There are two possible scenarios:

* The **Prometheus instances are running in clusters that you administer**. In this case you can use the Thanos sidecar that you will attach the the pod running the prometheus server. The Thanos sidecar will read directly the samples from the Prometheus server using the read API. Then the sidecar will behave similarly to the other scenario without the routing and ingestion parts. Thus we will not go further into this use case.
* The **Prometheus servers are running in clusters that you do not control**. You'll often hear air gapped clusters. In this case, you cannot attach a sidecar to the Prometheus server. The samples will trvel to your Thanos system using the remote write protocol. This is the scenario we will focus on.

Comparing the two deployment modes, the Sidecar Mode is generally preferable due to its simpler configuration and fewer moving parts. However, if this isn't possible, opt for the **Receive Mode**. Bear in mind, this mode requires careful configuration to ensure high availability, scalability, and durability. It adds another layer of indirection and comes with the overhead of operating the additional component.

<!-- Add schema of both topologies, with side car in grey -->

### Sending samples to Thanos: the remote write protocol with limits

Say hi to our first Thanos component, the **Receive** or **Receiver**, the entry point to the system. This component facilitates the ingestion of metrics from multiple clients, eliminating the need for close integration with the clients' Prometheus deployments.

Thanos Receive is a server that exposes a remote-write endpoint (see [Prometheus remote-write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write)) that Prometheus servers can use to transmit metrics. The only prerequisite on the client side is to configure the remote write endpoint on each Prometheus server, a feature natively supported by Prometheus. 

The remote-write protocol is based on HTTP POST requests. The payload consists of a protobuf message containing a list of time-series samples and labels. Generally, a payload contains at most one sample per time series in a given payload and spans numerous time series. Metrics are typically scraped every 15 seconds, with a maximum remote write delay of 5 seconds to minimize latency, from scraping to query availability on the receiver. 

The Prometheus remote-write configuration offers various parameters to tailor connection specifications, parallelism, and payload properties (compression, batch size, etc.). While these may seem like implementation details for Prometheus, understanding them is essential for optimizing ingestion, as they form a sensitive part of the system.

Implementation-wise for Prometheus, the key idea is to read directly from the TSDB WAL (Write Ahead Log), a simple mechanism commonly used by databases to ensure data durability. If you wish to delve deeper into the TSDB WAL, check out this [great article](https://ganeshvernekar.com/blog/prometheus-tsdb-wal-and-checkpoint). Once samples are extracted from the WAL, they are aggregated into parallel queues (shards) as remote-write payloads. When a queue reaches its limit or a maximum timeout is attained, the remote-write client stops reading the WAL and dispatches the data. The cycle continues. The parallelism is defined by the number of shards, their number is dynamically optimized. More insights on Prometheus' remote write tuning can be found [here](https://prometheus.io/docs/practices/remote_write/).

<!-- TODO: add diagram showing the parameters influence over the protocol, explain tradeoffs, show sensible config, explain external labels management? -->

The remote write endpoint is configured with the `--remote-write.address` flag. You can also configure TLS options using other `--remote-write.*` flags. 

As you cannot control the configuration of your clients, you must protect yourselft from abusive usage. To that effect the receive component offers a few configuration options that are well described in the [documentation](https://thanos.io/tip/components/receive.md/#limits--gates-experimental). Here is a schema illustrating the impact of these options:

<!-- SCHEMA -->

### Receiving samples with high availability and durability

To have high availability, you can run multiple receive components. They will all listen on the same address and a load balancer will distribute requests. However, a given sample must always land on the same receive instance so that it can build data blocks as we'll describe later. The receive component will then use a hash ring to distribute the samples to the different receive components. There are two possible hashrings:

* hashmod EXPLAIN
* ketama EXPLAIN

These hashrings use gossip to maintain a consistent view of the ring. Receive components must then be able to communicate with each others. They expose to that end a http server that is configured with the `--http-address` flag.

Also some clients really don't want their data to be lost. Which my happen if a receive is down ... EXPLAIN REPLICATION, REPLICA LABEL

As we've seen, just for receiving samples, a lot of data can be flowing between receivers. We can reduce this overhead with the notion of Router and Ingestor. While in our preceding example, Receive wa acting as an `IngestorRouter`, doing both the routing... EXPLAIN CONFIGURATION AND DO SCHEMA

### Preparing samples for object storage: building chunks and blocks

One of the key feature of Thanos is that it uses cheap object storage (e.g. AWS S3) to store the data with configurable retention durations. While Prometheus will typically keep the data for a few weeks on its local disk. Preparing data for the object store is the responsability of the **Receive** component.

The object storage follows the TSDB data model with a few adaptations. It means that **Receive** aggregates samples over time to build TSDB Blocks. These blocks are build by aggregating data over two hours periods. Why two hours? EXPLAIN. Once ready these blocks are sent to object storage. Configuration of the Object storage is the same for every component that needs access to it. 
EXPLAIN OBJSTOER CONFIG< RETENTION DURATION AND NEED FOR STORE API SERVER. PROVIDE RECOMMENDATION FOR RETENTION DURATION. EXPLAIN TRADEOFFS.

EXPLAIN STORE API LIMITS

EXPLAIN OUT OF ORDER CONSIDERATIONSAS WE DON'T CONTROL OUR CLIENTS CLOCKS

EXPLAIN PARTIAL BLOCK SEND ON RESTART

### Maintaining data in shape: compaction

DEDUP RECEIVE REPLICATED DATA
DEDUP HIGH AVAILABILITY PROMETHEUS REPLICATED DATA
REARRENGE PARTIAL BLOCKS
INTRODUCE LEVELS MEANING
OPTIMIZE READ FOR LONG QUERIES

### Exposing buckets data for qureies: the store gateway and the store API

EXPLAIN HOW IT WORKS. EXPLAIN CONFIGURATION. EXPLAIN CACHES. EXPLAIN RECOMMENDATIONS. 

### Querying data: the engine, limits, and ruler split

TWO QUERY EVELUATION ENGINES: PROMQL AND VOLCANO. EXPLAIN TRADEOFFS. EXPLAIN CONFIGURATION. EXPLAIN VOLCANO HTTP PORTS. EXPLAIN NOISY NEIGHBOURS PROTECTION. EXPLAIN QUERY RULER SPLIT AND LIMITS.



### Annexes

### Metrics terminology: Samples, Labels and Series

* **Sample**: In the context of Prometheus, a sample is a data point representing a measurement of a dynamic system aspect or property at a specific moment in time. 
* **Labels** Each sample in Prometheus is associated with a set of labels, which are key-value pairs that provide details about:

  * The property being measured
  * Its source or origin
  * Additional contextual information

* **Series**: A series is defined by a unique combination of labels and their values. To illustrate, here's an example of a series representation:

```
http_requests_total{method="GET", handler="/users", status="200"} 
    ^                                        ^                            
The series name               labels in the form label=value                        
```

In this case, the series name (http_requests_total) is effectively a specific label named `__name__`. The unique set of labels and values collectively identifies a series.

* **Time Series**: Prometheus scrapes these samples, attaching a timestamp to each value. As the system evolves, these values change, and Prometheus continues to scrape new samples over time, forming a time series.

For our discussion, samples can be of various types, but we'll treat them as simple integers for simplicity.

### TSDB terminology: Blocks, Chunks, indexes

TSDB stores Series for fast read access, low data expansion. DESCIBE TSDB, DESIGN CONSTRAINTS, 120 SANPLES FORMS A BLOCK, BLOCKS ARE AGGREGATED INTO??ALL THIS IS INDEXED...




