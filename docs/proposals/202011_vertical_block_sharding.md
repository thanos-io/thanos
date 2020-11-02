---
title: "Vertical Block Sharding"
type: proposal
menu: proposals
status: proposed
owner: @bwplotka
Date: 2 November 2020
---

## Prior Reading

Before you start, please read carefully [compactor docs](../components/compact.md), especially `Compaction`, `Resource` and `Scalability` parts.

## Problem Statement

Thanos uses TSDB format to store data in object storage. Block is just a directory with different files. Mainly index file for
metadata, string interning (symbols) and label-pair index and a few (max 512MB) chunk files to store the actual data.
Blocks have globally unique ID and are create for certain time ranges and for certain external labels (so also tenants).

This being said, the block size depends on multiple factors:

* Index size
  * Number of series
  * Number of labels per series
  * The cardinality of labels (how many labels are shared between different series)
  * The length and cardinality of strings inside labels
* Total Chunk File size
  * Number of samples per series
  * Variance of scrape intervals and sample values

Realistically in Thanos system blocks usually are created either by Prometheus or [Thanos Receive](../components/receive.md). In both case the same TSDB database is 
used. Initial block have always 2 hours range. In terms of "vertical" size it generally does not have more than 10 millions series, as 
ingesting this many by single Prometheus takes lots of resources, so it's usually sharded functionally at ingestion time if anything.
  
However, there are few things that can cause arbitrary large blocks in modern Thanos systems:

* Horizontal Compactions, compacts multiple blocks into bigger time ranges. Especially to get advantage of downsampling you 
want blocks to be compacted to longer time ranges. But this gives benefits only from long living series. On normal heavily used
Kubernetes we seen average life duration of ~13h, not mentioning few hundred thousands of single sample series. Even with limit
of 2w time range compaction for Thanos this can result compaction to be not that effecting, casing block to grow to enormous sizes (1TB with index
sizes over 60 GBs)
* Recently Thanos and Prometheus enabled [vertical compaction](../components/compact.md#vertical-compactions) (under hidden flag). This introduced
abilities to do backfilling (importing blocks) and offline deduplications.
* Both Prometheus and Thanos Receive write paths are getting more & more optimized allowing the initial 2h blocks to be much larger in practice. 

On top of that, there is currently **no programmatic limitation** for the size of the block in write block in Prometheus and Thanos, so in theory
user can build as large blocks as they want.

### Effects of "Too Large" TSDB blocks.

There are various, sometimes not obvious, limits and drawbacks of too large blocks for both Prometheus and Thanos:

* There is known limitation for Prometheus index file size. It can't exceed 64GB due to [uint32 used for postings](https://grafana.com/blog/2019/10/31/lifting-the-index-size-limit-of-prometheus-with-postings-compression/),
see [issue](https://github.com/thanos-io/thanos/issues/1424) and [code](https://github.com/prometheus/prometheus/blob/3d8826a3d42566684283a9b7f7e812e412c24407/tsdb/index/index.go#L279).
While there is work to lift this limitation and allow bigger indexes, there will be **some** other limit anyway.
* Compaction is not streaming bytes directly from & to object storage (yet). This is due to its current index format, which is [being worked on](https://github.com/thanos-io/thanos/issues/3389).
This means that Compactor has to download all source blocks & upload all output block files to and from disk, impacting the disk storage you need to prepare for Compactor.
This is a **potential scalability limit for Compactor**
* Compactor can be currently horizontally scaled to compact among streams of blocks. With some locking mechanisms we can get up to single compaction.
However, with single block having arbitrary size, this can be **another potential scalability issue**, where you cannot split this operation to two separate machines and do concurrently.
* Similarly, with reading the data from block, Store Gateway can shard up to single block, so with arbitrary large block, you cannot split lookup and reading to multiple
instances with the current implementation. This poses **potential scalability issue for Store Gateway**.
* The object storages are designed for big files, but with some limitation. The current limit e.g for [GCS is 5TB per upload/stored file](https://cloud.google.com/storage/quotas#:~:text=There%20is%20a%20maximum%20size,request%20is%20also%205%20TB).
* Last, but not the least operating with such big blocks is not easy. We are still creating new ways of deleting, cleaning up or analysing your data for
easier operation of Thanos. Operating (downloading, rewriting) TBs of bytes per blocks hits limitation of different tooling (e.g SRE can't just use
own laptop anymore). 

To sum up: Current logic of not caring about the block size got us far, however as expected, it's time to think about limiting the block size (primarily
index size).

### Related Tickets:

* [Limit size of blocks](https://github.com/thanos-io/thanos/issues/3068)
* [index exceeding max size of postings](https://github.com/thanos-io/thanos/issues/1424)

## Goals

* Automatically cap the size of index per block to X GB without impacting read performance.
  * Optional: CLI for vertical sharding of old blocks. 

## Non Goals

* Redesign Block index format 
* Introduce breaking changes
* Lift current limitation of index size
* Automatically shard old blocks

## Proposal

**We propose to vertically shard all blocks which goes beyond the specified index size limit during the compaction process.**

* Special metadata in meta.json indicating a shard.

TBD



## Alternatives

* Shard number stored in external label.

## Future Work

* Add [`Cuckoofilter`](https://github.com/seiflotfy/cuckoofilter) to improve query performance on store GW as well with vertically sharded blocks.

![Why Cuckoo filter: https://bdupras.github.io/filter-tutorial/](../img/cuckoo-pros.png)

See paper: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
