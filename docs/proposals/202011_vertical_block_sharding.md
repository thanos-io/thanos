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
* Alternatively, increase the number of compactions before index size hits the current limitations, to improve performance of querying.

## Non Goals

* Redesign Block index format 
* Introduce breaking changes
* Lift current limitation of index size
* Automatically shard old blocks

## Proposal

**We propose to vertically shard all blocks which goes beyond the specified index size limit during the compaction process, else efficiently shard the blocks at compaction level 0.**

* Special metadata in meta.json indicating a shard.

#### Approach 1: Sharding at compaction level 0

The idea behind this approach is to use a hash function which takes the metric_name as an input, and returns an integer value in the range of 1 to X (X denoting the number of shards). As of now, X is static and has been set to an experimental value of 10. We would also like decide whether keeping X dynamic would be a better approach. To do that we would have to include some sort of consistent hashing mechanism so that X can be changed dynamically as per the need of the user.

For this approach, we'd begin by adding an external label, `hash_number` during block creation, which would denote the shard number. We can keep it 0, because subsequently the hash function would return values only between 1 and X (inclusive), so there won't be any chance of a possible clash. During the first compaction of the initially created 2h blocks, we'll divide each block into X blocks. The block number i (1<=i<=X) would contain only those series whose hash value of mertic names is i.

Once this step is completed, we can be certain that all the series that belong to a certain block have the same `hash_number`. Now, we'll allow grouper and planner to group only those blocks together which have the same `hash_number`. This would serve us two purposes : 

 * Allow grouper to group blocks in such a way that we can run X concurrent compactor processes.
 * Intra-shard horizontal and vertical compaction would mean less variance in metrics and hence we can expect a larger block in terms of the difference in `maxt` and `mint` when a certain blocks index hits the limit, and then marked with `noCompact`.

#### Approach 2: Sharding a block when its index size hits the limit

This approach is a slight modification to Apporach 1. 
Instead of creating just one external label `hash_number`, we can create a series of hash_numbers and corresponding `hash_function`s.
This is also a static method, but an optimization over Approach 1. 
The way it would work is, intially during block creation, we'll add `n` external labels 
```
hash_number_1 : 0
hash_number_2 : 0
.
.
.
hash_number_n : 0
```
Wherein, n is the number of times we want a blocks index to hit the limit. 
The current method ignores a block for further compaction once its index reaches a limit. We'd let that stay as it is, with a caveat that whenever a blocks index size hits the limit, we'd find the first 0-valued hash_number (let's say i), then further sub-divide (shard) the existing block with the corresponding `hash_function_i`, and then let the compaction process run as usual.
Here also, we'd allow grouper and planner to compact (horizontally and vertically) only those blocks together whose `hash_number` external labels match completely. 
The advantage of this method is, that it would allow us to help grow the size of the block in terms of time difference  (`maxt - mint`) exponentially, so we can make an estimated guess that n would not exceed 3 or 4. 
The negative aspect of this approach would be, as blocks grow larger, it would become difficult to find a subsequent block to compact it with.

### Design Decisions (Pros and Cons)

#### Decision 1: Shard always by X (static), for everything 
* Pros
  * We can grow the time window a block spans, even if total index size reaches cap limit, because we vertically shard it into X blocks.
  * The compaction process is simple and predictable, as compaction of any sort would take place between blocks that belong to the same shard. 
* Cons
  * The problem of index size hitting limit is still present.
  * Statically sharding the block into X can be an overkill (if X is too large, say 10 for a few cases), or it might underperform (if X is as less as 2 or 3) for some massive TSDB with larger magnitude of metric numbers.  

#### Decision 2: Using metric name as the only parameter for hash_function 
* Pros
  * It is simple to implement and can prove to be quite efficient while querying because metric names can be obtained from the query itself, and calculating the corresponding hash would take minimal time as well.
  * For deeper hash layers, we can statistically be certain of having an even distribution (for Approach 2).
* Cons 
  * If the nested hashing is shallow, then there's a possibility of uneven sharding.

## Alternatives

* Keep the system as it is.
* In the above approach, keep the number of shards dynamic, depending on the use case.
  * Pros
    * We can dynamically adjust to the situation. If a certain block stream has only few metrics, we can keep 1 shard, and if during the 2w period, suddenly we have 100 millions series (or some number beyond index size), we can horizontally split to 10 or more. And then later back to 1.
   * Cons
     * With dynamically changing metas, it would become difficult to group blocks together for vertical compaction.

## Future Work

* Add [`Cuckoofilter`](https://github.com/seiflotfy/cuckoofilter) to improve query performance on store GW as well with vertically sharded blocks.

![Why Cuckoo filter: https://bdupras.github.io/filter-tutorial/](../img/cuckoo-pros.png)

See paper: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
