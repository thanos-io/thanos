---
type: proposal
title: Block Series Deletion with Compactor
status: accepted
owner: yeya24
menu: proposals-accepted
---

* **Owners:**
    * @yeya24

## Summary

This design document continues the feature of series deletion for object storage discussed in [proposal](https://thanos.io/tip/proposals-accepted/202012-deletions-object-storage.md/). This proposal is based on the previous work and focuses more on the actual implementation plan and the unaddressed questions from the original proposal like performing the actual deletion in the object storage using Thanos compactor.

## Goals

* Make it more clear about the main workflow of the series deletion process.
* Perform the actual series deletion using compactor in a scalable way.

## Non-Goals

* Allow setting different retentions per series. This feature can be built on top of this work ideally but it is out of scope of this proposal.

## How

The main workflow of the series deletion will look like this:

1. User create a tombstone file via a CLI tool and the tombstone file is uploaded to the object storage.
2. Store Gateway syncs tombstone files periodically and makes the block series based on the tombstones.
3. Tombstone files have a pending period, which can be either specified by each tombstone or configured globally. Before the pending period is over, the tombstone file will remain in the object storage and user can remove a tombstone file using the CLI tool. Once the pending period is over, the compactor starts processing the tombstones and performs deletion.
4. For each tombstone file, once all the matched blocks are processed, compactor will delete the tombstone file from the object storage.

### Tombstone File

The design of the tombstone file mainly follows the previous proposal. Tombstone file is immutable. It is using a custom JSON format, single file per request and global, which means it applies to all the blocks. For the detailed considerations and tradeoffs, please refer to the original proposal.

The file format is as follows:
``` bash
cat 01G61GC0Q957TVZ2FC8S6Y2ZK7.json
{
        "ulid": "01G61GC0Q957TVZ2FC8S6Y2ZK7",
        "matchers": [
                "__name__=\"k8s_app_metric39\"",
                "cluster=\"us\"",
                "__block_id=\"01G61D0Q4BHTRC7HW08YRD0KGS\""
        ],
        "minTime": -62167219200000,
        "maxTime": 253402300799000,
        "author": "Ben Ye",
        "reason": "not specified"
}
```

Similar to the block `meta.json` file, the tombstone file is named using ULID which is basically its creation time. For other fields we have:
* **label matchers**: label matchers to match the block and series. Like this example, blocks can be matched using external labels and blocks can be matched using a special `__block_id` label. All other labels are applied to series. This design makes it easy to support multi-tenant environment and match specific blocks.
* **start timestamp**
* **end timestamp**: end timestamp should be the tombstone creation time if not specified, otherwise the compactor needs to wait for a long time to make sure all matched blocks are processed. Considering the tombstone pending period, the end timestamp can be set to `(now + pending period)` as well. We don't consider history blocks backfilling now.
* **author**
* **reason**

### Store Gateway Masks Series based on Tombstones

Like block metadata, the store gateway also fetches tombstones in a configurable interval, which means some delay is expected: user creates a tombstone but she is still able to query the data to be deleted.

There are different ways to mask series based on tombstones. We will discuss two options here:

#### Mask Series At Query Time

During query time, the store gateway decides what blocks to query based on the request min, max time and label matchers. Meanwhile, it can also decide what tombstones are going to apply for the selected blocks based on tombstone time range and label matchers.

Then during the query time, it checks the series label sets against the tombstone matchers, as well as the chunks and tombstone time range to mask the series data.

Pros:
* Easy to implement and very straightforward
* Less memory usage.

Cons:
* Slower at query time

#### Mask Series At Tombstone Sync Time

During the tombstone file sync, store gateway can also eagerly apply the tombstones to all blocks, calculate the matched postings and time range for each block. For each block, each tombstone's processed result will be stored in memory using the Prometheus `MemTombstone` [format](https://github.com/prometheus/prometheus/blob/main/tsdb/tombstones/tombstones.go#L230).

```go
// Result for the processed result for each tombstone, each block.
type MemTombstones struct {
    intvlGroups map[storage.SeriesRef]Intervals
    mtx         sync.RWMutex
}

// Result for the processed result for each block.
// Key: global tombstone ID, Value: processed result for each tombstone.
type BlockTombstones map[ulid.ULID]*MemTombstones
```

Since all the blocks and tombstones are immutable, this result can be easily cached in memory or in filesystem. Ideally for each tombstone, each block we just need to calculate once. Since `BlockTombstones` is a map, it is easy to add or delete one key if users want to add more tombstones or remove one tombstone.

During the query time, the cached tombstone will be used. As it knows which `SeriesRef`, which intervals to apply, it is very straightforward to mask series.

Pros:
* More efficient during query time, less processing required.
* As data can be cached there is not much computation required.

Cons:
* More memory required.
* If read is much less than write, it is inefficient as the pre-calculated result won't be used much.

From my perspective, option 2 is better because data can be cached.

### Tombstone Deletion with Compactor

Compactor performs the job of deleting the series from object storage and cleans up tombstone files.

The main challenge is about scalability. The reasons are:
* Global tombstone files makes the compactor difficult to know which blocks match the tombstones. It needs to scan all the block indexes to find the matched blocks.
* If one block has too many matched series to mask, the query performance can be hugely impacted. In this case, compactor planner should be able to know the ratio of the masked series / total series. If the ratio is high, the block needs to be compacted and masked series should be removed from the block.

There are other questions about how we should perform the deletion along with compaction:
* Series are deleted at normal compaction. This might need to wait a long time and hard to apply for blocks at the max compaction level.
* Rewrite blocks separately. This needs more coordination with compaction to avoid duplicate/unnecessary work.

To overcome those challenges, we propose two options.

#### Option 1: Compactor Scans Blocks

Like store gateway, compactor can also scan block indexes to find the blocks matching the tombstones. Similar to the `Mask Series At Tombstone Sync Time` of store gateway, compactor can calculate the processed tombstones for each block, each tombstone and cache the results locally. As blocks and tombstones are immutable, the result needs to be calculated only once and then reused easily. As the Prometheus format tombstones are calculated, during the compaction time the compactor just needs to generate the tombstone file in the filesystem and the TSDB compactor will apply the deletion.

There are two approaches about how to scan block indexes:
1. Download all the blocks locally and check against the tombstones.
2. Use part of the store gateway code to scan the block index and chunks on the object storage to generate `MemTombstones` result.

Approaches 2 should be better as only block indexes need to be downloaded, chunks data are reading from the object storage on the fly so disk space can be saved. As compactor is an offline component, it is acceptable to read from the object storage rather than downloading the whole data.

The pitfall is that we need to expose part of the store gateway code like `BucketIndexReader`, `BucketChunkReader` as well as other internal code to be used by the compactor.

#### Option 2: New API to Match Tombstones

Since the block indexes are already downloaded by the store gateway, it can already scan the blocks and calculate the tombstones. So a way here is to expose some APIs from store gateway, and then compactor can query the APIs to know what blocks can be deleted.

The gRPC API is called `Tombstone` API. The compactor can either query from the store gateway directly or query from queriers, and queriers do the API federation from multiple store gateway instances.

The example gRPC format looks like below. The response is almost the same as the `MemTombstone` result.

```protobuf
service Tombstone {
  rpc Tombstone(TombstoneRequest) returns (TombstoneResponse);
}

message TombstoneRequest {
  int64 min_time = 1;
  int64 max_time = 2;
  repeated LabelMatcher matchers = 3 [(gogoproto.nullable) = false];
}

message TombstoneResponse {
  repeated SeriesIntervals series_intervals = 1;
}

message SeriesIntervals {
  int64 series_ref = 1;
  repeated Interval intervals = 2;
}

message Interval {
  int64 start = 1;
  int64 end = 2;
}
```

After getting the query result, the compactor does almost the same thing as the previous option: generate the tombstone file during compaction time.

The drawback for this approach is that we are inventing a new API for this purpose only, which might be a bit of pain in the future.

#### Option 3: Delete Series using Rewrite Logic

The previous two options do the series deletion at compaction time, this option does actual series deletion using block rewrite. For the index scanning, it is still required to choose either option 1 or 2 to avoid unncessary rewrite even if the block doesn't have any matched series.

This is not so efficient as rewrite is a different process than compaction. We need to add this step and make sure rewrite can work together with compaction and no redundant work is done.

Overall, among the 3 options, option 1 is better as it is easier to implement and straightforward.

### Other Considerations

#### Compaction Planner for Tombstone

If series deletion is done at compaction time, then no much thing to change at the planner as we just need to delete series at compaction time. For the ratio calculation, as the tombstone results are pre-calculated it is easy to know the ratio as well. The threshold can be made configurable too.

The exception is for blocks that at max compaction level, no compaction will happen to it and the tombstones will remain forever if the ratio is lower than the deletion threshold. In this case the planner can check the block compaction level first, if it is max compaction level and the tombstone exists for some period of time, then do compaction for that block.

#### Tombstone File Deletion

After one compaction iteration is done, metadata will be synced again and compactor starts doing garbage collection. This time the compactor can sync tombstones again and check is there any matched block for the tombstone file. If no, then the tombstone file can be deleted.

For any new blocks that are compacted by blocks along with some tombstone requests, we want to avoid calculating tombstones on it again as those series are deleted from it already. We propose a new field in the `meta.json` file called `tombstone_applied` to indicate what tombstones have been applied to the block.

Tombstones will skip blocks that have already been applied with its ID.

### Tombstones sharding

Unlike blocks, we cannot shard tombstones simply using its ULIDs because each tombstone is able to match all blocks from the object storage. When compactor sharding is enabled, it still needs to check all the tombstones in the shard to find the matched blocks.

## Conclusions

* For store gateway, it masks series at tombstone sync time.
* For compactor, it scans block indexes directly and caches the tombstones results. During compaction time, it generates the tombstone file and series deletion is done by the TSDB compactor.
