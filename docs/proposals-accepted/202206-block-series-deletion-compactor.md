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

* Make the main workflow of the series deletion process clearer.
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

The design of the tombstone file mainly follows the previous proposal. Tombstone file is immutable. It is using a custom JSON format, single file per request and global, which means it applies to all the blocks if no external label or block ID matchers are specified. For the detailed considerations and tradeoffs, please refer to the original proposal.

The file format is as follows:

```bash
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

Similar to the block `meta.json` file, the tombstone file is named by ULID from its creation time. For other fields we have:
* **label matchers**: label matchers to match the block and series. Like this example, blocks can be matched using external labels and blocks can be matched using a special `__block_id` label. All other labels are applied to series. If no external label matcher and block ID matcher specified, tombstones apply to all blocks. This design makes it easy to support multi-tenant environment and match specific blocks.
* **start timestamp**
* **end timestamp**: If end timestamp can be a future time, then the compactor might need to wait for a long time to make sure all matched blocks are processed. To avoid this, end timestamp is the tombstone creation time if not specified and cannot be later than it. Considering the tombstone pending period, the end timestamp can be set to `(now + pending period)` as well. We don't consider history blocks backfilling for now.
* **author**
* **reason**

### Tombstone Deletion with Compactor

Compactor performs the job of deleting the series from object storage and cleans up tombstone files.

The main challenge is about scalability. The reasons are:
* Global tombstone files make the compactor difficult to know which blocks match the tombstones. It needs to scan all the blocks to find the blocks matched.
* If one block has too many matched series to mask, the query performance can be hugely impacted. In this case, compactor planner should be able to tell the ratio of the masked series / total series. If the ratio is high, the block needs to be compacted and masked series should be removed from the block.

There are other questions about how we should perform the deletion along with compaction:
* Series are deleted at normal compaction. This might need to wait a long time and hard to apply for blocks at the max compaction level.
* Rewrite blocks separately. This needs more coordination with compaction to avoid duplicate/unnecessary work.

To overcome those challenges, I'd like to propose one solution with blocks scanning from the compactor.

#### Proposed Option: Compactor Scans Blocks and Cache Tombstone Results

Compactor can scan block indexes to find the blocks and series matching the tombstones. In Prometheus, an in-memory `MemTombstone` [format](https://github.com/prometheus/prometheus/blob/main/tsdb/tombstones/tombstones.go#L230) is used to efficiently mask and delete series during query and compaction time.

With block index, compactor can just calculate the prometheus `MemTombstone` for each block, each tombstone and cache the results locally. We can use the code below.

As blocks and tombstones are immutable, the result needs to be calculated only once and then reused easily. As the Prometheus format tombstones are calculated, during the compaction time the compactor just needs to generate the tombstone file in the filesystem and the TSDB compactor will apply the deletion.

There are two approaches about how to scan blocks:
1. Download blocks locally and check against the tombstones. This needs a large amount of disk space and we might download and check each block one by one.
2. Use part of the store gateway code to scan the block index and chunks on the object storage to generate `MemTombstones` result.

Approach 1 is simple and easy to implement. But it doesn't scale because it needs to download the block locally and then check the tombstone matchers. If there is no matching series then it is a waste of time and disk space to download the whole block.

So we propose our solution with option 2. With this option, only block indexes need to be downloaded, chunks data are reading from the object storage on the fly so disk space can be saved. As compactor is an offline component, it is acceptable to read from the object storage rather than downloading the whole data.

Another change is that we need to expose part of the store gateway code like `BucketIndexReader`, `BucketChunkReader` as well as other internal code to be used by the compactor. Some refactorings are needed to make those code reusable.

#### Alternatives

##### New API to Match Tombstones

Since the block indexes are already downloaded by the store gateway, it can already scan the blocks and calculate the tombstones. So a way here is to expose some APIs from store gateway, and then compactor can query the APIs to get the tombstones or know which blocks to download for deletion.

We propose a new `Tombstone` gRPC API at store gateway and querier is able to federate `Tombstone` API from multiple store gateways instances. The compactor can query from queriers to get required tombstones data. Querier federation is required because each store gateway only takes care of some blocks when sharding is enabled.

The example gRPC request and response look like below. The response is almost the same as the `MemTombstone` result.

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

After getting the query result, the compactor does almost the same thing as the proposed option: generate the tombstone file during compaction time.

This option can be considered as a variation for the proposed solution. Compactors avoid scanning blocks by querying store gateway, thus improving scalability.

The drawback for this approach is that we are inventing a new API for this purpose only, which might be a bit of pain in the future.

##### Delete Series using `thanos tools block rewrite` CLI

To perform series deletion, there is a `block rewrite tool`. But to integrate it with the compactor, the CLI won't be used directly. Instead, we will reuse the code to implement the rewrite logic in the compactor.

To perform block rewrite, we still need to know what blocks to rewrite/delete so block scanning is still required to avoid unnecessary operations. For the blocks scanning, either scanning blocks directly by the compactor or querying `Tombstone` APIs can be used.

Overall, this is not efficient as rewrite is a different step from compaction so blocks might need to be compacted multiple times, causing [write amplification](https://en.wikipedia.org/wiki/Write_amplification) issues. We also need to add this step to the planner and make sure it works together with existing compaction.

### Additional Design Considerations

#### Store Gateway Masks Series based on Tombstones

Like block metadata, the store gateway also fetches tombstones in a configurable interval, which means some delay is expected: user creates a tombstone but she is still able to query the data to be deleted.

There are different ways to mask series based on tombstones. In the previous proposal, we didn't mention the detailed implementation. We will discuss two options here:

##### Option 1: Mask Series At Query Time

During query time, the store gateway decides what blocks to query based on the request min, max time and label matchers. Meanwhile, it can also decide what tombstones are going to be applied for the selected blocks based on tombstone time range and label matchers.

Then during the query time, it checks the series label sets against the tombstone matchers, as well as the chunks and tombstone time range to mask the series data.

Pros:
* Easy to implement and very straightforward
* Less memory usage.

Cons:
* Slower at query time

##### Option 2: Mask Series At Tombstone Sync Time

During the tombstone file sync, store gateway can also eagerly apply the tombstones to all blocks, calculate the matched postings and time range for each block. The result is the same as `MemTombstone` we mentioned before.

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

Since all the blocks and tombstones are immutable, this result can be easily cached in memory or in filesystem. Ideally for each tombstone, each block we just need to calculate once. Because `BlockTombstones` is a map, it is easy to add or delete one key if users want to add more tombstones or remove one tombstone.

During the query time, the cached tombstone will be used. As it knows which `SeriesRef`, which intervals to apply, it is very straightforward to mask series.

Pros:
* More efficient during query time, less processing required.
* As data can be cached there is not much computation required.

Cons:
* More memory required.
* If read is much less than write, it is inefficient as the pre-calculated result won't be used much.

##### Considerations

Considering the infrequent change of tombstones, option 2 is preferred as all data can be easily cached, and it is faster during query time.

#### Compaction Planner for Tombstone

If series deletion is done at compaction time, then there's not much thing to change about the planner, as we just need to delete series at compaction time. For the ratio calculation, as the tombstone results are pre-calculated it is easy to know the ratio as well. The threshold can be made configurable too.

The exception is for blocks that at max compaction level, no compaction will happen to it and the tombstones will remain forever if the ratio is lower than the deletion threshold. In this case the planner can check the block compaction level first, if it is max compaction level and the tombstone exists for some period of time, then do compaction for that block.

#### Tombstone File Deletion

After one compaction iteration is done, metadata will be synced again and compactor starts doing garbage collection. This time the compactor can sync tombstones again and check is there any matched block for the tombstone file. If no, then the tombstone file can be deleted.

For any new blocks that are compacted by blocks along with some tombstone requests, we want to avoid calculating tombstones on it again as those series are deleted from it already. We propose a new field in the `meta.json` file called `tombstone_applied` to indicate what tombstones have been applied to the block.

Tombstones will skip blocks that have already been applied with its ID.

#### Tombstones sharding

Unlike blocks, we cannot shard tombstones simply using its ULIDs because each tombstone is able to match all blocks from the object storage. When compactor sharding is enabled, it still needs to check all the tombstones in the shard to find the matched blocks.

## Conclusions

* For store gateway, it masks series at tombstone sync time.
* For compactor, it scans block indexes directly and caches the tombstones results. During compaction time, it generates the tombstone file and series deletion is done by the TSDB compactor.
