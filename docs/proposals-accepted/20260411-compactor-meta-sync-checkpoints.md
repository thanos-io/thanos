## Implement checkpoints for SyncMetas in compactors

* **Owners:**  
  * `@waltherlee`

TL;DR: Implement on-disk and in-memory checkpoints to speed up metadata syncs in the compactors.

## Why

With downsampling enabled, compactors sync meta files at least 4 times every cycle, beginning as soon as they start. In addition, if progress metrics are enabled, another sync runs periodically every 5 minutes by default.

A sync walks through every block in the bucket and stores their meta files in a local cache. The storing process gets faster in following syncs as the cache fills up, but the whole iteration takes always the same time. 

For users with 1M+ blocks, latency and disk lookups can make a fully-cached listing take a long time. Even longer if the sync uses a recursive discovery strategy.

With this many blocks, every sync essentially halts compaction.

### Pitfalls of the current solution

Most blocks listed don’t change between syncs. The files are sorted by the block’s ULID ID, and the first chars are the creation timestamp, so newly created ones are always listed last.

In a single cycle, the compactors use syncs to catch two types of events:

* Newly created blocks that must be loaded and considered in following compaction plans.  
* Blocks deleted from the bucket. Technically, only one compactor must run against a stream of blocks, so it should be able to tell if blocks were deleted and a full sync is necessary. Otherwise, the list of relevant preexisting blocks doesn’t really change between syncs.

There’s also blocks marked for deletion, but the compactors check and exclude blocks marked in a separate step, so following syncs don’t change this result either.

Then, the main purpose of syncing multiple times is to find new blocks, and with the current solution, a lot of iteration time is redundant. It would be useful to support checkpoints and a \`startAfter\` option in the bucket listing to resume an interrupted sync after a restart, and to only sync preexisting blocks once per cycle.

## Goals

* Implement on-disk checkpoints to resume a sync after a restart.  
* Implement in-memory checkpoints to list only new blocks after the first sync in a cycle. This will speed up the one used periodically for progress metrics as well.  
* Clear both checkpoints after a completed cycle.

## Non-Goals

* Implement new discovery strategies. Checkpoints will work with both existing ones  
* Improve caching or disk lookups. Having millions of blocks cached on disk in a HDD is a big bottleneck even without listing. This won’t try to fix that.

### Audience

* Users with a long list of blocks in a single bucket.  
* Users who added compactors later, after ingesting a high number of blocks.  
* Users with compactors that have been lagging for long, piling up pending blocks in the bucket. 

## How

The compactors already use `BaseFetcher` to sync metadata. This adds 5 new fields:

* `enableCheckpoint` `bool`, true to enable checkpoints. False by default.  
* `blocksDownloading` `struct`, sorted list to keep track of downloads pending.   
* `lastCheckpointableID` `ulid.ULID`, all blocks up to this ID have been completely downloaded.  
* `knownBlockIDs` `map[ulid.ULID]struct{}`, all block IDs known since last compaction.  
* `checkpointDir` `string`, for the on-disk checkpoint.

`blocksDownloading` keeps track of blocks downloading. After completion, only blocks with no previous blocks pending will be used for checkpoints.

With this strategy, `lastCheckpointableID` can be suboptimal but it won’t miss blocks. For example, if the blocks A, B, and C are pending, and they finish in the opposite order, the checkpoint used will be A. This is acceptable because the concurrency is low compared to the number of blocks, and the extra lookups cost minimal time because the files are already cached.

`knownBlockIDs` is a list of all blocks previously listed and cached. It could be replaced with a list of all blocks on disk, but after multiple interrupted cycles, data on disk can be stale.

For the on-disk checkpoint, `lastCheckpointableID` and `knownBlockIDs` are stored in a gzip-compressed JSON file in `checkpointDir`. If a file exists, it is loaded to `BaseFetcher` when the compactor starts. 

To resume a sync, the `GetActiveAndPartialBlockIDs` method in the `Lister` interface in the package `block` takes a new `startAfter` `string` that is passed to the object storage implementation in `obj_store`. Storage must respond by listing only objects alphabetically higher than the value. In this case, it doesn’t matter if it’s inclusive or not.

After the first sync, following ones in the same cycle will only list new blocks using the `startAfter` option. This includes syncs outside the main compact function, like the goroutine used to update progress metrics.

The compactor saves the checkpoint file after every `syncMetas` in the main compaction cycle. Basically before starting compactions, before each of both downsampling cycles and before applying retention policies. It also saves a checkpoint if an error interrupts the main `runCompact`. 

At the end of a cycle it removes the file and clears the in-memory checkpoint in `BaseFetcher`.

The checkpoint implementation is by default disabled and will have to be enabled with a flag.

**NOTE**: Only one compactor should run against a stream of blocks. That means that some blocks could be deleted from storage between syncs, but if they were not deleted by the running compactor, then they were out of scope and are not relevant for the running compactions and downsamplings. However, I’ve noticed that this isn’t always the case, and sometimes, relevant blocks do get deleted due to race conditions or previous bugs.

This will also need a safeguard measure to clear both caches and run a full sync if the compactor finds a retriable “specified key does not exist” error.

## Alternatives

The alternatives to skipping blocks using `startAfter` would be increasing concurrency and/or changing the discovery strategy. 

However, this is still a redundant cost, and none of them save disk lookups.

## Action Plan

- [ ] Add support for `startAfter` in all implementations in `obj_store`.  
- [ ] Update `BaseFetcher` to keep track of known blocks and the last checkpointable ID (in-memory checkpoints).  
- [ ] Implement on-disk checkpoints.  
- [ ] Use checkpoint data to skip blocks and resume an interrupted sync.
