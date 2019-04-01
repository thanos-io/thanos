# Read-Write coordination free operational contract for object storage

Status: draft | in-review | rejected | **accepted** | complete

Implementation Owner: [@bwplotka](https://github.com/bwplotka)

Tickets:
* https://github.com/improbable-eng/thanos/issues/298 (eventual consistency)
* https://github.com/improbable-eng/thanos/issues/377 (eventual consistency & partial upload)
* https://github.com/improbable-eng/thanos/issues/564 (retention vs store gateway)
* https://github.com/improbable-eng/thanos/issues/271 (adapt for removals faster)

## Summary

Our goals here are:

* A) Define consistent way of having multiple readers and multiple writers (technically multiple appenders and single remover) on shared object storage that can be
eventual consistent.
* B) Allow readers to sync the object storage changes in eventual consistent manner: (e.g to poll for changes
every X minutes instead of watch and react to changes immediately)
* C) Allow readers to detect and handle partial uploads consistently without any coordination.

## Motivation

Thanos performs similar operations as Prometheus do on TSDB blocks with the following differences:
* It operates on Object Storage API instead of local filesystem.
* Operations are done from multiple processes that lack coordination.

Moving from lock-based logic to coordination free and from strongly consistent local filesystem to potentially eventually
consistent remote simplified "filesystem" in form of Object Storage API, causes additional cases that we need to consider
in Thanos system, like:
* Thanos sidecar or compactor crashes during the process of uploading the block. It uploaded index, 2 chunk files and crashed. How to ensure readers (e.g compactor, store gateway) will handle this gracefully?
* Thanos compactor uploads compacted block and deletes source blocks. After next sync iteration it does not see a new block (read after write eventual consistency). It sees gap, wrongly plans next compaction and causes non-resolvable overlap.
* Thanos compactor uploads compacted block and deletes source blocks. Thanos Store Gateway syncs every 3m so it missed that fact. Next query that hits store gateway tries to fetch deleted source blocks and fails.

Currently, we have only basic safeguards against those in form of [`syncDelay`](https://github.com/improbable-eng/thanos/blob/bc088285a1b4bf464fdf2539e4b365b805874eed/pkg/compact/compact.go#L187).
That helps with operations between sidecar and compactor but does not fix inter-compactor logic in case of eventual consistent storage or partial upload failures.

We also partially assumed strong consistency for object storage, because [GCS has strong consistency](https://cloud.google.com/storage/docs/consistency) and S3 has it as well,
but with [some caveats](https://codeburst.io/quick-explanation-of-the-s3-consistency-model-6c9f325e3f82) that we met.

However with increasing number of other providers (ceph, hdfs, COS, Azure, Swift) with no (or unknown) strong consistency
guarantee this assumption needs to go away.

We have an explicit API for Series fetching in form of gRPC StoreAPI, but we lack the verbose API for other side: the object storage format with assumptions on behavior that will
cover partial blocks, eventual consistency and syncing requirements, which is a goal for this proposal.

## Proposal

We propose defining those set of rules to solve A, B and C goals that will define the object storage format API (and how to operate with those objects):

1. A block, once uploaded is never edited (no file within ULID directory is removed, edited or added).
1. A block is assumed ready to be read *only* after 15 minutes (configured by `syncDelay`) from *creation time*. Block id (`ULID`) is used
   to determine creation time (`if ulid.Now()-id.Time() < uint64(c.syncDelay/time.Millisecond)`). Block fresher than 15 minutes is assumed
   still in upload (`Fresh`). This can be overridden by `ignore_delay` field in meta.json.
1. TSDB `meta.json` file within block *have to be* uploaded as the **last** one in the block folder.
1. After 15 minutes (or time configured by `syncDelay`), if there is no fully loadable `meta.json`, we assume block is malformed, partially uploaded or marked for deletion and can be
   *removed* by compactor. [Compactor change needed]
1. Overlaps (in external labels and time dimensions) with the same data are allowed and should be handled by readers gracefully. [Compactor change needed]
1. Adding "replacement" or "overlapped" block causes old blocks to be scheduled for removal when new block is ready by compactor (`HealthyOverlapped` state). [Compactor/Repair change needed]
1. To schedule delete operation, delete `meta.json` file. All components will exclude this block and compactor will do eventual deletion assuming the block is partially uploaded. [Compactor change needed]
1. Compactor waits minimum 15m (`deleteDelay`) before deleting the whole `To Delete` block. [Compactor change needed]

Below diagram shows the block states defined by our rules.

[![Diagram](https://docs.google.com/drawings/d/e/2PACX-1vSZjddrWBkf2ynczeIMSzGo-yqe7Cg9X-TG_LjyWAP_ToyZolMEG_UnbNlvoSetRhDLmlfABd_rJ4W9/pub?w=960&h=720)](https://docs.google.com/drawings/d/1leRN6GUvU98Ur2TowMefk4ycTFNBRi9t1-dGtOPmHg0/edit?usp=sharing)

### Why?

To deal with A and B, so overall eventual consistency, we will leverage existing feature of metric format (TSDB) which is
*block immutability*. In object storage format, a block is a directory named with [`ULID`](https://github.com/oklog/ulid) that contains
`chunks` dir with one or more chunk files (each not bigger than 512MB), `index` and `meta.json` files. To ensure every component
has the same state of metrics we create first strict rule:

> 1 . A block, once uploaded is never edited (no file within ULID directory is removed, edited or added).

With this rule alone we restrict bucket operations only to:
* Block upload
* Block removal

As those operations are not atomic itself, partial blocks or inconsistent block view may occur for short time (read-after-write consistency).
To mitigate this we introduce second rule:

 > 2 . A block is assumed ready to be read *only* after 15 minutes (configured by `syncDelay`) from *creation time*. Block id (`ULID`) is used
to determine creation time (`if ulid.Now()-id.Time() < uint64(c.syncDelay/time.Millisecond)`). Block fresher than 15 minutes is assumed
still in upload (`Fresh`). This can be overridden by `ignore_delay` field in meta.json.

Readers may attempt to load block's files (e.g meta.json to grab external labels), but should be aware that those files can be partially uploaded.

The delay is not only for block eventual consistency. Even if everyone can read block properly, we are not aware if all readers loaded the block properly.

To ensure we have block fully uploaded we define:

> 3 . TSDB `meta.json` file within block *have to be* uploaded as the **last** one in the block folder.

Currently and as proposed `meta.json` is our integrity marker (it is uploaded as the last one). It matters even when we don't have strong consistency, because
it helps us detect the case of `upload` crashed in between of upload. We only upload meta.json after successful uploads of rest of the files for the block.

With those rule one could say "why to wait 15 minutes if we can just check if meta.json is parsable". In eventual consistency world
we cannot just check meta.json as even when it was uploaded at the end, other files might be still unavailable for a reader.

Thanks of meta.json being uploaded at the end we can form 4th rule:

> 4 . After 15 minutes (or time configured by `syncDelay`), if there is no fully loadable `meta.json`, we assume block is malformed, partially uploaded or marked for deletion and can be
*removed* by compactor. [Compactor change needed]

This is to detect partial uploads caused by for example compactor being rolled out or crashed. Other writers managed by `shipper` package relies on
fact that blocks are persisted on disk so the upload operation can be retried.

Similar to writers, readers are excluding all blocks fresher than 15m or older but without `meta.json`. Overall those 4 rules
solved issue C) and A), B) in terms of block upload.

However, sometimes we need to change blocks for reasons, like:

* compaction, so merging couple of blocks into one with shared index.
* potentially in future: `delete series` operations.
* repair operations (e.g fixes against Prometheus or Thanos bugs like index issues).

Since all blocks are immutable, such changes are performed by rewriting block (or compacting multiple) into new block. This
makes `removal` operations a first citizen object lifecycle. The main difficult point during this process is to make sure all
readers are synced and aware that new block is ready in place of the old one(s), so writer can remove old block(s). In eventual consistency system
we don't have that information without additional coordination. To mitigate we propose 2 new rules. All to support lazy deletion:

> 5 . Overlaps (in external labels and time dimensions) with the same data are allowed and should be handled by readers gracefully. [Compactor change needed]

This should be the case currently for all components except compactor. For example store having overlapping blocks with even the same data
or sidecar and store exposing same data via StoreAPI is handled gracefully by frontend.

The major difference here is compactor. Currently compactor does not allow overlap. It immediately halts compactor and waits for
manual actions. This is on purpose to not allow block malformation by blocks which should be not compacted together.

"Overlaps with the same data" is crucial here as valid overlaps are when:

*Newer block from two or more overlapping blocks fully submatches the source blocks of older blocks. Older blocks can be then ignored.*

The word **fully** is crucial. For example we won't be able to resolve case with block ABCD and CDEF. This is because there is no logic for decompact or vertical compaction.

Having this kind of overlap support, we can delay deletion by forming 6th rule:

> 6 . Adding "replacement" or "overlapped" block causes old blocks to be scheduled for removal when new block is ready by compactor (`HealthyOverlapped` state). [Compactor/Repair change needed]

This rule ensures that we can detect *when* to delete block, and we delete it only once new block is ready, so when all readers already loaded new block (e.g to syncDelay).

This also means that repairing block gracefully (rewriting for deleting series or other reason that does not need to be unblocked), is as easy as uploading new block. Compactor will delete older overlapped block eventually.

There is caveat for rule 2nd rule (block being ready) for compactor. In case of compaction process we compact and we want to be aware of this block later on. Because of
eventual consistent storage, we cannot, so we potentially have uploaded a block that is "hidden" for first 15 minutes. This is bad, as compactor
will see the source blocks (see 6th rule why we don't delete those immediately) and will compact same blocks for next 15 min (until we can spot the uploaded block).
To mitigate this compactor:
* Attempts reading meta.json from not-ready block
* Maintains meta.json of newly compacted (and downsampled) blocks in filesystem, similar as shipper is (but it has full blocks).

In the worst case it is possible that compactor will compact twice same source blocks. This will be handled gracefully because of rule 5th and 6th (valid overlaps are ok and older submatching will be deleted anyway later on)

To match partial upload safeguards we want to delete block in reverse order:

> 7 . To schedule delete operation, delete `meta.json` file. All components will exclude this block and compactor will do eventual deletion assuming the block is partially uploaded. [Compactor change needed]

We schedule deletions instead of doing them straight away for 3 reasons:
* Readers that have loaded this block can still access index and metrics for some time. On next sync they will notice lack of meta.json and assume partial block which excludes it from block being loaded.
* Only compactor deletes metrics and index.
* In further delete steps, starting with meta.json first ensures integrity mark being deleted first, so in case of deletion process being stopped, we can treat this block as partial block (rule 4th) and delete it gracefully.

There might be exception for malformed blocks that blocks compaction or reader operations. Since we may need to unblock the system
immediately the block can be forcibly removed meaning that query failures may occur (reader loaded block, but not aware block was deleted).

> 8 . Compactor waits minimum 15m (`deleteDelay`) before deleting the whole `To Delete` block. [Compactor change needed]

This is to make sure we don't forcibly remove block which is still loaded on reader side. We do that by counting time from
spotting lack of meta.json first. After 15 minutes we are ok to delete the whole directory.

## Risks

* What if someone will set too low `syncDelay`? And upload itself will take more than `syncDelay` time. Block will be assumed as `ToDelete` state and will be removed. Other use case is when Prometheus is partitioned/misconfigured from object storage for longer time (hours?). Once up it will upload all blocks with ULID older than time.Now-sync-delay. How to ensure that will not happen?
  * Grab time for syncDelay from object meta instead? Not from ULID? That is quite hard.
  * Thanks of `deleteDelay` it might have still some time to recover, we might rely on that.
  * Add safeguard on upload path? `upload timeout` has to smaller than `syncDelay`.
  * Minimum syncDelay

* What if one block is malformed. Readers cannot handle it and crashes. How repair procedure will work? We can have repair process that can download block locally, rewrite it and fix it and upload. Problem is that it will take `syncDelay` time to
appear in system. Since we are blocked, we need to make the block available immediately, ignoring eventual consistency limitations.
Potential solutions:
  * Just delete problematic block and live with 15 minute of delay for not having some portion of metrics? Will it not cause any downsampling issues?
  * Use `ignore_delay` option and avoid Fresh state as possible. Eventual consistency issue may hit us.

* Do we allow repair procedure when compactor is running? This is quite unsafe as compaction/downsamlping operation might be in progress
so repair procedure might be avoided. Turning off compactor might be tricky as well in potential HA mode. Maybe we want to introduce
"locking" some external labels from compactor operations. If yes, how to do it in eventual consistency system?


## Action Items:

* Ensure `syncDelay` is implemented on all components (handling fresh blocks) with reasonable minimum value for syncDelay (e.g 5 minutes?)
* Ensure readers (e.g store gateway) handle `Healthy/HealthyOverlapped` -> `toDelete` state change.
* Implement handling of `HealthyOverlapped` block based on source ULIDs from meta file.
* Delete should start from deleting meta.json. Add `ScheduleDelete` that will delete only meta.json
* Add `deleteDelay` support for compactor on apply on `toDelete` blocks.
* [Low] Save successful compactor block uploads to persistent file, same as `shipper` to avoid unnecessary duplicated compactions/downsamplings
* [Low] Add `ignore_delay` parameter that will ignore syncDelay for sudden repairs.
* [Low] Ignore `HealthyOverlapped` to reduce resource consumption for store gateway.
* Test common and edge cases e.g:
  * Worst case scenario for compaction partial upload and multiple duplicated compaction.
  * Rule 5th validation: Handling valid and invalid overlaps for various components.

## Alternatives:

### Require strong consistent object storage.

As mentioned in #Motivation section this would block community adoption as only a few of object storages has strong or even
 clearly stated consistency guarantees.

### Full immutability

By not removing any object, we may simplify problem. However we need hard deletions, because they are:
- enormously reducing storage size (e.g retention is exactly for this)
- enormously reducing number of blocks in storage, improving syncing time.

### Special integrity JSON file

We may want to add some file to commit and mark integrity of the block. That file may hold some information like
files we expect to have in the directory and maybe some hashes of those.

This was rejected for we can reuse existing meta.json as "integrity" marker (e.g if after 15m we don't have meta file, it is partially uploaded and should be removed).

Additional information like expected number of files can be added later if needed, however with rule no 3. we should not need that.

The main drawback of this is changing or required additional things from TSDB block format which increases complexity.

### Additional coordination based on system with strong consistency (e.g etcd)

As this would help with mentioned issues, Thanos aims for being coordination free and having no additional system dependency to
stay operationally simple.

### Optimistic/vertical compaction

Currently, compactor requires the blocks to appear in order of creation. That might be not the case for eventual consistent
 storage. In those cases compactor assumes gap (no block being produced) and compacts with the mentioned gap. Once the new
 block is actually available for reading it causes compactor to halt due to overlap. [Vertical compaction](https://github.com/prometheus/tsdb/pull/370)
 might help here as can handle those overlaps gracefully, by compacting those together. This however:

 * Heavily complicates the compaction
 * Malformed block in case of simple misconfiguration (misconfigured external labels - quite often happens)
 * There are lots of edge cases for eventual consistent storage that will cause vertical compaction to be required quite often
 which will affect performance.
 * Does not help with partial upload and syncing issues

 As vertical compaction might be something we can support in future, it clearly does not help with problems we stated here.

### Do not introduce `deleteDelay`

We may avoid introducing additional state, by adding mitigation for not having delayed removal.

For example for retention apply or manual block deletion, when we would delete block immediately we can have query failures (object does not exists for getrange operations)

We can avoid it by:
* Adding `retention` flag to readers (compactor have it, but store gateway does not). Cons:
  * This will increase configuration burden as you need to configure retention for each resolution on each reader.
  * This does not cover other deletion reasons, like manual removal (e.g for repair)
* Handle not exists errors for queries against store gateway gracefully. For instance, if we have block loaded and bucket operation will return object not exists. We can return no data. Cons:
  * We might hide actual errors. No difference between "healthy" no exists error and failure one.
