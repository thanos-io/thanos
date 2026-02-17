---
type: proposal
title: Avoid querying blocks in object storage if they have a live source
status: approved
owner: MichaHoffmann
menu: proposals-accepted
---

## Avoid querying blocks in object storage if they have a live source

> TL;DR: We propose to not query blocks in object storage that are still served by a Sidecar/Ruler or Receiver.

## Why

Accessing blocks from object storage is generally orders of magnitude slower then accessing the same data from other sources that can query them from disk or from memory. If the source that originally uploaded the blocks is still alive and still has access to the block then we would throw away the expensively obtained data from object storage during deduplication. Note that this will not put additional pressure on the source components since they would get queried during fan-out anyway. As an example, imagine a Sidecar to a Prometheus server that has 3 months of retention and a Storage Gateway that is configured to serve the whole range. Right now we dont have a great way to deal with this dynamically. This proposal should address this hopefully.

## How

### Solution

Each source utilizes the `shipper` component to upload blocks into object storage. Additionally the `shipper` component also has a complete picture of the blocks that the source owns. If we extend the `shipper` to also own a register in object storage named `thanos/sources/<uuid>/live_blocks.json` that contains a plain list of the live blocks that this source owns. We can deduce a timestamp when it was last updated by checking attributes in object storage. When the storage gateway syncs its list of block metas, it can also iterate the `thanos/sources` directory and see which `live_blocks.json` files have been updated recently enough to assume that the sources are still alive. It can subsequently build an index of live block ids and proceed to prune them when it handles Store API requests (Series, LabelValues, LabelNames RPCs). In theory this should not lead to gaps since the block_ids are still owned by other live sources. Care needs to be taken to make sure that the blocks are within the `--min-time/--max-time` bounds of the source. The UUID for the source should be propagated in the `Info` gRPC call to the querier and through `Series` gRPC call into the storage gateway. This enables us to only prune sources that are still alive and registered with the querier. Note that this is not a breaking change - it should only be an opt-in optimization.

### Problems

1. How do we obtain a stable UUID to claim ownership of `thanos/sources/<uuid>/live_blocks.json`?

I propose that we extend Sidecar, Ruler and Receiver to read the file `./thanos-id` on startup. This file should contain a UUID that will identify it. If this file should not exist we generate a random UUID and write this file, which should give us a reasonably stable UUID for the live time of this service.

2. What happens to `thanos/sources/<uuid>/live_blocks.json` once a source churns?

We will extend the compactor to delete this directory once the timestamp in `live_blocks.json` crosses a configurable threshold. Since this is asynchronous the storage gateway also needs to disregard this file if it was not updated within a configurable threshold.

3. What happens if a block was deleted due to retention in Prometheus but shipper has not uploaded a new `live_blocks.json` file yet?

We shall only filter blocks from the `live_blocks.json` list with a small buffer depending on the last updated timestamp. Since this list is essentially a snapshot of the blocks on disk, any block deleted because of retention will be deleted after the this timestamp. Any blocks whose range overlaps `oldest_live_block_start - (now - last_updated)` could be deleted because of retention, so they should not be pruned. Example: If we were updated 1 hour ago we should not filter the oldest block from the list. If we were updated 3 hours ago we should not filter the last 2 blocks, etc.

4. What if the storage gateway has severe clockskew?

Not sure how to address this yet.

### Misc

The `live_blocks.json` layout I propose would be:

```
[<ulid>, <ulid>, ...]
```

We should configure this behavior with a pair of flags `--shipper.upload-live-blocks` on sources `--syncer.observe-live-blocks` on storage gateway. We should also have metrics for this, I propose `thanos_shipper_last_updated_live_blocks` and `thanos_syncer_last_updated_live_blocks` with unix timestamp values and `thanos_store_live_blocks_dropped` to measure how many blocks were discarded through this.

## Alternatives

1. Share a Bloomfilter using Info/Series gRPC calls

* We tend to do tons of `Series` calls and a bloomfilter for a decently sized bucket of 10k blocks could be enormous. Additionally the live blocks dont really change often so updating it on every `Series` call seems unnecessarily expensive.

2. Shared Redis/Memcached instance

* Would work similar to shared object storage, we also would need a way to update shared keys, think about their ownership and expiration.
* Object storage feels more thanos-y and since this infofmation changes slowly it feels more appropriate to use shipper and object storage here.
