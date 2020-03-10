---
title: Troubleshooting
type: docs
menu: operating
slug: /troubleshooting.md
---

# Troubleshooting; Common cases


## Overlaps

Overlaps should never happen in healthy and well configured Thanos system. That's why there is no automatic repair.

Let's take two examples:

- `msg="critical error detected; halting" err="compaction failed: compaction: pre compaction overlap check: overlaps found while gathering blocks. [mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s, blocks: 2]: <ulid: 01D94ZRM050JQK6NDYNVBNR6WQ, mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s>, <ulid: 01D8AQXTF2X914S419TYTD4P5B, mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s>`

It shows that you have really the same blocks for 2h overlaps. It worth checking whether are those blocks the same (check the number of series and samples, checksum the index and chunks).

- `msg="critical error detected; halting" err="compaction failed: compaction: pre compaction overlap check: overlaps found while gathering blocks. [mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s, blocks: 2]: <ulid: 01D94ZRM050JQK6NDYNVBNR6WQ, mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s>, <ulid: 01D8AQXTF2X914S419TYTD4P5B, mint: 1555128000000, maxt: 1555142400000, range: 4h0m0s>`

It shows there are two blocks overlapping in 2h time range. One is 4h, the other is 2h.It's more tricky. This suggests you uploaded compacted blocks rather. Again looking on meta.json would help.

You can read following reasons and choose a right solution to fix it by hand.

### Reasons

- Misconfiguraiton of sidecar/ruler: Same external labels or no external labels across many block producers.
- Running multiple compactors for single block "stream", even for short duration.
- Manually uploading blocks to the bucket.
- Eventually consistent block storage until we fully implement [RW for bucket](https://thanos.io/proposals/201901-read-write-operations-bucket.md)

### Solutions

- Compactor can be blocked for some time, but if it is urgent. Mitigate by removing overlap or better: Backing up somewhere else (you can rename block ULID to non-ulid).
- Who uploaded the block? Search for logs with this ULID across all sidecars/rulers. Check access logs to object storage. Check debug/metas or meta.json of problematic block to see how blocks looks like and what is the `source`.
- Determine what you misconfigured.
- If all looks sane and you double-checked everything: Then post an issue on Github, Bugs can happen but we heavily test against such problems.
