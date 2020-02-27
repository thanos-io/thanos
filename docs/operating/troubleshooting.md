---
title: Troubleshooting
type: docs
menu: operating
slug: /troubleshooting.md
---

# Overlaps

Overlaps should never happen in healthy and well configured Thanos system. That's why there is no automatic repair.

You can read following reasons and choose a right solution to fix it by hand.

## Reasons

- Misconfiguraiton of sidecar/ruler: Same external labels or no external labels across many block producers.
- Running multiple compactors for single block "stream", even for short duration.
- Manually uploading blocks to the bucket.
- Eventually consistent block storage until we fully implement [RW for bucket](https://thanos.io/proposals/201901-read-write-operations-bucket.md)

## Solutions

- Compactor can be blocked for some time, but if it is urgent. Mitigate by removing overlap or better: Backing up somewhere else (you can rename block ULID to non-ulid).
- Who uploaded the block? Search for logs with this ULID across all sidecars/rulers. Check access logs to object storage. Check debug/metas or meta.json of problematic block to see how blocks looks like and what is the `source`.
- Determine what you misconfigured.
- If all looks sane and you double-checked everything: Then post an issue on Github, Bugs can happen but we heavily test against such problems.
