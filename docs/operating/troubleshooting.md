---
title: Troubleshooting
type: docs
menu: operating
---

# Troubleshooting; Common cases

## Overlaps

**Block overlap**: Set of blocks with exactly the same external labels in meta.json and for the same time or overlapping time period.

Thanos is designed to never end up with overlapped blocks. This means that (uncontrolled) block overlap should never happen in a healthy and well configured Thanos system. That's why there is no automatic repair for this. Since it's an unexpected incident:
* All reader components like Store Gateway will handle this gracefully (overlapped samples will be deduplicated).
* Thanos compactor will stop all activities and HALT or crash (with metric and will error log). This is because it cannot perform compactions and downsampling. In the overlap situation, we know something unexpected happened (e.g manual block upload, some malformed data etc), so it's safer to stop or crash loop (it's configurable).

Let's take an example:

- `msg="critical error detected; halting" err="compaction failed: compaction: pre compaction overlap check: overlaps found while gathering blocks. [mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s, blocks: 2]: <ulid: 01D94ZRM050JQK6NDYNVBNR6WQ, mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s>, <ulid: 01D8AQXTF2X914S419TYTD4P5B, mint: 1555128000000, maxt: 1555135200000, range: 2h0m0s>`

In this halted example, we can read that compactor detected 2 overlapped blocks. What's interesting is that those two blocks look like they are "similar". They are exactly for the same period of time. This might mean that potential reasons are:

* Duplicated upload with different ULID (non-persistent storage for Prometheus can cause this)
* 2 Prometheus instances are misconfigured and they are uploading the data with exactly the same external labels. This is wrong, they should be unique.

Checking producers log for such ULID, and checking meta.json (e.g if sample stats are the same or not) helps. Checksum the index and [chunks files](../design.md/#chunk-file) as well to reveal if data is exactly the same, thus ok to be removed manually. You may find `scripts/thanos-block.jq` script useful when inspecting `meta.json` files, as it translates timestamps to human-readable form.

### Reasons

- You are running Thanos (sidecar, ruler or receive) older than 0.13.0. During transient upload errors there is a possibility to have overlaps caused by the compactor not being aware of all blocks See: [this](https://github.com/thanos-io/thanos/issues/2753)
- Misconfiguraiton of sidecar/ruler: Same external labels or no external labels across many block producers.
- Running multiple compactors for single block "stream", even for short duration.
- Manually uploading blocks to the bucket.
- Eventually consistent block storage until we fully implement [RW for bucket](https://thanos.io/tip/proposals/201901-read-write-operations-bucket.md)

### Solutions

- Upgrade sidecar, ruler and receive to 0.13.0+
- Compactor can be blocked for some time, but if it is urgent. Mitigate by removing overlap or better: Backing up somewhere else (you can rename block ULID to non-ulid).
- Who uploaded the block? Search for logs with this ULID across all sidecars/rulers. Check access logs to object storage. Check debug/metas or meta.json of problematic block to see how blocks looks like and what is the `source`.
- Determine what you misconfigured.
- If all looks sane and you double-checked everything: Then post an issue on Github, Bugs can happen but we heavily test against such problems.

# Sidecar

## Connection Refused

### Description

```shell
level=warn ts=2020-04-18T03:07:00.512902927Z caller=intrumentation.go:54 msg="changing probe status" status=not-ready reason="request flags against http://localhost:9090/api/v1/status/config: Get \"http://localhost:9090/api/v1/status/config\": dial tcp 127.0.0.1:9090: connect: connection refused"
```

* This issue might happen when thanos is not configured properly.

### Possible Solution

* Make sure that prometheus is running while thanos is started. The `connection_refused` states that there is no server running in the `localhost:9090`, which is the address for prometheus in this case.


## Thanos not identifying Prometheus

### Description

```shell
level=info ts=2020-04-18T03:16:32.158536285Z caller=grpc.go:137 service=gRPC/server component=sidecar msg="internal server shutdown" err="no external labels configured on Prometheus server, uniquely identifying external labels must be configured"
```

* This issue happens when thanos doesn't recognise prometheus

### Possible Solution

* Thanos requires **unique** `external_labels` for further processing. So make sure that the `external_labels` are not empty and globally unique in the prometheus config file. A possible example -

```yml
global:
  external_labels:
    cluster: eu1
    replica: 0
```
