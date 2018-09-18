# Thanos Compact - High Availability

Status: draft | in-review | rejected | **accepted** | complete

Implementation Owner: [@domgreen](https://github.com/domgreen)

## Summary

Thanos Compact currently runs as a singleton against a given bucket, this proposal outlines how we could create a highly avaliable
setup so that given one compactor dies or is unable to reach a bucket another instance of `thanos compact` can take over the data compaction.

## Motivation

If a `thanos compact` instance dies we would like another to take over compaction of the data in a bucket.

## Goals

- ensure `thanos compact` can be run in a HA manner

## Proposal

An initial proposal of adding ETCD via a interface over the existing [concurrency](https://github.com/etcd-io/etcd/blob/master/clientv3/concurrency/election.go#L44) package into `thanos compact` so that we can run master election against multiple components.


```

                 EU Cluster            │            US Cluster 
                                       │
      ┌─────────────────────────────┐  │  ┌─────────────────────────────┐
      │        Thanos Compact A     │  │  │        Thanos Compact B     │
      └──────────────┬──────────────┘  │  └──────────────┬──────────────┘
                     │                 │                 │                   
                 Check Lock            │             Check Lock        
                     │                 │                 │                   
                     v                 │                 v                  
                ┌────────────────────────────────────────────┐
                │                Global ETCD                 │
                └────────────────────────────────────────────┘
```


## User Experience

### Use Cases

- User wishes to run `thanos compact` in HA mode across multiple clusters.

### Example Use Case

A user would like to ensure more than one instance of `thanos compact` is running so that given one fails
another will take over.
In a multi-cluster setup we may want the `thanos compact` component in the second cluster to take over the work if the
original cluster fails or needs to be drained.
In a single cluster mode this could be achieved simply with kubernetes setting the number of replicas to 1 for the job.

## Implementation

### Thanos Compact

The `thanos compact` will take extra args to enable ETCD master election:

```
$ thanos compact \
    --etcd.url         "http://thanos.etcd.io
    --gcs.bucket        "blah"
    ...
    --wait
```

The `etcd.url` flag will be where we attempt to access ETCD if it is not present we assume no master election is being done
for the component and this instance will start compacting the bucket.

As this is an advanced use case for global multi-cluster compaction of data this would **not** be enabled by default and 
should be documented as such.

### Why only ETCD

We would code this against an interface to allow us to do master election against other providers but we do not see more than a 
handful of providers that would need to be implemented at this time. 
We would welcome community contributions if people would like to add this for their own use case.

## Alternatives considered

### Signal via File & External Process

An alternative to this is to use an external process of the users choosing that simply writes to a file to signal if the 
component is master or not. 
This would then most likley be a sidecar on compact that will update a file when the instance becomes master and the `thanos compact`
component would watch for the updates to a file.


### Implement via GCP / AWS storage

As we already use GCP / AWS storage we could attempt to do master election based on a file within these storage options. 
This however seems like a bad idea and would probably end up in many race conditions and therefore data corruption.


