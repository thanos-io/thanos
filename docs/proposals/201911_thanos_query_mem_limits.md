---
title: Thanos Query Memory Limits
type: proposal
menu: proposals
status: pending
owner: ppanyukov
---

### Related Tickets

Quite a few related to OOM and improving memory utilisation:

* https://github.com/thanos-io/thanos/issues/448
* https://github.com/thanos-io/thanos/issues/1625 (previously failed attempt and PR)
* https://github.com/thanos-io/thanos/issues/1649 (discussion with ideas)
* https://github.com/thanos-io/thanos/issues/1705 (another tracking issue)


## Summary

This document describes the motivation and design of memory accounting and limits for query execution in Thanos. This affects both Querier (Q) and Store Gateway (SG) components.

## Motivation

**TL;DR: Availability is the main issue.**

Both Store Gateway (SG) and Querier (Q) OOMs at query time. Why is this a problem?

* Thanos is used by multiple customers (users).
* One query issued by one customer can OOM SG or Q, which kills all other queries.
* This same query gets retried many times.
* Effectively becoming a DOS attack.
* Making the entire service completely unavailable to everyone.

Conclusion:
* Availability of the service is the main issue.

## Top-level idea

Introduce memory accounting and limits in SG and Q components. This will ensure that one query does not take over all resources. Combined with the limit on the number of concurrent queries (already available) should result in a much better multi-user story.

There are two main limits in both Q and SG components. They are conceptually very similar.

* Querier
    * (x) Query Pipe Limit: when querying we can contact multiple StoreAPIs, limit the size of bytes we receive and keep from each source.
    * (y) Query Map Size: limit the number of bytes that Q can merge from all sources (StoreAPI), cancel requests if the merged result becomes too big
* Store Gateway:
    * (x) Query Pipe Limit: when querying we can read from multiple blocks, limit the size of bytes we receive and keep from each block.
    * (y) Query Map Size: limit the number of bytes that SG can merge from all sources (blocks), cancel requests if the merged result becomes too big


## Main challenges

* [1] Many allocations happen in "shared" paths, e.g. Chunk Pool in SG, label/symbol interning, block indexing. 
* [2] Go is GC language, accounting for memory is hard.
* [3] We cannot quite easily plug ourselves into lower-level code paths which allocate memory, e.g. protobufs unmarshalling.
* [4] Don't want to introduce a performance hit with this feature.
* [5] Verification: how good are our calculations? 

The [1] will need to be addressed a separate piece of work, if at all. There is already an effort to improve index for example.

The approach to [2], [3], and [4] is *to approximate* and not aim for 100% accuracy. As long as the calculated memory use is in ballpark of the actual and tracks it well, this should do as a limit. The main challenge is therefore find the place in code where to place calculations and then figure out what exactly we need to calculate and how. 

The approach to [5] is instrumentation with pprof profile heap snapshots at the right spots and comparing these figures with our calculations.


## Pseudo code

For calculations of sizes see separate section further.

**Limiter interface:**

```golang
type Limiter interface {
    Add(n int64) error
}
```


**Querier:**

```golang
// Querier: pkg/store/proxy.go
func (s *ProxyStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
    // Let each query source add their mem usage to this total limiter
    queryTotalLimiter := limit.NewLimiterAtomic(limit.QueryTotalLimit())

    for _, st := range s.stores() {
        seriesSet := startStreamSeriesSet(..., queryTotalLimiter)
    }
}

// Query of each store: startStreamSeriesSet
func startStreamSeriesSet(..., queryTotalLimiter) {
    // Have local limiter for this source
    localLimiter limit.Limiter = limit.NewLimiterNoLock(limit.QueryPipeLimit())

    // Receive frames
    for {
        r, err := s.stream.Recv()

        // get the size of the received frame
        frameSize := getApproxSize(r)

        // check with local limiter first
        if err := localLimiter.Add(frameSize); err != nil {
            return err
        }

        // now add to total
        if err := queryTotalLimiter.Add(frameSize); err != nil {
            return err
        }
    }
}
```

**Store Gateway:**

```golang
// pkg/store/bucket.go
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
    // Let each query source add their mem usage to this total limiter
    queryTotalLimiter := limit.NewLimiterAtomic(limit.QueryTotalLimit())

    for _, bs := range s.blockSets {
        for _, b := range blocks {
            part, pstats, err := blockSeries(..., queryTotalLimiter) {
            }
        }
    }
}

func blockSeries(..., queryTotalLimiter) {
    // Main memory users are lebels and aggrChunk. But not all. Only showing main parts

    // Have local limiter here.
    localLimiter limit.Limiter = limit.NewLimiterNoLock(limit.QueryPipeLimit())
    
    ps, err := indexr.ExpandedPostings(matchers)
    for _, id := range ps {
        frameSize := getFrameSize()

        // check with local limiter first
        if err := localLimiter.Add(frameSize); err != nil {
            return err
        }

        // now add to total
        if err := queryTotalLimiter.Add(frameSize); err != nil {
            return err
        }
    }
}
```

## Verification

Verification was done by instrumenting code and performing pprof heap dumps and comparing pprof numbers with our calculated numbers. These show as very close.

Instrumentation is performed using custom code, which is now extracted into a separate package:  https://github.com/ppanyukov/go-dump  (note, it’s work in progress still).

Example of instrumented figures:

**// Store Gateway - Pipe Limit**

```
WRITTEN HEAP DUMP TO /Users/philip/thanos/github.com/ppanyukov/thanos-oom/heap-blockSeries-13-before.pb.gz
MEM PROF DIFF:    	blockSeries 	blockSeries - AFTER 	-> Delta
    AllocObjects: 	14.04K 		14.88K 			-> 863
    AllocBytes  : 	3.86G 		4.27G 			-> 424.12M
    InUseObjects: 	3.36K 		4.46K 			-> 1.09K
    InUseBytes  : 	792.25M 	1.03G 			-> 265.61M

LOCAL LIMITER: limit=99.95GB, current=258.01MB
WRITTEN HEAP DUMP TO /Users/philip/thanos/github.com/ppanyukov/thanos-oom/heap-blockSeries-13-after.pb.gz
```

See that pprof reports `265.61M` and our calculations report `258.01MB`. 

Running the pprof tool itself to compare the “official” snapshots, shows: `Showing nodes accounting for 220.65MB, 71.34% of 309.30MB total`, which is very close to our own reported figures.


**// Store Gateway - Total Limit**

```
MEM PROF DIFF:    	Series 		Series - AFTER 	-> Delta
    AllocObjects: 	12.36K 		15.70K 		-> 3.34K
    AllocBytes  : 	3.00G 		4.52G 		-> 1.53G
    InUseObjects: 	1.03K 		5.00K 		-> 3.97K
    InUseBytes  : 	237.00M 	1.17G 		-> 962.28M

TOTAL LIMITER: limit=99.95GB, current=919.95MB
```

Here pprof reports total merged result as `962.28M` and our calculations show `919.95MB`. Again, very close.

**// Querier**

```
MEM PROF DIFF:    	Query 	Query - AFTER 	-> Delta
    AllocObjects: 	93.59K 	116.51K 	-> 22.92K
    AllocBytes  : 	5.64G 	7.72G 		-> 2.08G
    InUseObjects: 	7 	23.11K 		-> 23.10K
    InUseBytes  : 	3.96M 	916.06M 	-> 912.10M

TOTAL LIMITER: limit=1.55GB, current=912.32MB
```

Similarly, pprof is `912.10M` and our calculation is `912.32MB` which is very close.

Again, comparing official pprof snapshots, shows use this: `Showing nodes accounting for 800.25MB, 71.97% of 1111.96MB total`. 


## Calculations.

The calculations were done by approximations.

The approach to this is using sizes of structs and objects that make up the query results. Code is like this:

**Querier: receiving frames**

```golang
// Used in startStreamSeriesSet(), pkg/store/proxy.go, 
getApproxSize := func(r *storepb.SeriesResponse) int64 {
    size := int64(0)
    size += int64(unsafe.Sizeof(storepb.SeriesResponse{}))
    size += int64(len(r.XXX_unrecognized))
    size += int64(len(r.GetWarning()))

    s := r.GetSeries()
    size += int64(unsafe.Sizeof(storepb.Series{}))
    size += int64(len(s.XXX_unrecognized))
    size += int64(len(s.Chunks)) * int64(unsafe.Sizeof(storepb.AggrChunk{}))
    size += int64(len(s.Labels)) * int64(unsafe.Sizeof(storepb.Label{}))

    // approximate the length of each label being about 20 chars, e.g. "k8s_app_metric0"
    const approxLabelLen = int64(10)
    size += approxLabelLen * int64(len(s.Labels))

    // approximate the size if chunks by having 120 bytes in each?
    const approxChunkLen = int64(120)
    size += approxChunkLen * int64(len(s.Chunks))

    return size
}
```


**Store Gateway, more elaborate but similar:**

```golang
// Used in blockSeries(), pkg/store/bucket.go 
labelSize := func(label *storepb.Label) int64 {
    size := int64(0)
    size += int64(unsafe.Sizeof(storepb.Label{}))
    size += int64(len(label.Name))
    size += int64(len(label.Value))
    return size
}

aggrChunkSize := func(aggrChunk *storepb.AggrChunk) int64 {

    chunkSize := func(chunk *storepb.Chunk) int64 {
        if chunk == nil {
            return 0
        }
        size := int64(0)
        size += int64(unsafe.Sizeof(storepb.Chunk{}))
        size += int64(len(chunk.Data))
        return size
    }

    size := int64(0)
    size += int64(unsafe.Sizeof(storepb.AggrChunk{}))
    size += chunkSize(aggrChunk.Raw)
    size += chunkSize(aggrChunk.Count)
    size += chunkSize(aggrChunk.Sum)
    size += chunkSize(aggrChunk.Min)
    size += chunkSize(aggrChunk.Max)
    size += chunkSize(aggrChunk.Counter)
    return size
}
```

In addition to the above, there are many small additions to account for various array sizes. These are not shown for brevity. The `labelSize` and `aggrChunkSize` are the main things.


## Conclusion

Overall the calculated figures show very close to "real" figures reported by pprof. The calculations themselves should be fast enough with minimal performance impact. The exact implementation of limiter interface can have several variations but broadly this will be the kind of idea.

In summary, this feature should help our customers improve availability of Thanos service.


==END==



