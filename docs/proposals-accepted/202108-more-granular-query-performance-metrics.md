---
type: proposal
title: More granular query performance metrics
status: in-progress
owner: moadz
menu: proposals-done
---

* **Owners:**
  * @moadz

* **Related Tickets:**
  * [More granular query performance metrics issue by moadz](https://github.com/thanos-io/thanos/issues/1706)
  * [Accurately Measuring Query Performance by ianbillet](https://github.com/thanos-io/thanos/discussions/4674)

## Why
Currently, within thanos, the only way to measure query latency is to look at the `http_request_duration_seconds` on the `query` and `query_range` HTTP handlers.

### Pitfalls of the current solution
There's a few reason why measuring the latency on the query and query_range endpoints is a poor indicator of how much 'work' Thanos is doing. Thanos will fan-out the 'select' phase of the query to each of its store targets (other stores, sidecars etc.) streaming the series from each target over the network, before executing the query on the resulting stream. This means that we actually touch far more series over the network than is returned to the final query response. 

**An ideal metric for query performance must include the total number of samples and series retrieved remotely before the query is executed.**

## Goals
* **Must** instrument the aggregate number of samples returned during the select phase of a fanned out query with respect to time to better understand how query latency scales with respect to samples touched
* **Must** instrument the aggregate number of series returned during the select phase of fanned out query with respect to time to better understand how query latency scales with respect to number of series touched
* **Could** instrument the aggregate response size (bytes) of fanned out requests 
  * This is less critical as response size can be approximated from number of series/number of samples


### Audience 
* Thanos' developers and maintainers who would like to better understand the series/sample funnel before a request is processed more systematically
* Thanos' users who would like to better understand and enforce SLO's around query performance with respect to number of series/samples a query invariably touches

## Non-Goals
* Any performance related optimizations 
* Any approach that involves sampling of queries (traces already sample the aforementioned data)

## How

Mercifully, we already capture these stats in [tracing spans from the query path](https://github.com/thanos-io/thanos/blob/de0e3848ff6085acf89a5f77e053c555a2cce550/pkg/store/proxy.go#L393-L397). Unfortunately, traces are sampled and are difficult to query reliably for SLI's. My suggestion is that we add a new histogram, `thanos_store_query_duration_seconds` that persists the total elapsed time for the query, with partitions(labels) for `series_le` and `samples_le` attached to each observation to allow for querying for a particular dimension (e.g. `thanos_query_duration_seconds` for the `0.99` request duration quantile with `> 1,000,000 samples` returned).  Ideally we would represent this with an N-dimensional histogram, but this is not possible with prometheus.

As the [`storepb.SeriesStatsCounter`](https://github.com/thanos-io/thanos/blob/de0e3848ff6085acf89a5f77e053c555a2cce550/pkg/store/storepb/custom.go#L470) already trackes this information in `thanos/store/proxy.go`, we will have to write an aggregator in the same file that can sum the series/samples for each 'query'. As one thanos query is translated into multiple, fan out queries and aggregated into a single response, we will need to do this once for all the queries [here]().   

### Given most queries will have highly variable samples/series returned for each query, how do we manage cardinality?
Yes, in fact if we used the unique samples/series I suspect this metric would be totally useless. Instead we should define a set of 5-10 distinct 't-shirt size' buckets for the samples/series dimensions.


e.g. Exponential Scale
```
Samples: 
s <= 1000 samples, m <=  10_000 samples, l <= 100_000 samples, xl > 1_000_000 samples 

Series: 
s <= 10 series, m <= 100 series, m <= 1000 series, l <= 10_000 series, xl <= 100_000 series
```

So if a query returns `50,000 samples` spanning `10 series` in `32s` it would be an observation of `32s` with the labels `sample-size=l, series-size=s`.

This would allow us to define query SLI's with respect to a specific number of series/samples and mean response time

e.g. 90% of queries for up to 1,000,000 samples and up to 10 series complete in less than 2s.

### How will we determine appropriate buckets for our additional dimensions?
This is my primary concern. Given the countless permutations of node size, data distribution and thanos topologies that might influence the query sizes and response times, it is unlikely any set thresholds will be appropriate for all deployments of Thanos. As a result, the 't-shirt sizes' will have to be configurable (possibly via runtime args) with some sensible defaults to allow users to tweak them. The obvious caveat being that if this metric is recorded, any changes to the bucketing would render previous data corrupted/incomprehensible.  I would like guidance here if possible. 

Suggested flags for configuring query performance histogram quantiles 
```
// Buckets for labelling query performance metrics with respect to duration of query
--query.telemetry.request-duration-seconds-quantiles = [ 0.1, 0.25, 0.75, 1.25, 1.75, 2.5, 3, 5, 10 ]

// Buckets for labelling query performance metrics with respect to number of samples
--query.telemetry.sample-quantiles = [ 100, 1_000, 10_000, 100_000, 1_000_000 ]

// Buckets for labelling query performance metrics with respect to number of series
--query.telemetry.series-quantiles = [ 10, 100, 1000, 10_000, 100_000 ]
```

### How will we aggregate all series stats between unique series sets
With our new metric we: 
* Do not want to create separate histograms for each individual store query, so they will need to be aggregated at the `Series` request level so that our observations include all 
* Do not want to block series receive on a `seriesStats` merging mutex for each incoming response, so maintaining a central `seriesStats` reference and passing it into each of the proxied store requests is out of the question

First we would create a new `SeriesQueryPerformanceCalculator` for aggregating/tracking the `SeriesStatsCounters` for each fanned out query

go pseudo: 
```go
type SeriesQueryPerformanceMetricsAggregator struct {
	QueryDuration *prometheus.HistogramVec

	SeriesLeBuckets  []float64
	SamplesLeBuckets []float64
	SeriesStats      storepb.SeriesStatsCounter
}

func NewSeriesQueryPerformanceMetricsAggregator(reg prometheus.Registerer) *SeriesQueryPerformanceMetrics {
	return &SeriesQueryPerformanceMetrics{
		QueryDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_store_query_duration_seconds",
			Help:    "duration of the thanos store select phase for a query",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 3, 5}, // These quantiles will be passed via commandline arg 
		}, []string{"series_le", "samples_le"}),
		SeriesStats: storepb.SeriesStatsCounter{},
	}
}

// Aggregator for merging `storepb.SeriesStatsCounter` for each incoming fanned out query
func (s *SeriesQueryPerformanceMetricsAggregator) Aggregate(seriesStats *storepb.SeriesStatsCounter) {
	s.SeriesStats.Count(seriesStats)
}

// Commit the aggregated SeriesStatsCounter as an observation 
func (s *SeriesQueryPerformanceMetricsAggregator) Observe(duration float64) {
	// Bucket matching for series/labels matchSeriesBucket/matchSamplesBucket => float64, float64
	seriesLeBucket := s.findBucket(s.SeriesStats.Series, &s.SeriesLeBuckets)
	samplesLeBucket := s.findBucket(s.SeriesStats.Samples, &s.SamplesLeBuckets)
	s.QueryDuration.With(prometheus.Labels{
		"series_le": strconv.Itoa(seriesLeBucket),
		"samples_le": strconv.Itoa(samplesLeBucket),
	}).Observe(duration)
}

// Determine the appropriate bucket for a given value relative to a set of quantiles
func (s *SeriesQueryPerformanceMetricsAggregator) findBucket(value int, quantiles *[]float64) int 
```

Current query fanout logic: 
```go
for _, st := range s.stores() {
	        // [Error handling etc.] 
			// Schedule streamSeriesSet that translates gRPC streamed response
			// into seriesSet (if series) or respCh if warnings.
			seriesSet = append(seriesSet, startStreamSeriesSet(seriesCtx, reqLogger, span, closeSeries,
				wg, sc, respSender, st.String(), !r.PartialResponseDisabled, s.responseTimeout, s.metrics.emptyStreamResponses, seriesStatsCh))
		}
```

[Propagating the `SeriesStats` via channel](https://github.com/thanos-io/thanos/blob/de0e3848ff6085acf89a5f77e053c555a2cce550/pkg/store/proxy.go#L362):
```go
func startStreamSeriesSet(
  ctx context.Context,
  logger log.Logger,
  span tracing.Span,
  closeSeries context.CancelFunc,
  wg *sync.WaitGroup,
  stream storepb.Store_SeriesClient,
  warnCh directSender,
  name string,
  partialResponse bool,
  responseTimeout time.Duration,
  emptyStreamResponses prometheus.Counter,
  seriesStatsCh: chan<- *storepb.SeriesStatsCounter // Passed via arg
) *streamSeriesSet {
	s := &streamSeriesSet{
		ctx:             ctx,
		logger:          logger,
		closeSeries:     closeSeries,
		stream:          stream,
		warnCh:          warnCh,
		recvCh:          make(chan *storepb.Series, 10),
		name:            name,
		partialResponse: partialResponse,
		responseTimeout: responseTimeout,
		seriesStatsCh:   seriesStatsCh, // Reference maintained in the seriesSet
	}
  // Process stream
}
```

The `seriesStats` sent to the channel on response [here](https://github.com/thanos-io/thanos/blob/de0e3848ff6085acf89a5f77e053c555a2cce550/pkg/store/proxy.go#L454-L463):
```go
if series := rr.r.GetSeries(); series != nil {
				seriesStats.Count(series)
				seriesStatsCh<- seriesStats // Propogate the stats for this response 

				select {
				case s.recvCh <- series:
				case <-ctx.Done():
					s.handleErr(errors.Wrapf(ctx.Err(), "failed to receive any data from %s", s.name), done)
					return false
				}
			}
```

We would then process and aggregate the series stats up the call stack in the [`Series`](https://github.com/thanos-io/thanos/blob/de0e3848ff6085acf89a5f77e053c555a2cce550/pkg/store/proxy.go#L191) handler.

### What time shall we observe? 
We can either:
* [Preferred] Measure Select Phase duration: start a timer within the `QueryPerformanceHistogramCalculator` when it is initialised in the `thanos/store/proxy.go` `Series` handler   
* Measure entire query latency including processing: Initialise the time further up the callstack inside 

tl;dr: We add a `seriesStats` channel that receives `storepb.SeriesStatsCounter`'s form each response, aggregating it up the stack in the `thanos/store/proxy.go` `Series` handler. We add a new `SeriesQueryPerformanceMetricsAggregator` that is responsible for aggregating `seriesStats`. The timer will start when the `Series` function is first hit.  After the response channel is exhausted, the calculator will take an observation according to the `--query.telemetry` quantile flags to the `thanos_store_query_duration_seconds` histogram.

**Caveat**
 * This doesn't capture the entire query performance, only the thanos/store select phase query performance. To measure the entire query path latency we will need to propagate the stats further and aggregate them in the HTTP handlers for the `query` and `query_range`

# Alternatives
* Add the `seriesStats` to the `SeriesResponse` and aggregate it in the `responseCh`, this would enable us to propagate the seriesStats further up/in other components if need be