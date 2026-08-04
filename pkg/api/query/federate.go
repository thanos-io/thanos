// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is a modified copy from
// github.com/prometheus/prometheus/web/federate.go@v3.13.2.

package v1

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/gzhttp"
	"github.com/opentracing/opentracing-go"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/api"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/server/http/middleware"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// RegisterFederate registers the /federate endpoint on the given router. It is
// separate from Register because federation serves raw exposition text instead
// of the JSON API response format.
func (qapi *QueryAPI) RegisterFederate(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !qapi.disableCORS {
			api.SetCORS(w)
		}
		qapi.federate(w, r)
	})

	r.Get("/federate", middleware.RequestID(
		tracing.HTTPMiddleware(tracer, "federate", logger,
			ins.NewHandler("federate",
				gzhttp.GzipHandler(
					logMiddleware.HTTPMiddleware("federate", hf),
				),
			),
		),
	))
}

func (qapi *QueryAPI) federate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("error parsing form values: %v", err), http.StatusBadRequest)
		return
	}

	matcherSets, ctx, err := tenancy.RewriteLabelMatchers(
		r.Context(),
		r,
		qapi.tenantHeader,
		qapi.defaultTenant,
		qapi.tenantCertField,
		qapi.enforceTenancy,
		qapi.tenantLabel,
		r.Form[MatcherParam],
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := qapi.gate.Start(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer qapi.gate.Done()

	var (
		lookbackDelta = qapi.lookbackDeltaCreate(0)
		mint          = timestamp.FromTime(qapi.baseAPI.Now().Add(-lookbackDelta))
		maxt          = timestamp.FromTime(qapi.baseAPI.Now())
		format        = expfmt.Negotiate(r.Header)
		enc           = expfmt.NewEncoder(w, format)
	)
	w.Header().Set("Content-Type", string(format))

	q, err := qapi.queryableCreate(
		true,
		qapi.replicaLabels,
		nil,
		0,
		qapi.enableQueryPartialResponse,
		false,
		nil,
		query.NoopSeriesStatsReporter,
	).Querier(mint, maxt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "federate querier")

	// vec collects all matched samples before they are sorted by metric name
	// and encoded. 8000 is the initial capacity used by upstream Prometheus,
	// it avoids slice growth for typical federation response sizes.
	vec := make(promql.Vector, 0, 8000)

	hints := &storage.SelectHints{
		Start: mint,
		End:   maxt,
		Limit: toHintLimit(qapi.federateMaxSeries),
	}

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		sets = append(sets, q.Select(ctx, false, hints, mset...))
	}

	set := storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
	it := storage.NewBuffer(int64(lookbackDelta / 1e6))
	var chkIter chunkenc.Iterator
	matchedSeries := 0
Loop:
	for set.Next() {
		matchedSeries++
		if qapi.federateMaxSeries > 0 && matchedSeries > qapi.federateMaxSeries {
			http.Error(w, fmt.Sprintf("federation matched more series than the configured limit of %d", qapi.federateMaxSeries), http.StatusInternalServerError)
			return
		}
		s := set.At()

		chkIter = s.Iterator(chkIter)
		it.Reset(chkIter)

		var (
			t  int64
			f  float64
			fh *histogram.FloatHistogram
		)
		valueType := it.Seek(maxt)
		switch valueType {
		case chunkenc.ValFloat:
			t, f = it.At()
		case chunkenc.ValFloatHistogram, chunkenc.ValHistogram:
			t, fh = it.AtFloatHistogram(nil)
		default:
			sample, ok := it.PeekBack(1)
			if !ok {
				continue Loop
			}
			t = sample.T()
			switch sample.Type() {
			case chunkenc.ValFloat:
				f = sample.F()
			case chunkenc.ValHistogram:
				fh = sample.H().ToFloat(nil)
			case chunkenc.ValFloatHistogram:
				fh = sample.FH()
			default:
				continue Loop
			}
		}
		// The exposition formats do not support stale markers, so drop them. This
		// is good enough for staleness handling of federated data, as the
		// interval-based limits on staleness will do the right thing for supported
		// use cases (which is to say federating aggregated time series).
		if value.IsStaleNaN(f) || (fh != nil && value.IsStaleNaN(fh.Sum)) {
			continue
		}

		vec = append(vec, promql.Sample{
			Metric: s.Labels(),
			T:      t,
			F:      f,
			H:      fh,
		})
	}
	// A missing store must fail the scrape instead of returning an incomplete
	// exposition that downstream Prometheus would treat as stale series.
	if ws := set.Warnings(); len(ws) > 0 {
		msgs := make([]string, 0, len(ws))
		for _, err := range ws.AsErrors() {
			msgs = append(msgs, err.Error())
		}
		slices.Sort(msgs)
		http.Error(w, fmt.Sprintf("federation select returned warnings: %s", strings.Join(msgs, "; ")), http.StatusInternalServerError)
		return
	}
	if set.Err() != nil {
		http.Error(w, set.Err().Error(), http.StatusInternalServerError)
		return
	}

	slices.SortFunc(vec, func(a, b promql.Sample) int {
		ni := a.Metric.Get(labels.MetricName)
		nj := b.Metric.Get(labels.MetricName)
		return strings.Compare(ni, nj)
	})

	var (
		lastMetricName                          string
		lastWasHistogram, lastHistogramWasGauge bool
		protMetricFam                           *dto.MetricFamily
	)
	for _, s := range vec {
		isHistogram := s.H != nil
		formatType := format.FormatType()
		if isHistogram &&
			!s.H.UsesCustomBuckets() &&
			formatType != expfmt.TypeProtoDelim &&
			formatType != expfmt.TypeProtoText &&
			formatType != expfmt.TypeProtoCompact {
			// Can't serve a native histogram with a non-protobuf format.
			// (We can serve an NHCB, though, as it is converted to a
			// classic histogram for federation.)
			// TODO(codesome): Serve them when other protocols get the native histogram support.
			continue
		}

		nameSeen := false
		protMetric := &dto.Metric{}

		err := s.Metric.Validate(func(l labels.Label) error {
			if l.Value == "" {
				// No value means unset. Never consider those labels.
				// This is also important to protect against nameless metrics.
				return nil
			}
			if l.Name == labels.MetricName {
				nameSeen = true
				// We already have the name in the current MetricDescriptor,
				// and we ignore nameless metrics.
				if l.Value == lastMetricName &&
					// The sample type matches (float vs histogram).
					lastWasHistogram == isHistogram &&
					// If it was a histogram, the histogram type
					// (counter vs gauge) also matches.
					(!isHistogram ||
						lastHistogramWasGauge == (s.H.CounterResetHint == histogram.GaugeType)) {
					return nil
				}

				// Since we now check for the sample type and
				// type of histogram above, we will end up
				// creating multiple metric families for the
				// same metric name. This would technically be
				// an invalid exposition. But since the consumer
				// of this is Prometheus, and Prometheus can
				// parse it fine, we allow it and bend the rules
				// to make federation possible in those cases.

				// Need to start a new MetricDescriptor. Ship
				// off the old one (if any) before creating the
				// new one.
				if protMetricFam != nil {
					if err := enc.Encode(protMetricFam); err != nil {
						return err
					}
				}
				protMetricFam = &dto.MetricFamily{
					Type: dto.MetricType_UNTYPED.Enum(),
					Name: proto.String(l.Value),
				}
				if isHistogram {
					if s.H.CounterResetHint == histogram.GaugeType {
						protMetricFam.Type = dto.MetricType_GAUGE_HISTOGRAM.Enum()
					} else {
						protMetricFam.Type = dto.MetricType_HISTOGRAM.Enum()
					}
				}
				lastMetricName = l.Value
				return nil
			}
			protMetric.Label = append(protMetric.Label, &dto.LabelPair{
				Name:  proto.String(l.Name),
				Value: proto.String(l.Value),
			})
			return nil
		})
		if err != nil {
			level.Error(qapi.logger).Log("msg", "federation failed", "err", err)
			return
		}
		if !nameSeen {
			level.Warn(qapi.logger).Log("msg", "Ignoring nameless metric during federation", "metric", s.Metric)
			continue
		}

		protMetric.TimestampMs = proto.Int64(s.T)
		if !isHistogram {
			lastHistogramWasGauge = false
			protMetric.Untyped = &dto.Untyped{
				Value: proto.Float64(s.F),
			}
		} else {
			lastHistogramWasGauge = s.H.CounterResetHint == histogram.GaugeType
			if s.H.UsesCustomBuckets() {
				protMetric.Histogram = makeClassicHistogram(s.H)
			} else {
				protMetric.Histogram = makeNativeHistogram(s.H)
			}
		}
		lastWasHistogram = isHistogram
		protMetricFam.Metric = append(protMetricFam.Metric, protMetric)
	}
	// Still have to ship off the last MetricDescriptor, if any.
	if protMetricFam != nil {
		if err := enc.Encode(protMetricFam); err != nil {
			level.Error(qapi.logger).Log("msg", "federation failed", "err", err)
		}
	}
}

// makeNativeHistogram creates a dto.Histogram representing a native histogram.
// Use only for standard exponential schemas.
func makeNativeHistogram(h *histogram.FloatHistogram) *dto.Histogram {
	result := &dto.Histogram{
		SampleCountFloat: proto.Float64(h.Count),
		SampleSum:        proto.Float64(h.Sum),
		Schema:           proto.Int32(h.Schema),
		ZeroThreshold:    proto.Float64(h.ZeroThreshold),
		ZeroCountFloat:   proto.Float64(h.ZeroCount),
		NegativeCount:    h.NegativeBuckets,
		PositiveCount:    h.PositiveBuckets,
	}
	if len(h.PositiveSpans) > 0 {
		result.PositiveSpan = make([]*dto.BucketSpan, len(h.PositiveSpans))
		for i, sp := range h.PositiveSpans {
			result.PositiveSpan[i] = &dto.BucketSpan{
				Offset: proto.Int32(sp.Offset),
				Length: proto.Uint32(sp.Length),
			}
		}
	}
	if len(h.NegativeSpans) > 0 {
		result.NegativeSpan = make([]*dto.BucketSpan, len(h.NegativeSpans))
		for i, sp := range h.NegativeSpans {
			result.NegativeSpan[i] = &dto.BucketSpan{
				Offset: proto.Int32(sp.Offset),
				Length: proto.Uint32(sp.Length),
			}
		}
	}
	return result
}

// makeClassicHistogram creates a dto.Histogram representing a classic
// histogram. Use only for NHCB (schema -53).
func makeClassicHistogram(h *histogram.FloatHistogram) *dto.Histogram {
	result := &dto.Histogram{
		SampleCountFloat: proto.Float64(h.Count),
		SampleSum:        proto.Float64(h.Sum),
	}
	result.Bucket = make([]*dto.Bucket, len(h.CustomValues))
	var (
		cumulativeCount float64
		bucketIter      = h.PositiveBucketIterator()
		bucketAvailable = bucketIter.Next()
	)
	for i, le := range h.CustomValues {
		for bucketAvailable && int(bucketIter.At().Index) < i {
			bucketAvailable = bucketIter.Next()
		}
		if bucketAvailable && int(bucketIter.At().Index) == i {
			cumulativeCount += bucketIter.At().Count
		}
		result.Bucket[i] = &dto.Bucket{
			UpperBound:           proto.Float64(le),
			CumulativeCountFloat: proto.Float64(cumulativeCount),
		}
	}
	// Note that we do not add the +Inf bucket explicitly. In the protobuf
	// exposition format, it is optional. For other exposition formats, the
	// code converting the protobuf created here into the actual exposition
	// payload will add the +Inf bucket.
	return result
}
