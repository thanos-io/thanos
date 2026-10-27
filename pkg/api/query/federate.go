// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

// defaultFederateLookbackDelta matches the PromQL engine default used when --query.lookback-delta is unset.
const defaultFederateLookbackDelta = 5 * time.Minute

// Federate serves the latest sample of every series selected by the match[] parameters
// in the Prometheus exposition format, the same way the Prometheus /federate endpoint does.
// This lets a Prometheus server scrape the deduplicated global view of Thanos Query.
//
// It is adapted from Prometheus web/federate.go. Native histograms are not exposed yet.
func (qapi *QueryAPI) Federate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, errors.Wrap(err, "parse form").Error(), http.StatusBadRequest)
		return
	}
	if len(r.Form[MatcherParam]) == 0 {
		http.Error(w, "no match[] parameter provided", http.StatusBadRequest)
		return
	}

	matcherSets, ctx, err := tenancy.RewriteLabelMatchers(r.Context(), r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField, qapi.enforceTenancy, qapi.tenantLabel, r.Form[MatcherParam])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		http.Error(w, apiErr.Err.Error(), http.StatusBadRequest)
		return
	}
	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		http.Error(w, apiErr.Err.Error(), http.StatusBadRequest)
		return
	}
	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		http.Error(w, apiErr.Err.Error(), http.StatusBadRequest)
		return
	}
	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		http.Error(w, apiErr.Err.Error(), http.StatusBadRequest)
		return
	}
	lookbackDelta, apiErr := qapi.parseLookbackDeltaParam(r)
	if apiErr != nil {
		http.Error(w, apiErr.Err.Error(), http.StatusBadRequest)
		return
	}
	if lookbackDelta == 0 {
		lookbackDelta = qapi.lookbackDeltaCreate(0)
	}
	if lookbackDelta == 0 {
		lookbackDelta = defaultFederateLookbackDelta
	}

	maxt := qapi.baseAPI.Now().UnixMilli()
	mint := maxt - lookbackDelta.Milliseconds()

	q, err := qapi.queryableCreate(
		enableDedup,
		replicaLabels,
		storeDebugMatchers,
		0,
		enablePartialResponse,
		false,
		nil,
		query.NoopSeriesStatsReporter,
	).Querier(mint, maxt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "queryable federate")

	hints := &storage.SelectHints{Start: mint, End: maxt}
	sets := make([]storage.SeriesSet, 0, len(matcherSets))
	for _, mset := range matcherSets {
		sets = append(sets, q.Select(ctx, true, hints, mset...))
	}
	set := storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)

	type sample struct {
		lset labels.Labels
		t    int64
		v    float64
	}
	var (
		samples []sample
		it      chunkenc.Iterator
	)
	for set.Next() {
		s := set.At()
		it = s.Iterator(it)

		var (
			t     int64
			v     float64
			found bool
		)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			if vt != chunkenc.ValFloat {
				// Native histograms are skipped for now.
				continue
			}
			st, sv := it.At()
			if st > maxt {
				break
			}
			t, v, found = st, sv, true
		}
		if it.Err() != nil {
			http.Error(w, it.Err().Error(), http.StatusInternalServerError)
			return
		}
		if !found || value.IsStaleNaN(v) {
			continue
		}
		samples = append(samples, sample{lset: s.Labels(), t: t, v: v})
	}
	if set.Err() != nil {
		http.Error(w, set.Err().Error(), http.StatusInternalServerError)
		return
	}
	for _, warn := range set.Warnings() {
		level.Warn(qapi.logger).Log("msg", "federation select returned warning", "warn", warn)
	}

	slices.SortFunc(samples, func(a, b sample) int {
		if c := strings.Compare(a.lset.Get(labels.MetricName), b.lset.Get(labels.MetricName)); c != 0 {
			return c
		}
		return labels.Compare(a.lset, b.lset)
	})

	format := expfmt.Negotiate(r.Header)
	w.Header().Set("Content-Type", string(format))
	enc := expfmt.NewEncoder(w, format)

	var (
		lastMetricName string
		protMetricFam  *dto.MetricFamily
	)
	for _, s := range samples {
		name := s.lset.Get(labels.MetricName)
		if protMetricFam == nil || name != lastMetricName {
			if protMetricFam != nil {
				if err := enc.Encode(protMetricFam); err != nil {
					level.Error(qapi.logger).Log("msg", "federation failed", "err", err)
					return
				}
			}
			protMetricFam = &dto.MetricFamily{
				Type: dto.MetricType_UNTYPED.Enum(),
				Name: &name,
			}
			lastMetricName = name
		}

		protMetric := &dto.Metric{
			Untyped:     &dto.Untyped{Value: &s.v},
			TimestampMs: &s.t,
		}
		hasInstance := false
		s.lset.Range(func(l labels.Label) {
			if l.Name == labels.MetricName {
				return
			}
			if l.Name == model.InstanceLabel {
				hasInstance = true
			}
			protMetric.Label = append(protMetric.Label, &dto.LabelPair{
				Name:  &l.Name,
				Value: &l.Value,
			})
		})
		if !hasInstance {
			// Add an empty instance label so that the scraping Prometheus does not
			// attach its own instance label to the federated series.
			protMetric.Label = append(protMetric.Label, &dto.LabelPair{
				Name:  proto(model.InstanceLabel),
				Value: proto(""),
			})
			slices.SortFunc(protMetric.Label, func(a, b *dto.LabelPair) int {
				return strings.Compare(a.GetName(), b.GetName())
			})
		}
		protMetricFam.Metric = append(protMetricFam.Metric, protMetric)
	}
	if protMetricFam != nil {
		if err := enc.Encode(protMetricFam); err != nil {
			level.Error(qapi.logger).Log("msg", "federation failed", "err", err)
		}
	}
}

func proto[T any](v T) *T { return &v }
