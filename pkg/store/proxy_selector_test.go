// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

// A label advertised by one StoreAPI can be an ordinary series label in another.
// Selector restrictions must neither hide those series nor admit tenants selected
// from a different store. Exercise all fanout APIs and both series strategies.
func TestProxyStore_SelectorMatchersPerStore(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			for _, api := range []string{"series-eager", "series-lazy", "label-names", "label-values"} {
				t.Run(fmt.Sprintf("selector=%v/reverse=%v/%s", enabled, reverse, api), func(t *testing.T) {
					type fixture struct {
						name     string
						external []labels.Labels
						accepted labels.Labels
						rejected []labels.Labels
					}
					series := func(tenant, replica string) labels.Labels {
						return labels.FromStrings("__name__", "ALERTS_FOR_STATE", "tenant_id", tenant, "thanos_ruler_replica", replica)
					}
					fixtures := []fixture{
						{
							name:     "receive-a",
							external: []labels.Labels{labels.FromStrings("tenant_id", "allowed-a"), labels.FromStrings("tenant_id", "blocked")},
							accepted: series("allowed-a", "workload-0"),
							rejected: []labels.Labels{series("blocked", "workload-0"), series("allowed-b", "workload-0")},
						},
						{
							name:     "receive-b",
							external: []labels.Labels{labels.FromStrings("tenant_id", "allowed-b"), labels.FromStrings("tenant_id", "blocked")},
							accepted: series("allowed-b", "workload-1"),
							rejected: []labels.Labels{series("blocked", "workload-1"), series("allowed-a", "workload-1")},
						},
						{
							name:     "global-ruler",
							external: []labels.Labels{labels.FromStrings("thanos_ruler_replica", "global-0")},
							accepted: labels.FromStrings("__name__", "ALERTS_FOR_STATE", "thanos_ruler_replica", "global-0"),
						},
						{
							name:     "no-external-labels",
							accepted: series("unadvertised", "workload-2"),
						},
						{
							name:     "blocked-only",
							external: []labels.Labels{labels.FromStrings("tenant_id", "blocked")},
							accepted: series("blocked", "workload-0"),
						},
					}
					if reverse {
						slices.Reverse(fixtures)
					}
					var clients []Client
					var mocks []*mockedStoreAPI
					for _, f := range fixtures {
						m := &mockedStoreAPI{RespLabelNames: &storepb.LabelNamesResponse{}, RespLabelValues: &storepb.LabelValuesResponse{}}
						mocks = append(mocks, m)
						clients = append(clients, &storetestutil.TestClient{StoreClient: m, Name: f.name, ExtLset: f.external, MinTime: 0, MaxTime: 100})
					}
					selector := DefaultSelector
					if enabled {
						config, err := block.ParseRelabelConfig([]byte("- action: drop\n  source_labels: [tenant_id]\n  regex: blocked\n"), block.SelectorSupportedRelabelActions)
						testutil.Ok(t, err)
						selector = NewTSDBSelector(config)
					}
					strategy := EagerRetrieval
					if api == "series-lazy" {
						strategy = LazyRetrieval
					}
					q := NewProxyStore(nil, nil, func() []Client { return clients }, component.Query, labels.EmptyLabels(), time.Second, strategy, WithTSDBSelector(selector))
					// Spare capacity catches accidental appends into the caller's matcher array.
					matchers := make([]storepb.LabelMatcher, 1, 16)
					matchers[0] = storepb.LabelMatcher{Name: "__name__", Value: "ALERTS_FOR_STATE", Type: storepb.LabelMatcher_EQ}
					before := slices.Clone(matchers[:cap(matchers)])
					ctx := context.Background()
					switch api {
					case "series-eager", "series-lazy":
						testutil.Ok(t, q.Series(&storepb.SeriesRequest{MinTime: 0, MaxTime: 100, Matchers: matchers}, newStoreSeriesServer(ctx)))
					case "label-names":
						_, err := q.LabelNames(ctx, &storepb.LabelNamesRequest{Start: 0, End: 100, Matchers: matchers})
						testutil.Ok(t, err)
					case "label-values":
						_, err := q.LabelValues(ctx, &storepb.LabelValuesRequest{Start: 0, End: 100, Label: "thanos_ruler_replica", Matchers: matchers})
						testutil.Ok(t, err)
					}
					testutil.Equals(t, before, matchers[:cap(matchers)])
					for i, f := range fixtures {
						var got []storepb.LabelMatcher
						m := mocks[i]
						switch api {
						case "series-eager", "series-lazy":
							if m.LastSeriesReq != nil {
								got = m.LastSeriesReq.Matchers
							}
						case "label-names":
							if m.LastLabelNamesReq != nil {
								got = m.LastLabelNamesReq.Matchers
							}
						case "label-values":
							if m.LastLabelValuesReq != nil {
								got = m.LastLabelValuesReq.Matchers
							}
						}
						if enabled && f.name == "blocked-only" {
							testutil.Assert(t, got == nil, "blocked store was queried")
							continue
						}
						testutil.Assert(t, len(got) > 0, "store %s was not queried", f.name)
						if !enabled || len(f.external) == 0 {
							testutil.Equals(t, matchers, got)
						}
						promMatchers, err := storepb.MatchersToPromMatchers(got...)
						testutil.Ok(t, err)
						matches := func(ls labels.Labels) bool {
							for _, matcher := range promMatchers {
								if !matcher.Matches(ls.Get(matcher.Name)) {
									return false
								}
							}
							return true
						}
						testutil.Assert(t, matches(f.accepted), "store %s rejects its own series: %v", f.name, got)
						if enabled {
							for _, ls := range f.rejected {
								testutil.Assert(t, !matches(ls), "store %s admits excluded series %v: %v", f.name, ls, got)
							}
						}
					}
				})
			}
		}
	}
}

func TestProxyStore_SelectorWithStaleMetadata(t *testing.T) {
	t.Parallel()

	for _, api := range []string{"series-eager", "series-lazy", "label-names", "label-values"} {
		t.Run(api, func(t *testing.T) {
			seriesLabels := func(tenant string) labels.Labels {
				return labels.FromStrings("__name__", "up", "tenant_id", tenant, tenant+"_label", "present")
			}
			var downstream []Client
			for _, tenant := range []string{"allowed", "blocked"} {
				downstream = append(downstream, &storetestutil.TestClient{
					Name:    tenant,
					MinTime: 0,
					MaxTime: 100,
					ExtLset: []labels.Labels{labels.FromStrings("tenant_id", tenant)},
					StoreClient: &mockedStoreAPI{
						RespSeries:      []*storepb.SeriesResponse{storeSeriesResponse(t, seriesLabels(tenant))},
						RespLabelNames:  &storepb.LabelNamesResponse{Names: []string{"__name__", tenant + "_label", "tenant_id"}},
						RespLabelValues: &storepb.LabelValuesResponse{Values: []string{tenant}},
					},
				})
			}
			inner := NewProxyStore(nil, nil, func() []Client { return downstream }, component.Query, labels.EmptyLabels(), time.Second, EagerRetrieval)
			// The downstream proxy has discovered the blocked TSDB since the parent's
			// last Info refresh. All label sets still cached by the parent are allowed.
			clients := []Client{&storetestutil.TestClient{
				Name:        "downstream",
				MinTime:     0,
				MaxTime:     100,
				ExtLset:     []labels.Labels{labels.FromStrings("tenant_id", "allowed")},
				StoreClient: storepb.ServerAsClient(inner, atomic.Bool{}),
			}}
			config, err := block.ParseRelabelConfig([]byte("- action: drop\n  source_labels: [tenant_id]\n  regex: blocked\n"), block.SelectorSupportedRelabelActions)
			testutil.Ok(t, err)
			strategy := EagerRetrieval
			if api == "series-lazy" {
				strategy = LazyRetrieval
			}
			outer := NewProxyStore(nil, nil, func() []Client { return clients }, component.Query, labels.EmptyLabels(), time.Second, strategy, WithTSDBSelector(NewTSDBSelector(config)))
			matchers := []storepb.LabelMatcher{{Name: "__name__", Type: storepb.LabelMatcher_EQ, Value: "up"}}
			ctx := context.Background()
			switch api {
			case "series-eager", "series-lazy":
				srv := newStoreSeriesServer(ctx)
				testutil.Ok(t, outer.Series(&storepb.SeriesRequest{MinTime: 0, MaxTime: 100, Matchers: matchers, SkipChunks: true}, srv))
				var got []labels.Labels
				for _, s := range srv.SeriesSet {
					got = append(got, s.PromLabels())
				}
				testutil.Equals(t, []labels.Labels{seriesLabels("allowed")}, got)
			case "label-names":
				resp, err := outer.LabelNames(ctx, &storepb.LabelNamesRequest{Start: 0, End: 100, Matchers: matchers})
				testutil.Ok(t, err)
				testutil.Equals(t, []string{"__name__", "allowed_label", "tenant_id"}, resp.Names)
			case "label-values":
				resp, err := outer.LabelValues(ctx, &storepb.LabelValuesRequest{Start: 0, End: 100, Label: "tenant_id", Matchers: matchers})
				testutil.Ok(t, err)
				testutil.Equals(t, []string{"allowed"}, resp.Values)
			}
		})
	}
}

func TestProxyStore_SelectorPrunesOnSelectedLabelSets(t *testing.T) {
	t.Parallel()

	m := &mockedStoreAPI{}
	clients := []Client{&storetestutil.TestClient{
		StoreClient: m,
		Name:        "receive-a",
		ExtLset:     []labels.Labels{labels.FromStrings("tenant_id", "allowed-a"), labels.FromStrings("tenant_id", "blocked")},
		MinTime:     0,
		MaxTime:     100,
	}}
	config, err := block.ParseRelabelConfig([]byte("- action: drop\n  source_labels: [tenant_id]\n  regex: blocked\n"), block.SelectorSupportedRelabelActions)
	testutil.Ok(t, err)
	q := NewProxyStore(nil, nil, func() []Client { return clients }, component.Query, labels.EmptyLabels(), time.Second, EagerRetrieval, WithTSDBSelector(NewTSDBSelector(config)))

	matchers := []storepb.LabelMatcher{{Name: "tenant_id", Value: "blocked", Type: storepb.LabelMatcher_EQ}}
	testutil.Ok(t, q.Series(&storepb.SeriesRequest{MinTime: 0, MaxTime: 100, Matchers: matchers}, newStoreSeriesServer(context.Background())))
	testutil.Assert(t, m.LastSeriesReq == nil, "store was queried for a tenant dropped by the selector")
}
