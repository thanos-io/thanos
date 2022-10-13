// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

// TestQuerySelect benchmarks querier Select method. Note that this is what PromQL is using, but PromQL might invoke
// this many times and within different interval e.g
// TODO(bwplotka): Add benchmarks with PromQL involvement.
func TestQuerySelect(t *testing.T) {
	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchQuerySelect(t, samplesPerSeries, series, true)
	})
}

func BenchmarkQuerySelect(b *testing.B) {
	tb := testutil.NewTB(b)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchQuerySelect(t, samplesPerSeries, series, true)
	})
}

func benchQuerySelect(t testutil.TB, totalSamples, totalSeries int, dedup bool) {
	tmpDir := t.TempDir()

	const numOfReplicas = 2

	samplesPerSeriesPerReplica := totalSamples / numOfReplicas
	if samplesPerSeriesPerReplica == 0 {
		samplesPerSeriesPerReplica = 1
	}
	seriesPerReplica := totalSeries / numOfReplicas
	if seriesPerReplica == 0 {
		seriesPerReplica = 1
	}

	random := rand.New(rand.NewSource(120))
	var stores []storepb.StoreServer
	var expectedSeries []labels.Labels
	for j := 0; j < numOfReplicas; j++ {
		resps := make([]*storepb.SeriesResponse, 0)
		// Note 0 argument - this is because we want to have two replicas for the same time duration.
		head, created := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", j)),
			SamplesPerSeries: samplesPerSeriesPerReplica,
			Series:           seriesPerReplica,
			Random:           random,
			AppendLabels:     labels.FromStrings("z_replica", fmt.Sprintf("%d", j)), // z_ prefix to keep replica label at the end.
		})
		testutil.Ok(t, head.Close())
		for i := 0; i < len(created); i++ {
			if !dedup || j == 0 {
				lset := labelpb.ZLabelsToPromLabels(created[i].Labels).Copy()
				if dedup {
					lset = lset[:len(lset)-1]
				}
				expectedSeries = append(expectedSeries, lset)
			}

			resps = append(resps, storepb.NewSeriesResponse(created[i]))
		}
		stores = append(stores, &mockedStoreServer{responses: resps})
	}

	logger := log.NewNopLogger()
	q := &querier{
		ctx:                 context.Background(),
		logger:              logger,
		proxy:               newProxyForStore(true, stores...),
		replicaLabels:       []string{"z_replica"},
		replicaLabelSet:     map[string]struct{}{"z_replica": {}},
		deduplicate:         dedup,
		selectGate:          gate.NewNoop(),
		selectTimeout:       1 * time.Minute,
		seriesStatsReporter: NoopSeriesStatsReporter,
	}
	matchers := []*labels.Matcher{
		{
			Type:  labels.MatchRegexp,
			Name:  "z_replica",
			Value: ".+",
		},
	}
	testSelect(t, q, expectedSeries, matchers)
}

type mockedStoreServer struct {
	storepb.StoreServer

	responses []*storepb.SeriesResponse
}

func (m *mockedStoreServer) Series(_ *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	for _, r := range m.responses {
		if err := server.Send(r); err != nil {
			return err
		}
	}
	return nil
}

var (
	testT    int64
	testV    float64
	testLset labels.Labels
)

func testSelect(t testutil.TB, q *querier, expectedSeries []labels.Labels, matchers []*labels.Matcher) {
	t.Run("select", func(t testutil.TB) {
		t.ResetTimer()

		for i := 0; i < t.N(); i++ {
			ss := q.Select(true, nil, matchers...) // Select all.
			testutil.Equals(t, 0, len(ss.Warnings()))

			if t.IsBenchmark() {
				var gotSeriesCount int
				for ss.Next() {
					s := ss.At()
					testLset = s.Labels()
					gotSeriesCount++

					// This is when resource usage should actually start growing.
					iter := s.Iterator()
					for iter.Next() {
						testT, testV = iter.At()
					}
					testutil.Ok(t, iter.Err())
				}

				testutil.Equals(t, len(expectedSeries), gotSeriesCount)
			} else {
				// Check more carefully.
				var gotSeries []labels.Labels
				for ss.Next() {
					s := ss.At()
					gotSeries = append(gotSeries, s.Labels())

					// This is when resource usage should actually start growing.
					iter := s.Iterator()
					for iter.Next() {
						testT, testV = iter.At()
					}
					testutil.Ok(t, iter.Err())
				}

				testutil.Equals(t, expectedSeries, gotSeries)
			}
			testutil.Ok(t, ss.Err())
		}
	})
}
