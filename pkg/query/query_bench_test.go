// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

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
	var resps []*storepb.SeriesResponse
	var expectedSeries []labels.Labels
	for j := 0; j < numOfReplicas; j++ {
		// Note 0 argument - this is because we want to have two replicas for the same time duration.
		head, created := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", j)),
			SamplesPerSeries: samplesPerSeriesPerReplica,
			Series:           seriesPerReplica,
			Random:           random,
			PrependLabels:    labels.FromStrings("a_replica", fmt.Sprintf("%d", j)), // a_ prefix so we keep sorted order.
		})
		testutil.Ok(t, head.Close())
		for i := 0; i < len(created); i++ {
			if !dedup || j == 0 {
				lset := labelpb.ZLabelsToPromLabels(created[i].Labels).Copy()
				if dedup {
					lset = lset[1:]
				}
				expectedSeries = append(expectedSeries, lset)
			}

			resps = append(resps, storepb.NewSeriesResponse(created[i]))
		}

	}

	logger := log.NewNopLogger()
	q := &querier{
		ctx:                 context.Background(),
		logger:              logger,
		proxy:               &mockedStoreServer{responses: resps},
		replicaLabels:       map[string]struct{}{"a_replica": {}},
		deduplicate:         dedup,
		selectGate:          gate.NewNoop(),
		seriesStatsReporter: NoopSeriesStatsReporter,
	}
	testSelect(t, q, expectedSeries)
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

func testSelect(t testutil.TB, q *querier, expectedSeries []labels.Labels) {
	t.Run("select", func(t testutil.TB) {
		t.ResetTimer()

		for i := 0; i < t.N(); i++ {
			ss := q.Select(true, nil) // Select all.
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
