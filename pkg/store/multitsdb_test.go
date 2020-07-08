// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMultiTSDBSeries(t *testing.T) {
	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		if ok := t.Run("headOnly", func(t testutil.TB) {
			benchMultiTSDBSeries(t, samplesPerSeries, series, false)
		}); !ok {
			return
		}
		t.Run("blocksOnly", func(t testutil.TB) {
			benchMultiTSDBSeries(t, samplesPerSeries, series, true)
		})
	})
}

func BenchmarkMultiTSDBSeries(b *testing.B) {
	tb := testutil.NewTB(b)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		if ok := t.Run("headOnly", func(t testutil.TB) {
			benchMultiTSDBSeries(t, samplesPerSeries, series, false)
		}); !ok {
			return
		}
		t.Run("blocksOnly", func(t testutil.TB) {
			benchMultiTSDBSeries(t, samplesPerSeries, series, true)
		})
	})
}

type mockedStartTimeDB struct {
	*tsdb.DBReadOnly
	startTime int64
}

func (db *mockedStartTimeDB) StartTime() (int64, error) { return db.startTime, nil }

func benchMultiTSDBSeries(t testutil.TB, totalSamples, totalSeries int, flushToBlocks bool) {
	tmpDir, err := ioutil.TempDir("", "testorbench-multitsdbseries")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	const numOfTSDBs = 4

	samplesPerSeriesPerTSDB := totalSamples / numOfTSDBs
	if samplesPerSeriesPerTSDB == 0 {
		samplesPerSeriesPerTSDB = 1
	}
	seriesPerTSDB := totalSeries / numOfTSDBs
	if seriesPerTSDB == 0 {
		seriesPerTSDB = 1
	}

	var (
		dbs    = make([]*mockedStartTimeDB, numOfTSDBs)
		resps  = make([][]*storepb.SeriesResponse, 4)
		random = rand.New(rand.NewSource(120))
		logger = log.NewNopLogger()
	)

	defer func() {
		for _, db := range dbs {
			if db != nil {
				testutil.Ok(t, db.Close())
			}
		}
	}()
	for j := range dbs {
		head, created := storetestutil.CreateHeadWithSeries(t, j, storetestutil.HeadGenOptions{
			Dir:              tmpDir,
			SamplesPerSeries: samplesPerSeriesPerTSDB,
			Series:           seriesPerTSDB,
			WithWAL:          true,
			Random:           random,
			SkipChunks:       t.IsBenchmark(),
		})
		testutil.Ok(t, head.Close())

		tsdbDir := filepath.Join(tmpDir, fmt.Sprintf("%d", j))

		for i := 0; i < len(created); i++ {
			resps[j] = append(resps[j], storepb.NewSeriesResponse(&created[i]))
		}

		if flushToBlocks {
			db, err := tsdb.OpenDBReadOnly(tsdbDir, logger)
			testutil.Ok(t, err)

			testutil.Ok(t, db.FlushWAL(tmpDir))
			testutil.Ok(t, db.Close())
		}

		db, err := tsdb.OpenDBReadOnly(tsdbDir, logger)
		testutil.Ok(t, err)

		dbs[j] = &mockedStartTimeDB{DBReadOnly: db, startTime: int64(j * samplesPerSeriesPerTSDB * seriesPerTSDB)}
	}

	tsdbs := map[string]*TSDBStore{}
	for i, db := range dbs {
		tsdbs[fmt.Sprintf("%v", i)] = &TSDBStore{db: db, logger: logger}
	}

	store := NewMultiTSDBStore(logger, nil, component.Receive, func() map[string]*TSDBStore { return tsdbs })

	var expected []storepb.Series
	lastLabels := storepb.Series{}
	for _, resp := range resps {
		for _, r := range resp {
			// MultiTSDB same as Proxy will merge all series with same labels without limit (https://github.com/thanos-io/thanos/issues/2332).
			// Let's do this here as well.
			x := storepb.Series{Labels: r.GetSeries().Labels}
			if x.String() == lastLabels.String() {
				expected[len(expected)-1].Chunks = append(expected[len(expected)-1].Chunks, r.GetSeries().Chunks...)
				continue
			}
			lastLabels = x
			expected = append(expected, *r.GetSeries())
		}
	}

	storetestutil.TestServerSeries(t, store,
		&storetestutil.SeriesCase{
			Name: fmt.Sprintf("%d TSDBs with %d samples, %d series each", numOfTSDBs, samplesPerSeriesPerTSDB, seriesPerTSDB),
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: math.MaxInt64,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			ExpectedSeries: expected,
		},
	)
}
