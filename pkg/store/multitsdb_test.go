// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMultiTSDBSeries(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

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
		tsdbDir := filepath.Join(tmpDir, fmt.Sprintf("%d", j))

		head, created := storetestutil.CreateHeadWithSeries(t, j, storetestutil.HeadGenOptions{
			TSDBDir:          tsdbDir,
			SamplesPerSeries: samplesPerSeriesPerTSDB,
			Series:           seriesPerTSDB,
			WithWAL:          !flushToBlocks,
			Random:           random,
			SkipChunks:       t.IsBenchmark(),
		})
		for i := 0; i < len(created); i++ {
			resps[j] = append(resps[j], storepb.NewSeriesResponse(created[i]))
		}

		if flushToBlocks {
			_ = createBlockFromHead(t, tsdbDir, head)
		}
		testutil.Ok(t, head.Close())

		db, err := tsdb.OpenDBReadOnly(tsdbDir, logger)
		testutil.Ok(t, err)

		dbs[j] = &mockedStartTimeDB{DBReadOnly: db, startTime: int64(j * samplesPerSeriesPerTSDB * seriesPerTSDB)}
	}

	tsdbs := map[string]storepb.StoreServer{}
	for i, db := range dbs {
		tsdbs[fmt.Sprintf("%v", i)] = &TSDBStore{db: db, logger: logger}
	}

	store := NewMultiTSDBStore(logger, nil, component.Receive, func() map[string]storepb.StoreServer { return tsdbs })

	var expected []*storepb.Series
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
			expected = append(expected, r.GetSeries())
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

// Regression test against https://github.com/thanos-io/thanos/issues/2823.
func TestTenantSeriesSetServert_NotLeakingIfNotExhausted(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	t.Run("exhausted StoreSet", func(t *testing.T) {
		s := newTenantSeriesSetServer(context.Background(), "a", nil)

		resps := []*storepb.SeriesResponse{
			storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{0, 0}, {2, 1}, {3, 2}}),
		}

		m := &mockedStoreServer{responses: resps}

		go func() {
			s.Series(m, &storepb.SeriesRequest{PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT})
		}()

		testutil.Ok(t, s.Err())
		i := 0
		for s.Next() {
			l, c := s.At()

			testutil.Equals(t, labelpb.ZLabelsToPromLabels(resps[i].GetSeries().Labels), l)
			testutil.Equals(t, resps[i].GetSeries().Chunks, c)

			i++
		}
		testutil.Ok(t, s.Err())
		testutil.Equals(t, 3, i)
	})

	t.Run("canceled, not exhausted StoreSet", func(t *testing.T) {
		defer testutil.TolerantVerifyLeak(t)

		ctx, cancel := context.WithCancel(context.Background())
		s := newTenantSeriesSetServer(ctx, "a", nil)

		m := &mockedStoreServer{responses: []*storepb.SeriesResponse{
			storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{0, 0}, {2, 1}, {3, 2}}),
		}}
		go func() {
			s.Series(m, &storepb.SeriesRequest{PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT})
		}()

		testutil.Ok(t, s.Err())
		testutil.Equals(t, true, s.Next())
		cancel()
	})
}

type mockedSeriesServer struct {
	storepb.Store_SeriesServer
	ctx context.Context

	send func(*storepb.SeriesResponse) error
}

func (s *mockedSeriesServer) Send(r *storepb.SeriesResponse) error {
	return s.send(r)
}
func (s *mockedSeriesServer) Context() context.Context { return s.ctx }

// Regression test against https://github.com/thanos-io/thanos/issues/2823.
// This is different leak than in TestTenantSeriesSetServert_NotLeakingIfNotExhausted.
func TestMultiTSDBStore_NotLeakingOnPrematureFinish(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	m := NewMultiTSDBStore(log.NewNopLogger(), nil, component.Receive, func() map[string]storepb.StoreServer {
		return map[string]storepb.StoreServer{
			// Ensure more than 10 (internal respCh channel).
			"a": &mockedStoreServer{responses: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "d"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "e"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "f"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "g"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "h"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "i"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("a", "j"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			}},
			"b": &mockedStoreServer{responses: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labels.FromStrings("b", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "b"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "c"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "d"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "e"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "f"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "g"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "h"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "i"), []sample{{0, 0}, {2, 1}, {3, 2}}),
				storeSeriesResponse(t, labels.FromStrings("b", "j"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			}},
		}
	})

	t.Run("failing send", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// We mimic failing series server, but practically context cancel will do the same.
		testutil.NotOk(t, m.Series(&storepb.SeriesRequest{PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT}, &mockedSeriesServer{
			ctx: ctx,
			send: func(*storepb.SeriesResponse) error {
				cancel()
				return ctx.Err()
			},
		}))
		testutil.NotOk(t, ctx.Err())
	})
}
