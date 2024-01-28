// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/cespare/xxhash"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

const skipMessage = "Chunk behavior changed due to https://github.com/prometheus/prometheus/pull/8723. Skip for now."

func TestTSDBStore_Info(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	resp, err := tsdbStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []labelpb.ZLabel{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_RULE, resp.StoreType)
	testutil.Equals(t, int64(math.MaxInt64), resp.MinTime)
	testutil.Equals(t, int64(math.MaxInt64), resp.MaxTime)

	app := db.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("a", "a"), 12, 0.1)
	testutil.Ok(t, err)
	testutil.Ok(t, app.Commit())

	resp, err = tsdbStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []labelpb.ZLabel{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_RULE, resp.StoreType)
	testutil.Equals(t, int64(12), resp.MinTime)
	testutil.Equals(t, int64(math.MaxInt64), resp.MaxTime)
}

func TestTSDBStore_Series_ChunkChecksum(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	appender := db.Appender(context.Background())

	for i := 1; i <= 3; i++ {
		_, err = appender.Append(0, labels.FromStrings("a", "1"), int64(i), float64(i))
		testutil.Ok(t, err)
	}
	err = appender.Commit()
	testutil.Ok(t, err)

	srv := newStoreSeriesServer(ctx)

	req := &storepb.SeriesRequest{
		MinTime: 1,
		MaxTime: 3,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
		},
	}

	err = tsdbStore.Series(req, srv)
	testutil.Ok(t, err)

	for _, chk := range srv.SeriesSet[0].Chunks {
		want := xxhash.Sum64(chk.Raw.Data)
		testutil.Equals(t, want, chk.Raw.Hash)
	}
}

func TestTSDBStore_Series(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	appender := db.Appender(context.Background())

	for i := 1; i <= 3; i++ {
		_, err = appender.Append(0, labels.FromStrings("a", "1"), int64(i), float64(i))
		testutil.Ok(t, err)
	}
	err = appender.Commit()
	testutil.Ok(t, err)

	for _, tc := range []struct {
		title          string
		req            *storepb.SeriesRequest
		expectedSeries []rawSeries
		expectedError  string
	}{
		{
			title: "total match series",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "1", "region", "eu-west"),
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
		},
		{
			title: "partially match time range series",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 2,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{
				{
					lset:   labels.FromStrings("a", "1", "region", "eu-west"),
					chunks: [][]sample{{{1, 1}, {2, 2}}},
				},
			},
		},
		{
			title: "dont't match time range series",
			req: &storepb.SeriesRequest{
				MinTime: 4,
				MaxTime: 6,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{},
		},
		{
			title: "only match external label",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
				},
			},
			expectedError: "rpc error: code = InvalidArgument desc = no matchers specified (excluding external labels)",
		},
		{
			title: "dont't match labels",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{},
		},
		{
			title: "no chunk",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				SkipChunks: true,
			},
			expectedSeries: []rawSeries{
				{
					lset: labels.FromStrings("a", "1", "region", "eu-west"),
				},
			},
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			srv := newStoreSeriesServer(ctx)
			err := tsdbStore.Series(tc.req, srv)
			if len(tc.expectedError) > 0 {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedError, err.Error())
			} else {
				testutil.Ok(t, err)
				seriesEquals(t, tc.expectedSeries, srv.SeriesSet)
			}
		}); !ok {
			return
		}
	}
}

type delegatorServer struct {
	*storetestutil.SeriesServer

	closers []io.Closer
}

func (s *delegatorServer) Delegate(c io.Closer) {
	s.closers = append(s.closers, c)
}

// Regression test for: https://github.com/thanos-io/thanos/issues/3013 .
func TestTSDBStore_SeriesAccessWithDelegateClosing(t *testing.T) {
	t.Skip(skipMessage)

	tmpDir := t.TempDir()

	var (
		random = rand.New(rand.NewSource(120))
		logger = log.NewNopLogger()
	)

	// Generate one series in two parts. Put first part in block, second in just WAL.
	head, _ := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: 300,
		Series:           2,
		Random:           random,
		SkipChunks:       true,
	})
	_ = createBlockFromHead(t, tmpDir, head)
	testutil.Ok(t, head.Close())

	head, _ = storetestutil.CreateHeadWithSeries(t, 1, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: 300,
		Series:           2,
		WithWAL:          true,
		Random:           random,
		SkipChunks:       true,
	})
	testutil.Ok(t, head.Close())

	db, err := tsdb.OpenDBReadOnly(tmpDir, logger)
	testutil.Ok(t, err)

	dbToClose := make(chan *tsdb.DBReadOnly, 1)
	dbToClose <- db
	t.Cleanup(func() {
		// Close if not closed before.
		select {
		case db := <-dbToClose:
			testutil.Ok(t, db.Close())
		default:
		}
	})

	extLabels := labels.FromStrings("ext", "1")
	store := NewTSDBStore(logger, &mockedStartTimeDB{DBReadOnly: db, startTime: 0}, component.Receive, extLabels)

	srv := storetestutil.NewSeriesServer(context.Background())
	csrv := &delegatorServer{SeriesServer: srv}
	t.Run("call series and access results", func(t *testing.T) {
		testutil.Ok(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: math.MaxInt64,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
			},
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		}, csrv))
		testutil.Equals(t, 0, len(srv.Warnings))
		testutil.Equals(t, 0, len(srv.HintsSet))
		testutil.Equals(t, 4, len(srv.SeriesSet))

		// All chunks should be accessible for read, but not necessarily for write.
		for _, s := range srv.SeriesSet {
			testutil.Equals(t, 3, len(s.Chunks))
			for _, c := range s.Chunks {
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					_ = string(c.Raw.Data) // Access bytes by converting them to different type.
				}))
			}
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				s.Chunks[0].Raw.Data[0] = 0 // Check if we can write to the byte range.
				s.Chunks[1].Raw.Data[0] = 0
				s.Chunks[2].Raw.Data[0] = 0
			}))
		}
	})

	flushDone := make(chan error)
	t.Run("flush WAL and access results", func(t *testing.T) {
		go func() {
			// This should block until all queries are closed.
			flushDone <- db.FlushWAL(tmpDir)
			close(flushDone)
		}()
		// All chunks should be still accessible for read, but not necessarily for write.
		for _, s := range srv.SeriesSet {
			for _, c := range s.Chunks {
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					_ = string(c.Raw.Data) // Access bytes by converting them to different type.
				}))
			}
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				s.Chunks[0].Raw.Data[0] = 0 // Check if we can write to the byte range.
				s.Chunks[1].Raw.Data[0] = 0
				s.Chunks[2].Raw.Data[0] = 0
			}))
		}
	})
	select {
	case err, ok := <-flushDone:
		if !ok {
			t.Fatalf("expected flush to be blocked, but it seems it completed. Result: %v", err)
		}
	default:
	}

	closeDone := make(chan error)
	t.Run("close db with block readers and access results", func(t *testing.T) {
		go func() {
			select {
			case db := <-dbToClose:
				// This should block until all queries are closed.
				closeDone <- db.Close()
			default:
			}
			close(closeDone)
		}()
		// All chunks should be still accessible for read, but not necessarily for write.
		for _, s := range srv.SeriesSet {
			for _, c := range s.Chunks {
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					_ = string(c.Raw.Data) // Access bytes by converting them to different type.
				}))
			}
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				s.Chunks[0].Raw.Data[0] = 0 // Check if we can write to the byte range.
				s.Chunks[1].Raw.Data[0] = 0
				s.Chunks[2].Raw.Data[0] = 0
			}))
		}
	})
	select {
	case _, ok := <-closeDone:
		if !ok {
			t.Fatalf("expected db cloe to be blocked, but it seems it completed. Result: %v", err)

		}
	default:
	}

	t.Run("close querier and access results", func(t *testing.T) {
		// Let's close pending querier!
		testutil.Equals(t, 1, len(csrv.closers))
		testutil.Ok(t, csrv.closers[0].Close())

		// Expect flush and close to be unblocked and without errors.
		testutil.Ok(t, <-flushDone)
		testutil.Ok(t, <-closeDone)

		// Expect segfault on read and write.
		t.Run("non delegatable", func(t *testing.T) {
			for _, s := range srv.SeriesSet {
				testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
					_ = string(s.Chunks[0].Raw.Data) // Access bytes by converting them to different type.
					_ = string(s.Chunks[1].Raw.Data)
					_ = string(s.Chunks[2].Raw.Data)
				}))
				testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
					s.Chunks[0].Raw.Data[0] = 0 // Check if we can write to the byte range.
					s.Chunks[1].Raw.Data[0] = 0
					s.Chunks[2].Raw.Data[0] = 0
				}))
			}
		})
	})
}

func TestTSDBStore_SeriesAccessWithoutDelegateClosing(t *testing.T) {
	t.Skip(skipMessage)

	tmpDir := t.TempDir()

	var (
		random = rand.New(rand.NewSource(120))
		logger = log.NewNopLogger()
	)

	// Generate one series in two parts. Put first part in block, second in just WAL.
	head, _ := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: 300,
		Series:           2,
		Random:           random,
		SkipChunks:       true,
	})
	_ = createBlockFromHead(t, tmpDir, head)
	testutil.Ok(t, head.Close())

	head, _ = storetestutil.CreateHeadWithSeries(t, 1, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: 300,
		Series:           2,
		WithWAL:          true,
		Random:           random,
		SkipChunks:       true,
	})
	testutil.Ok(t, head.Close())

	db, err := tsdb.OpenDBReadOnly(tmpDir, logger)
	testutil.Ok(t, err)
	t.Cleanup(func() {
		if db != nil {
			testutil.Ok(t, db.Close())
		}
	})

	extLabels := labels.FromStrings("ext", "1")
	store := NewTSDBStore(logger, &mockedStartTimeDB{DBReadOnly: db, startTime: 0}, component.Receive, extLabels)

	srv := storetestutil.NewSeriesServer(context.Background())
	t.Run("call series and access results", func(t *testing.T) {
		testutil.Ok(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: math.MaxInt64,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
			},
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		}, srv))
		testutil.Equals(t, 0, len(srv.Warnings))
		testutil.Equals(t, 0, len(srv.HintsSet))
		testutil.Equals(t, 4, len(srv.SeriesSet))

		// All chunks should be accessible for read, but not necessarily for write.
		for _, s := range srv.SeriesSet {
			testutil.Equals(t, 3, len(s.Chunks))
			for _, c := range s.Chunks {
				testutil.Ok(t, testutil.FaultOrPanicToErr(func() {
					_ = string(c.Raw.Data) // Access bytes by converting them to different type.
				}))
			}
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				s.Chunks[0].Raw.Data[0] = 0 // Check if we can write to the byte range.
				s.Chunks[1].Raw.Data[0] = 0
				s.Chunks[2].Raw.Data[0] = 0
			}))
		}
	})

	t.Run("flush WAL and access results", func(t *testing.T) {
		// This should NOT block as close was not delegated.
		testutil.Ok(t, db.FlushWAL(tmpDir))

		// Expect segfault on read and write.
		for _, s := range srv.SeriesSet {
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				_ = string(s.Chunks[0].Raw.Data) // Access bytes by converting them to different type.
				_ = string(s.Chunks[1].Raw.Data)
				_ = string(s.Chunks[2].Raw.Data)
			}))
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				s.Chunks[0].Raw.Data[0] = 0 // Check if we can write to the byte range.
				s.Chunks[1].Raw.Data[0] = 0
				s.Chunks[2].Raw.Data[0] = 0
			}))
		}
	})
	t.Run("close db with block readers and access results", func(t *testing.T) {
		// This should NOT block as close was not delegated.
		testutil.Ok(t, db.Close())
		db = nil

		// Expect segfault on read and write.
		for _, s := range srv.SeriesSet {
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				_ = string(s.Chunks[0].Raw.Data) // Access bytes by converting them to different type.
				_ = string(s.Chunks[1].Raw.Data)
				_ = string(s.Chunks[2].Raw.Data)
			}))
			testutil.NotOk(t, testutil.FaultOrPanicToErr(func() {
				s.Chunks[0].Raw.Data[0] = 0 // Check if we can write to the byte range.
				s.Chunks[1].Raw.Data[0] = 0
				s.Chunks[2].Raw.Data[0] = 0
			}))
		}
	})
}

func TestTSDBStoreSeries(t *testing.T) {
	tb := testutil.NewTB(t)
	// Make sure there are more samples, so we can check framing code.
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchTSDBStoreSeries(t, samplesPerSeries, series)
	})
}

func BenchmarkTSDBStoreSeries(b *testing.B) {
	tb := testutil.NewTB(b)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchTSDBStoreSeries(t, samplesPerSeries, series)
	})
}

func benchTSDBStoreSeries(t testutil.TB, totalSamples, totalSeries int) {
	tmpDir := t.TempDir()

	// This means 3 blocks and the head.
	const numOfBlocks = 4

	samplesPerSeriesPerBlock := totalSamples / numOfBlocks
	if samplesPerSeriesPerBlock == 0 {
		samplesPerSeriesPerBlock = 1
	}
	seriesPerBlock := totalSeries / numOfBlocks
	if seriesPerBlock == 0 {
		seriesPerBlock = 1
	}

	var (
		resps  = make([][]*storepb.SeriesResponse, 4)
		random = rand.New(rand.NewSource(120))
		logger = log.NewNopLogger()
	)

	for j := 0; j < 3; j++ {
		head, created := storetestutil.CreateHeadWithSeries(t, j, storetestutil.HeadGenOptions{
			TSDBDir:          tmpDir,
			SamplesPerSeries: samplesPerSeriesPerBlock,
			Series:           seriesPerBlock,
			Random:           random,
		})
		for i := 0; i < len(created); i++ {
			resps[j] = append(resps[j], storepb.NewSeriesResponse(created[i]))
		}

		_ = createBlockFromHead(t, tmpDir, head)
		testutil.Ok(t, head.Close())
	}

	head2, created := storetestutil.CreateHeadWithSeries(t, 3, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: samplesPerSeriesPerBlock,
		Series:           seriesPerBlock,
		WithWAL:          true,
		Random:           random,
	})
	testutil.Ok(t, head2.Close())

	for i := 0; i < len(created); i++ {
		resps[3] = append(resps[3], storepb.NewSeriesResponse(created[i]))
	}

	db, err := tsdb.OpenDBReadOnly(tmpDir, logger)
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, db.Close()) }()

	extLabels := labels.FromStrings("ext", "1")
	store := NewTSDBStore(logger, &mockedStartTimeDB{DBReadOnly: db, startTime: 0}, component.Receive, extLabels)

	var expected []*storepb.Series
	for _, resp := range resps {
		for _, r := range resp {
			// Add external labels & frame it.
			s := r.GetSeries()
			bytesLeftForChunks := store.maxBytesPerFrame
			lbls := make([]labelpb.ZLabel, 0, len(s.Labels)+extLabels.Len())
			for _, l := range s.Labels {
				lbls = append(lbls, labelpb.ZLabel{
					Name:  l.Name,
					Value: l.Value,
				})
				bytesLeftForChunks -= lbls[len(lbls)-1].Size()
			}
			extLabels.Range(func(l labels.Label) {
				lbls = append(lbls, labelpb.ZLabel{
					Name:  l.Name,
					Value: l.Value,
				})
				bytesLeftForChunks -= lbls[len(lbls)-1].Size()
			})
			sort.Slice(lbls, func(i, j int) bool {
				return lbls[i].Name < lbls[j].Name
			})

			frameBytesLeft := bytesLeftForChunks
			frame := &storepb.Series{Labels: lbls}
			for i, c := range s.Chunks {
				frame.Chunks = append(frame.Chunks, c)
				frameBytesLeft -= c.Size()

				if i == len(s.Chunks)-1 {
					break
				}

				if frameBytesLeft > 0 {
					continue
				}
				expected = append(expected, frame)
				frameBytesLeft = bytesLeftForChunks
				frame = &storepb.Series{Labels: lbls}
			}
			expected = append(expected, frame)
		}
	}

	storetestutil.TestServerSeries(t, store,
		&storetestutil.SeriesCase{
			Name: fmt.Sprintf("%d blocks and one WAL with %d samples, %d series each", numOfBlocks-1, samplesPerSeriesPerBlock, seriesPerBlock),
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
