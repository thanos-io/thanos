// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storetestutil

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	// LabelLongSuffix is a label with ~50B in size, to emulate real-world high cardinality.
	LabelLongSuffix = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
)

func allPostings(t testing.TB, ix tsdb.IndexReader) index.Postings {
	k, v := index.AllPostingsKey()
	p, err := ix.Postings(k, v)
	testutil.Ok(t, err)
	return p
}

type HeadGenOptions struct {
	TSDBDir                  string
	SamplesPerSeries, Series int
	ScrapeInterval           time.Duration

	WithWAL       bool
	PrependLabels labels.Labels
	SkipChunks    bool // Skips chunks in returned slice (not in generated head!).
	SampleType    chunkenc.ValueType

	Random *rand.Rand
}

func CreateBlockFromHead(t testing.TB, dir string, head *tsdb.Head) ulid.ULID {
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil, nil)
	testutil.Ok(t, err)

	testutil.Ok(t, os.MkdirAll(dir, 0777))

	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	ulid, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
	testutil.Ok(t, err)
	return ulid
}

// CreateHeadWithSeries returns head filled with given samples and same series returned in separate list for assertion purposes.
// Returned series list has "ext1"="1" prepended. Each series looks as follows:
// {foo=bar,i=000001aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd} <random value> where number indicate sample number from 0.
// Returned series are framed in the same way as remote read would frame them.
func CreateHeadWithSeries(t testing.TB, j int, opts HeadGenOptions) (*tsdb.Head, []*storepb.Series) {
	if opts.SamplesPerSeries < 1 || opts.Series < 1 {
		t.Fatal("samples and series has to be 1 or more")
	}
	if opts.ScrapeInterval == 0 {
		opts.ScrapeInterval = 1 * time.Millisecond
	}
	// Use float type if sample type is not set.
	if opts.SampleType == chunkenc.ValNone {
		opts.SampleType = chunkenc.ValFloat
	}

	fmt.Printf(
		"Creating %d %d-sample series with %s interval in %s\n",
		opts.Series,
		opts.SamplesPerSeries,
		opts.ScrapeInterval.String(),
		opts.TSDBDir,
	)

	var w *wlog.WL
	var err error
	if opts.WithWAL {
		w, err = wlog.New(nil, nil, filepath.Join(opts.TSDBDir, "wal"), wlog.ParseCompressionType(true, string(wlog.CompressionSnappy)))
		testutil.Ok(t, err)
	} else {
		testutil.Ok(t, os.MkdirAll(filepath.Join(opts.TSDBDir, "wal"), os.ModePerm))
	}

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = opts.TSDBDir
	headOpts.EnableNativeHistograms = *atomic.NewBool(true)
	h, err := tsdb.NewHead(nil, nil, w, nil, headOpts, nil)
	testutil.Ok(t, err)

	app := h.Appender(context.Background())
	for i := 0; i < opts.Series; i++ {
		tsLabel := j*opts.Series*opts.SamplesPerSeries + i*opts.SamplesPerSeries
		switch opts.SampleType {
		case chunkenc.ValFloat:
			appendFloatSamples(t, app, tsLabel, opts)
		case chunkenc.ValHistogram:
			appendHistogramSamples(t, app, tsLabel, opts)
		}
	}
	testutil.Ok(t, app.Commit())

	return h, ReadSeriesFromBlock(t, h, opts.PrependLabels, opts.SkipChunks)
}

func ReadSeriesFromBlock(t testing.TB, h tsdb.BlockReader, extLabels labels.Labels, skipChunks bool) []*storepb.Series {
	// Use TSDB and get all series for assertion.
	chks, err := h.Chunks()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, chks.Close()) }()

	ir, err := h.Index()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, ir.Close()) }()

	var (
		lset       labels.Labels
		chunkMetas []chunks.Meta
		expected   = make([]*storepb.Series, 0)
	)

	var builder labels.ScratchBuilder

	all := allPostings(t, ir)
	for all.Next() {
		testutil.Ok(t, ir.Series(all.At(), &builder, &chunkMetas))
		lset = builder.Labels()
		expected = append(expected, &storepb.Series{Labels: labelpb.ZLabelsFromPromLabels(append(extLabels.Copy(), lset...))})

		if skipChunks {
			continue
		}

		for _, c := range chunkMetas {
			chEnc, err := chks.Chunk(c)
			testutil.Ok(t, err)

			// Open Chunk.
			if c.MaxTime == math.MaxInt64 {
				c.MaxTime = c.MinTime + int64(chEnc.NumSamples()) - 1
			}

			expected[len(expected)-1].Chunks = append(expected[len(expected)-1].Chunks, storepb.AggrChunk{
				MinTime: c.MinTime,
				MaxTime: c.MaxTime,
				Raw: &storepb.Chunk{
					Data: chEnc.Bytes(),
					Type: storepb.Chunk_Encoding(chEnc.Encoding() - 1),
					Hash: xxhash.Sum64(chEnc.Bytes()),
				},
			})
		}
	}
	testutil.Ok(t, all.Err())
	return expected
}

func appendFloatSamples(t testing.TB, app storage.Appender, tsLabel int, opts HeadGenOptions) {
	ref, err := app.Append(
		0,
		labels.FromStrings("foo", "bar", "i", fmt.Sprintf("%07d%s", tsLabel, LabelLongSuffix), "j", fmt.Sprintf("%v", tsLabel)),
		int64(tsLabel)*opts.ScrapeInterval.Milliseconds(),
		opts.Random.Float64(),
	)
	testutil.Ok(t, err)

	for is := 1; is < opts.SamplesPerSeries; is++ {
		_, err := app.Append(ref, nil, int64(tsLabel+is)*opts.ScrapeInterval.Milliseconds(), opts.Random.Float64())
		testutil.Ok(t, err)
	}
}

func appendHistogramSamples(t testing.TB, app storage.Appender, tsLabel int, opts HeadGenOptions) {
	sample := &histogram.Histogram{
		Schema:        0,
		Count:         9,
		Sum:           -3.1415,
		ZeroCount:     12,
		ZeroThreshold: 0.001,
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 4},
			{Offset: 1, Length: 1},
		},
		NegativeBuckets: []int64{1, 2, -2, 1, -1},
	}

	ref, err := app.AppendHistogram(
		0,
		labels.FromStrings("foo", "bar", "i", fmt.Sprintf("%07d%s", tsLabel, LabelLongSuffix), "j", fmt.Sprintf("%v", tsLabel)),
		int64(tsLabel)*opts.ScrapeInterval.Milliseconds(),
		sample,
		nil,
	)
	testutil.Ok(t, err)

	for is := 1; is < opts.SamplesPerSeries; is++ {
		_, err := app.AppendHistogram(ref, nil, int64(tsLabel+is)*opts.ScrapeInterval.Milliseconds(), sample, nil)
		testutil.Ok(t, err)
	}
}

// SeriesServer is test gRPC storeAPI series server.
type SeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer

	ctx context.Context

	SeriesSet []*storepb.Series
	Warnings  []string
	HintsSet  []*types.Any

	Size int64
}

func NewSeriesServer(ctx context.Context) *SeriesServer {
	return &SeriesServer{ctx: ctx}
}

func (s *SeriesServer) Send(r *storepb.SeriesResponse) error {
	s.Size += int64(r.Size())

	if r.GetWarning() != "" {
		s.Warnings = append(s.Warnings, r.GetWarning())
		return nil
	}

	if r.GetSeries() != nil {
		s.SeriesSet = append(s.SeriesSet, r.GetSeries())
		return nil
	}

	if r.GetHints() != nil {
		s.HintsSet = append(s.HintsSet, r.GetHints())
		return nil
	}
	// Unsupported field, skip.
	return nil
}

func (s *SeriesServer) Context() context.Context {
	return s.ctx
}

func RunSeriesInterestingCases(t testutil.TB, maxSamples, maxSeries int, f func(t testutil.TB, samplesPerSeries, series int)) {
	for _, tc := range []struct {
		samplesPerSeries int
		series           int
	}{
		{
			samplesPerSeries: 1,
			series:           maxSeries,
		},
		{
			samplesPerSeries: maxSamples / (maxSeries / 10),
			series:           maxSeries / 10,
		},
		{
			samplesPerSeries: maxSamples,
			series:           1,
		},
	} {
		if ok := t.Run(fmt.Sprintf("%dSeriesWith%dSamples", tc.series, tc.samplesPerSeries), func(t testutil.TB) {
			f(t, tc.samplesPerSeries, tc.series)
		}); !ok {
			return
		}
		runtime.GC()
	}
}

// SeriesCase represents single test/benchmark case for testing storepb series.
type SeriesCase struct {
	Name string
	Req  *storepb.SeriesRequest

	// Exact expectations are checked only for tests. For benchmarks only length is assured.
	ExpectedSeries   []*storepb.Series
	ExpectedWarnings []string
	ExpectedHints    []hintspb.SeriesResponseHints
	HintsCompareFunc func(t testutil.TB, expected, actual hintspb.SeriesResponseHints)
}

// TestServerSeries runs tests against given cases.
func TestServerSeries(t testutil.TB, store storepb.StoreServer, cases ...*SeriesCase) {
	for _, c := range cases {
		t.Run(c.Name, func(t testutil.TB) {
			t.ResetTimer()
			for i := 0; i < t.N(); i++ {
				srv := NewSeriesServer(context.Background())
				testutil.Ok(t, store.Series(c.Req, srv))
				testutil.Equals(t, len(c.ExpectedWarnings), len(srv.Warnings), "%v", srv.Warnings)
				testutil.Equals(t, len(c.ExpectedSeries), len(srv.SeriesSet))
				testutil.Equals(t, len(c.ExpectedHints), len(srv.HintsSet))

				if !t.IsBenchmark() {
					if len(c.ExpectedSeries) == 1 {
						// For bucketStoreAPI chunks are not sorted within response. TODO: Investigate: Is this fine?
						sort.Slice(srv.SeriesSet[0].Chunks, func(i, j int) bool {
							return srv.SeriesSet[0].Chunks[i].MinTime < srv.SeriesSet[0].Chunks[j].MinTime
						})
					}

					// Huge responses can produce unreadable diffs - make it more human readable.
					if len(c.ExpectedSeries) > 4 {
						for j := range c.ExpectedSeries {
							testutil.Equals(t, c.ExpectedSeries[j].Labels, srv.SeriesSet[j].Labels, "%v series chunks mismatch", j)

							// Check chunks when it is not a skip chunk query
							if !c.Req.SkipChunks {
								if len(c.ExpectedSeries[j].Chunks) > 20 {
									testutil.Equals(t, len(c.ExpectedSeries[j].Chunks), len(srv.SeriesSet[j].Chunks), "%v series chunks number mismatch", j)
								}
								for ci := range c.ExpectedSeries[j].Chunks {
									testutil.Equals(t, c.ExpectedSeries[j].Chunks[ci], srv.SeriesSet[j].Chunks[ci], "%v series chunks mismatch %v", j, ci)
								}
							}
						}
					} else {
						testutil.Equals(t, c.ExpectedSeries, srv.SeriesSet)
					}

					var actualHints []hintspb.SeriesResponseHints
					for _, anyHints := range srv.HintsSet {
						hints := hintspb.SeriesResponseHints{}
						testutil.Ok(t, types.UnmarshalAny(anyHints, &hints))
						actualHints = append(actualHints, hints)
					}
					testutil.Equals(t, len(c.ExpectedHints), len(actualHints))
					for i, hint := range actualHints {
						if c.HintsCompareFunc == nil {
							testutil.Equals(t, c.ExpectedHints[i], hint)
						} else {
							c.HintsCompareFunc(t, c.ExpectedHints[i], hint)
						}
					}
				}
			}
		})
	}
}
