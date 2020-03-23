package e2eutil

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"golang.org/x/sync/errgroup"
)

const (
	// LabelLongSuffix is a label with ~50B in size, to emulate real-world high cardinality.
	LabelLongSuffix = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
)

// NewS return S.
func NewS(series, samples int) S {
	return S{Float64: rand.Float64, SeriesNum: series, SamplesNum: samples}
}

// S is	testing structure which allows series generation.
type S struct {
	Float64 func() float64

	SeriesNum  int
	SamplesNum int
}

func (s S) Run(tb testutil.TB, r func(tb testutil.TB, s S)) {
	tb.Run(fmt.Sprintf("%dSeriesWith%dSamples", s.SeriesNum, s.SamplesNum), func(tb testutil.TB) {
		r(tb, S{Float64: s.Float64, SamplesNum: s.SamplesNum, SeriesNum: s.SeriesNum})
	})
}

func (s S) Split(split int) S {
	return S{Float64: s.Float64, SeriesNum: s.SeriesNum / split, SamplesNum: s.SamplesNum / split}
}

func (s S) timestamp(offset int, seriesIndex int, samplesIndex int) int64 {
	return int64(offset*(s.SeriesNum*s.SamplesNum) + (seriesIndex * s.SeriesNum) + (samplesIndex * s.SamplesNum))
}

// SeriesLabels is series that is used for all generated data. i depends on number of series.
func (s S) SeriesLabels(offset int, seriesIndex int) labels.Labels {
	return labels.FromStrings("foo", "bar", "i", fmt.Sprintf("%10d%s", (offset*s.SeriesNum)+seriesIndex, LabelLongSuffix))
}

// CreateHeadSeries creates head with specified number of series x samples. It's caller responsibility to close head when finished.
// Each series is created in a deterministic manner:
//
// {"foo", "bar", "i", "< 10 digit prefix of offset * series >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples >
// {"foo", "bar", "i", "< 10 digit prefix of offset * series >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + 1 >
// {"foo", "bar", "i", "< 10 digit prefix of offset * series >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + 2 >
// ...
// {"foo", "bar", "i", "< 10 digit prefix of offset * series >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + samples - 1 >
//
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + 1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples >
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + 1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + 1 >
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + 1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + 2 >
// ...
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + 1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + samples - 1 >
// ...
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + series -1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples >
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + series -1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + 1 >
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + series -1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + 2 >
// ...
// {"foo", "bar", "i", "< 10 digit prefix of offset * series + series -1 >aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"} value: < valueGetFn > ts: < offset * series * samples + samples - 1 >
//
// This gives us certain property of such series no matter what offset is.
//
// * For one series, X samples querying time [0, X] will give us X samples.
// * For X series, 1 sample querying tim [0, X] will give us X series.
// * For X series, Y samples querying time [0, X] will give us X series with Y samples.
//
// Offset literally means the segment of series (e.g number of block).
func (s S) CreateHeadSeries(offset int) (*tsdb.Head, error) {
	batchSize := s.SeriesNum / runtime.GOMAXPROCS(0)
	if batchSize < 1 {
		batchSize = 1
	}
	return createHeadSeries(s, offset, batchSize, int64(s.SeriesNum*s.SamplesNum))
}

func createHeadSeries(s S, offset int, batchSize int, blockSize int64) (h *tsdb.Head, err error) {
	fmt.Println("Creating head and generating there", s.SeriesNum, "series with", s.SamplesNum, "each")
	if s.SamplesNum == 0 || s.SeriesNum == 0 {
		return nil, errors.New("samples and series cannot be zero")
	}

	h, err = tsdb.NewHead(nil, nil, nil, blockSize)
	if err != nil {
		return nil, errors.Wrap(err, "create head ")
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, h, "TSDB Head")
		}
	}()

	var g errgroup.Group
	for series := 0; series < s.SeriesNum; series += batchSize {
		series := series
		g.Go(func() error {
			app := h.Appender()

			var ts int64
			for i := series; i < series+batchSize; i++ {
				ts = s.timestamp(offset, i, 0)

				ref, err := app.Add(s.SeriesLabels(offset, i), ts, s.Float64())
				if err != nil {
					if rerr := app.Rollback(); rerr != nil {
						err = errors.Wrapf(err, "rollback failed: %v", rerr)
					}
					return errors.Wrap(err, "add sample")
				}

				for j := 1; j < s.SamplesNum; j++ {
					ts = s.timestamp(offset, i, j)
					if err := app.AddFast(ref, ts, s.Float64()); err != nil {
						if rerr := app.Rollback(); rerr != nil {
							err = errors.Wrapf(err, "rollback failed: %v", rerr)
						}
						return errors.Wrap(err, "add sample fast")
					}
				}

				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	fmt.Println("done")
	return h, nil
}

// ResponseSeriesFromHead returns []*storepb.SeriesResponse from Head.
func ResponseSeriesFromHead(t testutil.TB, h *tsdb.Head) (resps []*storepb.SeriesResponse) {
	chks, err := h.Chunks()
	testutil.Ok(t, err)

	ir, err := h.Index()
	testutil.Ok(t, err)
	defer ir.Close()

	var (
		lset       labels.Labels
		chunkMetas []chunks.Meta
		sBytes     = 0
	)

	p, err := ir.Postings(index.AllPostingsKey())
	for p.Next() {
		testutil.Ok(t, ir.Series(p.At(), &lset, &chunkMetas))

		l := storepb.PromLabelsToLabelsUnsafe(lset)
		for _, l := range l {
			sBytes += l.Size()
		}

		i := 0
		r := storepb.NewSeriesResponse(&storepb.Series{
			Labels: l,
		})
		for {
			c := chunkMetas[i]
			i++

			chBytes, err := chks.Chunk(c.Ref)
			testutil.Ok(t, err)

			sBytes += len(chBytes.Bytes())

			r.GetSeries().Chunks = append(r.GetSeries().Chunks, storepb.AggrChunk{
				MinTime: c.MinTime,
				MaxTime: c.MaxTime,
				Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chBytes.Bytes()},
			})

			// Compose many frames as remote read would do (so sidecar StoreAPI): 1048576
			if i >= len(chunkMetas) || sBytes >= 1048576 {
				resps = append(resps, r)
				r = storepb.NewSeriesResponse(&storepb.Series{
					Labels: storepb.PromLabelsToLabelsUnsafe(lset),
				})
			}
			if i >= len(chunkMetas) {
				break
			}

		}
	}
	testutil.Ok(t, p.Err())

	return resps
}

type Samples interface {
	Len() int
	Get(int) tsdbutil.Sample
}

type SampleChunks interface {
	Len() int
	Get(int) Samples
}

// sample struct is always copied as it is used very often, so we want to avoid long `e2eutil.Sample` statements in tests.
type sample struct {
	t int64
	v float64
}

func (s sample) T() int64   { return s.t }
func (s sample) V() float64 { return s.v }

type samples []sample

func (s samples) Len() int { return len(s) }
func (s samples) Get(i int) tsdbutil.Sample {
	return s[i]
}

type sampleChunks [][]sample

func (c sampleChunks) Len() int { return len(c) }
func (c sampleChunks) Get(i int) Samples {
	return samples(c[i])
}

func NewTestSeries(t testing.TB, lset labels.Labels, smplChunks SampleChunks) storepb.Series {
	var s storepb.Series
	s.Labels = storepb.PromLabelsToLabels(lset)
	for i := 0; smplChunks != nil && i < smplChunks.Len(); i++ {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(t, err)

		for j := 0; j < smplChunks.Get(i).Len(); j++ {
			a.Append(smplChunks.Get(i).Get(j).T(), smplChunks.Get(i).Get(j).V())
		}

		ch := storepb.AggrChunk{
			MinTime: smplChunks.Get(i).Get(0).T(),
			MaxTime: smplChunks.Get(i).Get(smplChunks.Get(i).Len() - 1).T(),
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return s
}
