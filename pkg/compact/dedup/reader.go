package dedup

import (
	"io"
	"math"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	tsdberrors "github.com/prometheus/tsdb/errors"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

type ChunkSeries struct {
	lset labels.Labels
	chks []chunks.Meta
}

type Sample struct {
	timestamp int64
	value     float64
}

func NewSample(timestamp int64, value float64) *Sample {
	return &Sample{timestamp: timestamp, value: value}
}

type SampleIterator struct {
	samples []*Sample
	i       int
}

func NewSampleIterator(samples []*Sample) *SampleIterator {
	return &SampleIterator{samples: samples}
}

func (s *SampleIterator) Err() error {
	return nil
}

func (s *SampleIterator) At() (int64, float64) {
	return s.samples[s.i].timestamp, s.samples[s.i].value
}

func (s *SampleIterator) Next() bool {
	if s.i >= len(s.samples) {
		return false
	}
	s.i++
	return true
}

func (s *SampleIterator) Seek(t int64) bool {
	if s.i < 0 {
		s.i = 0
	}
	for {
		if s.i >= len(s.samples) {
			return false
		}
		if s.samples[s.i].timestamp >= t {
			return true
		}
		s.i++
	}
}

type SampleSeries struct {
	lset    labels.Labels
	samples []*Sample
}

func NewSampleSeries(lset labels.Labels, samples []*Sample) *SampleSeries {
	return &SampleSeries{
		lset:    lset,
		samples: samples,
	}
}

func (ss *SampleSeries) ToChunkSeries() (*ChunkSeries, error) {
	if len(ss.samples) == 0 {
		return nil, nil
	}

	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		return nil, err
	}
	minTime := int64(math.MaxInt64)
	maxTime := int64(math.MinInt64)
	for _, v := range ss.samples {
		if minTime > v.timestamp {
			minTime = v.timestamp
		}
		if maxTime < v.timestamp {
			maxTime = v.timestamp
		}
		appender.Append(v.timestamp, v.value)
	}
	return &ChunkSeries{
		lset: ss.lset,
		chks: []chunks.Meta{
			{
				Chunk:   chunk,
				MinTime: minTime,
				MaxTime: maxTime,
			},
		},
	}, nil
}

type SampleReader struct {
	cr   tsdb.ChunkReader
	lset labels.Labels
	chks []chunks.Meta
}

func NewSampleReader(cr tsdb.ChunkReader, lset labels.Labels, chks []chunks.Meta) *SampleReader {
	return &SampleReader{
		cr:   cr,
		lset: lset,
		chks: chks,
	}
}

func (r *SampleReader) Read(tw *TimeWindow) ([]*Sample, error) {
	samples := make([]*Sample, 0)
	for _, c := range r.chks {
		chk, err := r.cr.Chunk(c.Ref)
		if err != nil {
			return nil, errors.Wrapf(err, "get chunk %d", c.Ref)
		}
		iterator := chk.Iterator()
		for iterator.Next() {
			timestamp, value := iterator.At()
			if timestamp < tw.MinTime {
				continue
			}
			// Ignore the data point which timestamp is same with MaxTime.
			// Make sure the block use scope [MinTime, MaxTime) instead of [MinTime, MaxTime]
			if timestamp >= tw.MaxTime {
				break
			}
			samples = append(samples, &Sample{
				timestamp: timestamp,
				value:     value,
			})
		}
	}
	return samples, nil
}

type BlockReader struct {
	logger  log.Logger
	closers []io.Closer

	ir tsdb.IndexReader
	cr tsdb.ChunkReader

	postings index.Postings
}

func NewBlockReader(logger log.Logger, blockDir string) (*BlockReader, error) {
	reader := &BlockReader{
		logger:  logger,
		closers: make([]io.Closer, 0, 3),
	}

	b, err := tsdb.OpenBlock(logger, blockDir, chunkenc.NewPool())
	if err != nil {
		return reader, errors.Wrapf(err, "open block under dir %s", blockDir)
	}
	reader.closers = append(reader.closers, b)

	ir, err := b.Index()
	if err != nil {
		return reader, errors.Wrap(err, "open index")
	}
	reader.ir = ir
	reader.closers = append(reader.closers, ir)

	cr, err := b.Chunks()
	if err != nil {
		return reader, errors.Wrap(err, "open chunks")
	}
	reader.cr = cr
	reader.closers = append(reader.closers, cr)

	postings, err := ir.Postings(index.AllPostingsKey())
	if err != nil {
		return reader, errors.Wrap(err, "read index postings")
	}
	reader.postings = ir.SortedPostings(postings)

	return reader, nil
}

func (r *BlockReader) Symbols() (map[string]struct{}, error) {
	return r.ir.Symbols()
}

func (r *BlockReader) Close() error {
	var merr tsdberrors.MultiError
	for i := len(r.closers) - 1; i >= 0; i-- {
		merr.Add(r.closers[i].Close())
	}
	return errors.Wrap(merr.Err(), "close closers")
}
