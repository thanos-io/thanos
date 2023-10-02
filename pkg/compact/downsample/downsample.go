// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// Standard downsampling resolution levels in Thanos.
const (
	ResLevel0 = int64(0)              // Raw data.
	ResLevel1 = int64(5 * 60 * 1000)  // 5 minutes in milliseconds.
	ResLevel2 = int64(60 * 60 * 1000) // 1 hour in milliseconds.
)

// Downsampling ranges i.e. minimum block size after which we start to downsample blocks (in seconds).
const (
	ResLevel1DownsampleRange = 40 * 60 * 60 * 1000      // 40 hours.
	ResLevel2DownsampleRange = 10 * 24 * 60 * 60 * 1000 // 10 days.
)

// Downsample downsamples the given block. It writes a new block into dir and returns its ID.
func Downsample(
	ctx context.Context,
	logger log.Logger,
	origMeta *metadata.Meta,
	b tsdb.BlockReader,
	dir string,
	resolution int64,
) (id ulid.ULID, err error) {
	if origMeta.Thanos.Downsample.Resolution >= resolution {
		return id, errors.New("target resolution not lower than existing one")
	}

	indexr, err := b.Index()
	if err != nil {
		return id, errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithErrCapture(&err, indexr, "downsample index reader")

	chunkr, err := b.Chunks()
	if err != nil {
		return id, errors.Wrap(err, "open chunk reader")
	}
	defer runutil.CloseWithErrCapture(&err, chunkr, "downsample chunk reader")

	// Generate new block id.
	uid := ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))

	// Create block directory to populate with chunks, meta and index files into.
	blockDir := filepath.Join(dir, uid.String())
	if err := os.MkdirAll(blockDir, 0750); err != nil {
		return id, errors.Wrap(err, "mkdir block dir")
	}

	// Remove blockDir in case of errors.
	defer func() {
		if err != nil {
			var merr errutil.MultiError
			merr.Add(err)
			merr.Add(os.RemoveAll(blockDir))
			err = merr.Err()
		}
	}()

	// Copy original meta to the new one. Update downsampling resolution and ULID for a new block.
	newMeta := *origMeta
	newMeta.Thanos.Downsample.Resolution = resolution
	newMeta.ULID = uid

	// Writes downsampled chunks right into the files, avoiding excess memory allocation.
	// Flushes index and meta data after aggregations.
	streamedBlockWriter, err := NewStreamedBlockWriter(blockDir, indexr, logger, newMeta)
	if err != nil {
		return id, errors.Wrap(err, "get streamed block writer")
	}
	defer runutil.CloseWithErrCapture(&err, streamedBlockWriter, "close stream block writer")

	key, values := index.AllPostingsKey()
	postings, err := indexr.Postings(ctx, key, values)
	if err != nil {
		return id, errors.Wrap(err, "get all postings list")
	}

	var (
		aggrChunks []*AggrChunk
		all        []sample
		chks       []chunks.Meta
		builder    labels.ScratchBuilder
		reuseIt    chunkenc.Iterator
	)
	for postings.Next() {
		chks = chks[:0]
		all = all[:0]
		aggrChunks = aggrChunks[:0]

		// Get series labels and chunks. Downsampled data is sensitive to chunk boundaries
		// and we need to preserve them to properly downsample previously downsampled data.
		if err := indexr.Series(postings.At(), &builder, &chks); err != nil {
			return id, errors.Wrapf(err, "get series %d", postings.At())
		}
		lset := builder.Labels()

		for i, c := range chks[1:] {
			if chks[i].MaxTime >= c.MinTime {
				return id, errors.Errorf("found overlapping chunks within series %d. Chunks expected to be ordered by min time and non-overlapping, got: %v", postings.At(), chks)
			}
		}

		// While #183 exists, we sanitize the chunks we retrieved from the block
		// before retrieving their samples.
		for i, c := range chks {
			chk, err := chunkr.Chunk(c)
			if err != nil {
				return id, errors.Wrapf(err, "get chunk %d, series %d", c.Ref, postings.At())
			}
			chks[i].Chunk = chk
		}

		// Raw and already downsampled data need different processing.
		if origMeta.Thanos.Downsample.Resolution == 0 {
			for _, c := range chks {
				// TODO(bwplotka): We can optimze this further by using in WriteSeries iterators of each chunk instead of
				// samples. Also ensure 120 sample limit, otherwise we have gigantic chunks.
				// https://github.com/thanos-io/thanos/issues/2542.
				if err := expandChunkIterator(c.Chunk.Iterator(reuseIt), &all); err != nil {
					return id, errors.Wrapf(err, "expand chunk %d, series %d", c.Ref, postings.At())
				}
			}
			if err := streamedBlockWriter.WriteSeries(lset, DownsampleRaw(all, resolution)); err != nil {
				return id, errors.Wrapf(err, "downsample raw data, series: %d", postings.At())
			}
		} else {
			// Downsample a block that contains aggregated chunks already.
			for _, c := range chks {
				ac, ok := c.Chunk.(*AggrChunk)
				if !ok {
					if c.Chunk.NumSamples() == 0 {
						// Downsampled block can erroneously contain empty XOR chunks, skip those
						// https://github.com/thanos-io/thanos/issues/5272
						level.Warn(logger).Log("msg", fmt.Sprintf("expected downsampled chunk (*downsample.AggrChunk) got an empty %T instead for series: %d", c.Chunk, postings.At()))
						continue
					} else {
						if err := expandChunkIterator(c.Chunk.Iterator(reuseIt), &all); err != nil {
							return id, errors.Wrapf(err, "expand chunk %d, series %d", c.Ref, postings.At())
						}
						aggrDataChunks := DownsampleRaw(all, ResLevel1)
						for _, cn := range aggrDataChunks {
							ac, ok = cn.Chunk.(*AggrChunk)
							if !ok {
								return id, errors.New("Not able to convert non-empty XOR chunks to 5m downsampled aggregated chunks.")
							}
						}
					}

				}
				aggrChunks = append(aggrChunks, ac)

			}
			downsampledChunks, err := downsampleAggr(
				aggrChunks,
				&all,
				chks[0].MinTime,
				chks[len(chks)-1].MaxTime,
				origMeta.Thanos.Downsample.Resolution,
				resolution,
			)
			if err != nil {
				return id, errors.Wrapf(err, "downsample aggregate block, series: %d", postings.At())
			}
			if err := streamedBlockWriter.WriteSeries(lset, downsampledChunks); err != nil {
				return id, errors.Wrapf(err, "write series: %d", postings.At())
			}
		}
	}
	if postings.Err() != nil {
		return id, errors.Wrap(postings.Err(), "iterate series set")
	}

	id = uid
	return
}

// currentWindow returns the end timestamp of the window that t falls into.
func currentWindow(t, r int64) int64 {
	// The next timestamp is the next number after s.t that's aligned with window.
	// We subtract 1 because block ranges are [from, to) and the last sample would
	// go out of bounds otherwise.
	return t - (t % r) + r - 1
}

// rangeFullness returns the fraction of how the range [mint, maxt] covered
// with count samples at the given step size.
// It return value is bounded to [0, 1].
func rangeFullness(mint, maxt, step int64, count int) float64 {
	f := float64(count) / (float64(maxt-mint) / float64(step))
	if f > 1 {
		return 1
	}
	return f
}

// targetChunkCount calculates how many chunks should be produced when downsampling a series.
// It consider the total time range, the number of input sample, the input and output resolution.
func targetChunkCount(mint, maxt, inRes, outRes int64, count int) (x int) {
	// We compute how many samples we could produce for the given time range and adjust
	// it by how densely the range is actually filled given the number of input samples and their
	// resolution.
	maxSamples := float64((maxt - mint) / outRes)
	expSamples := int(maxSamples*rangeFullness(mint, maxt, inRes, count)) + 1

	// Increase the number of target chunks until each chunk will have less than
	// 140 samples on average.
	for x = 1; expSamples/x > 140; x++ {
	}
	return x
}

// aggregator collects cumulative stats for a stream of values.
type aggregator struct {
	total   int     // Total samples processed.
	count   int     // Samples in current window.
	sum     float64 // Value sum of current window.
	min     float64 // Min of current window.
	max     float64 // Max of current window.
	counter float64 // Total counter state since beginning.
	resets  int     // Number of counter resets since beginning.
	last    float64 // Last added value.
}

// reset the stats to start a new aggregation window.
func (a *aggregator) reset() {
	a.count = 0
	a.sum = 0
	a.min = math.MaxFloat64
	a.max = -math.MaxFloat64
}

func (a *aggregator) add(v float64) {
	if a.total > 0 {
		if v < a.last {
			// Counter reset, correct the value.
			a.counter += v
			a.resets++
		} else {
			// Add delta with last value to the counter.
			a.counter += v - a.last
		}
	} else {
		// First sample sets the counter.
		a.counter = v
	}
	a.last = v

	a.sum += v
	a.count++
	a.total++

	if v < a.min {
		a.min = v
	}
	if v > a.max {
		a.max = v
	}
}

// aggrChunkBuilder builds chunks for multiple different aggregates.
type aggrChunkBuilder struct {
	mint, maxt int64
	added      int

	chunks [5]chunkenc.Chunk
	apps   [5]chunkenc.Appender
}

func newAggrChunkBuilder() *aggrChunkBuilder {
	b := &aggrChunkBuilder{
		mint: math.MaxInt64,
		maxt: math.MinInt64,
	}
	b.chunks[AggrCount] = chunkenc.NewXORChunk()
	b.chunks[AggrSum] = chunkenc.NewXORChunk()
	b.chunks[AggrMin] = chunkenc.NewXORChunk()
	b.chunks[AggrMax] = chunkenc.NewXORChunk()
	b.chunks[AggrCounter] = chunkenc.NewXORChunk()

	for i, c := range b.chunks {
		if c != nil {
			b.apps[i], _ = c.Appender()
		}
	}
	return b
}

func (b *aggrChunkBuilder) add(t int64, aggr *aggregator) {
	if t < b.mint {
		b.mint = t
	}
	if t > b.maxt {
		b.maxt = t
	}
	b.apps[AggrSum].Append(t, aggr.sum)
	b.apps[AggrMin].Append(t, aggr.min)
	b.apps[AggrMax].Append(t, aggr.max)
	b.apps[AggrCount].Append(t, float64(aggr.count))
	b.apps[AggrCounter].Append(t, aggr.counter)

	b.added++
}

func (b *aggrChunkBuilder) encode() chunks.Meta {
	return chunks.Meta{
		MinTime: b.mint,
		MaxTime: b.maxt,
		Chunk:   EncodeAggrChunk(b.chunks),
	}
}

// DownsampleRaw create a series of aggregation chunks for the given sample data.
func DownsampleRaw(data []sample, resolution int64) []chunks.Meta {
	if len(data) == 0 {
		return nil
	}

	mint, maxt := data[0].t, data[len(data)-1].t
	// We assume a raw resolution of 1 minute. In practice it will often be lower
	// but this is sufficient for our heuristic to produce well-sized chunks.
	numChunks := targetChunkCount(mint, maxt, 1*60*1000, resolution, len(data))
	return downsampleRawLoop(data, resolution, numChunks)
}

func downsampleRawLoop(data []sample, resolution int64, numChunks int) []chunks.Meta {
	batchSize := (len(data) / numChunks) + 1
	chks := make([]chunks.Meta, 0, numChunks)

	for len(data) > 0 {
		j := batchSize
		if j > len(data) {
			j = len(data)
		}
		curW := currentWindow(data[j-1].t, resolution)

		// The batch we took might end in the middle of a downsampling window. We additionally grab
		// all further samples in the window to keep our samples regular.
		for ; j < len(data) && data[j].t <= curW; j++ {
		}

		batch := make([]sample, 0, j)
		for _, s := range data[:j] {
			if math.IsNaN(s.v) {
				continue
			}
			batch = append(batch, s)
		}
		data = data[j:]
		if len(batch) == 0 {
			continue
		}

		ab := newAggrChunkBuilder()

		// Encode first raw value; see ApplyCounterResetsSeriesIterator.
		ab.apps[AggrCounter].Append(batch[0].t, batch[0].v)

		lastT := downsampleBatch(batch, resolution, ab.add)

		// Encode last raw value; see ApplyCounterResetsSeriesIterator.
		ab.apps[AggrCounter].Append(lastT, batch[len(batch)-1].v)

		chks = append(chks, ab.encode())
	}

	return chks
}

// downsampleBatch aggregates the data over the given resolution and calls add each time
// the end of a resolution was reached.
func downsampleBatch(data []sample, resolution int64, add func(int64, *aggregator)) int64 {
	var (
		aggr  aggregator
		nextT = int64(-1)
		lastT = data[len(data)-1].t
	)
	// Fill up one aggregate chunk with up to m samples.
	for _, s := range data {
		if value.IsStaleNaN(s.v) {
			continue
		}
		if s.t > nextT {
			if nextT != -1 {
				add(nextT, &aggr)
			}
			aggr.reset()
			nextT = currentWindow(s.t, resolution)
			// Limit next timestamp to not go beyond the batch. A subsequent batch
			// may overlap in time range otherwise.
			// We have aligned batches for raw downsamplings but subsequent downsamples
			// are forced to be chunk-boundary aligned and cannot guarantee this.
			if nextT > lastT {
				nextT = lastT
			}
		}
		aggr.add(s.v)
	}
	// Add the last sample.
	add(nextT, &aggr)

	return nextT
}

// downsampleAggr downsamples a sequence of aggregation chunks to the given resolution.
func downsampleAggr(chks []*AggrChunk, buf *[]sample, mint, maxt, inRes, outRes int64) ([]chunks.Meta, error) {
	var numSamples int
	for _, c := range chks {
		numSamples += c.NumSamples()
	}
	numChunks := targetChunkCount(mint, maxt, inRes, outRes, numSamples)
	return downsampleAggrLoop(chks, buf, outRes, numChunks)
}

func downsampleAggrLoop(chks []*AggrChunk, buf *[]sample, resolution int64, numChunks int) ([]chunks.Meta, error) {
	// We downsample aggregates only along chunk boundaries. This is required
	// for counters to be downsampled correctly since a chunk's first and last
	// counter values are the true values of the original series. We need
	// to preserve them even across multiple aggregation iterations.
	res := make([]chunks.Meta, 0, numChunks)
	batchSize := len(chks) / numChunks

	for len(chks) > 0 {
		j := batchSize
		if j > len(chks) {
			j = len(chks)
		}
		part := chks[:j]
		chks = chks[j:]

		chk, err := downsampleAggrBatch(part, buf, resolution)
		if err != nil {
			return nil, err
		}
		res = append(res, chk)
	}

	return res, nil
}

// expandChunkIterator reads all samples from the iterator and appends them to buf.
// Stale markers and out of order samples are skipped.
func expandChunkIterator(it chunkenc.Iterator, buf *[]sample) error {
	// For safety reasons, we check for each sample that it does not go back in time.
	// If it does, we skip it.
	lastT := int64(0)

	for it.Next() != chunkenc.ValNone {
		t, v := it.At()
		if value.IsStaleNaN(v) {
			continue
		}
		if t >= lastT {
			*buf = append(*buf, sample{t, v})
			lastT = t
		}
	}
	return it.Err()
}

func downsampleAggrBatch(chks []*AggrChunk, buf *[]sample, resolution int64) (chk chunks.Meta, err error) {
	ab := &aggrChunkBuilder{}
	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
	var reuseIt chunkenc.Iterator

	// do does a generic aggregation for count, sum, min, and max aggregates.
	// Counters need special treatment.
	do := func(at AggrType, f func(a *aggregator) float64) error {
		*buf = (*buf)[:0]
		// Expand all samples for the aggregate type.
		for _, chk := range chks {
			c, err := chk.Get(at)
			if err == ErrAggrNotExist {
				continue
			} else if err != nil {
				return err
			}
			if err := expandChunkIterator(c.Iterator(reuseIt), buf); err != nil {
				return err
			}
		}
		if len(*buf) == 0 {
			return nil
		}
		ab.chunks[at] = chunkenc.NewXORChunk()
		ab.apps[at], _ = ab.chunks[at].Appender()

		downsampleBatch(*buf, resolution, func(t int64, a *aggregator) {
			if t < mint {
				mint = t
			} else if t > maxt {
				maxt = t
			}
			ab.apps[at].Append(t, f(a))
		})
		return nil
	}
	if err := do(AggrCount, func(a *aggregator) float64 {
		// To get correct count of elements from already downsampled count chunk
		// we have to sum those values.
		return a.sum
	}); err != nil {
		return chk, err
	}
	if err = do(AggrSum, func(a *aggregator) float64 {
		return a.sum
	}); err != nil {
		return chk, err
	}
	if err := do(AggrMin, func(a *aggregator) float64 {
		return a.min
	}); err != nil {
		return chk, err
	}
	if err := do(AggrMax, func(a *aggregator) float64 {
		return a.max
	}); err != nil {
		return chk, err
	}

	// Handle counters by applying resets directly.
	acs := make([]chunkenc.Iterator, 0, len(chks))
	for _, achk := range chks {
		c, err := achk.Get(AggrCounter)
		if err == ErrAggrNotExist {
			continue
		} else if err != nil {
			return chk, err
		}
		acs = append(acs, c.Iterator(reuseIt))
	}
	*buf = (*buf)[:0]
	it := NewApplyCounterResetsIterator(acs...)

	if err := expandChunkIterator(it, buf); err != nil {
		return chk, err
	}
	if len(*buf) == 0 {
		ab.mint = mint
		ab.maxt = maxt
		return ab.encode(), nil
	}
	ab.chunks[AggrCounter] = chunkenc.NewXORChunk()
	ab.apps[AggrCounter], _ = ab.chunks[AggrCounter].Appender()

	// Retain first raw value; see ApplyCounterResetsSeriesIterator.
	ab.apps[AggrCounter].Append((*buf)[0].t, (*buf)[0].v)

	lastT := downsampleBatch(*buf, resolution, func(t int64, a *aggregator) {
		if t < mint {
			mint = t
		} else if t > maxt {
			maxt = t
		}
		ab.apps[AggrCounter].Append(t, a.counter)
	})

	// Retain last raw value; see ApplyCounterResetsSeriesIterator.
	ab.apps[AggrCounter].Append(lastT, it.lastV)

	ab.mint = mint
	ab.maxt = maxt
	return ab.encode(), nil
}

type sample struct {
	t int64
	v float64
}

// ApplyCounterResetsSeriesIterator generates monotonically increasing values by iterating
// over an ordered sequence of chunks, which should be raw or aggregated chunks
// of counter values. The generated samples can be used by PromQL functions
// like 'rate' that calculate differences between counter values. Stale Markers
// are removed as well.
//
// Counter aggregation chunks must have the first and last values from their
// original raw series: the first raw value should be the first value encoded
// in the chunk, and the last raw value is encoded by the duplication of the
// previous sample's timestamp. As iteration occurs between chunks, the
// comparison between the last raw value of the earlier chunk and the first raw
// value of the later chunk ensures that counter resets between chunks are
// recognized and that the correct value delta is calculated.
//
// It handles overlapped chunks (removes overlaps).
// NOTE: It is important to deduplicate with care ensuring that you don't hit
// issue https://github.com/thanos-io/thanos/issues/2401#issuecomment-621958839.
// NOTE(bwplotka): This hides resets from PromQL engine. This means it will not work for PromQL resets function.
type ApplyCounterResetsSeriesIterator struct {
	chks        []chunkenc.Iterator
	i           int     // Current chunk.
	total       int     // Total number of processed samples.
	lastT       int64   // Timestamp of the last sample.
	lastV       float64 // Value of the last sample.
	totalV      float64 // Total counter state since beginning of series.
	lastValType chunkenc.ValueType
}

func NewApplyCounterResetsIterator(chks ...chunkenc.Iterator) *ApplyCounterResetsSeriesIterator {
	return &ApplyCounterResetsSeriesIterator{chks: chks}
}

// TODO(rabenhorst): Native histogram support needs to be added, float type is hardcoded.
func (it *ApplyCounterResetsSeriesIterator) Next() chunkenc.ValueType {
	for {
		if it.i >= len(it.chks) {
			return chunkenc.ValNone
		}
		it.lastValType = it.chks[it.i].Next()
		if it.lastValType == chunkenc.ValNone {
			it.i++
			// While iterators are ordered, they are not generally guaranteed to be
			// non-overlapping. Ensure that the series does not go back in time by seeking at least
			// to the next timestamp.
			return it.Seek(it.lastT + 1)
		}
		// Counter resets do not need to be handled for non-float sample types.
		if it.lastValType != chunkenc.ValFloat {
			it.lastT = it.chks[it.i].AtT()
			return it.lastValType
		}

		t, v := it.chks[it.i].At()
		if math.IsNaN(v) {
			return it.Next()
		}
		// First sample sets the initial counter state.
		if it.total == 0 {
			it.total++
			it.lastT, it.lastV = t, v
			it.totalV = v
			return chunkenc.ValFloat
		}
		// If the timestamp increased, it is not the special last sample.
		if t > it.lastT {
			if v >= it.lastV {
				it.totalV += v - it.lastV
			} else {
				it.totalV += v
			}
			it.lastT, it.lastV = t, v
			it.total++
			return chunkenc.ValFloat
		}
		// We hit a sample that indicates what the true last value was. For the
		// next chunk we use it to determine whether there was a counter reset between them.
		if t == it.lastT {
			it.lastV = v
		}
		// Otherwise the series went back in time and we just keep moving forward.
	}
}

func (it *ApplyCounterResetsSeriesIterator) At() (t int64, v float64) {
	return it.lastT, it.totalV
}

func (it *ApplyCounterResetsSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	return it.chks[it.i].AtHistogram()
}

func (it *ApplyCounterResetsSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return it.chks[it.i].AtFloatHistogram()
}

func (it *ApplyCounterResetsSeriesIterator) AtT() int64 {
	return it.lastT
}

func (it *ApplyCounterResetsSeriesIterator) Seek(x int64) chunkenc.ValueType {
	// Don't use underlying Seek, but iterate over next to not miss counter resets.
	for {
		if t := it.AtT(); t >= x {
			return it.lastValType
		}

		if it.Next() == chunkenc.ValNone {
			return chunkenc.ValNone
		}
	}
}

func (it *ApplyCounterResetsSeriesIterator) Err() error {
	if it.i >= len(it.chks) {
		return nil
	}
	return it.chks[it.i].Err()
}

// AverageChunkIterator emits an artificial series of average samples based in aggregate
// chunks with sum and count aggregates.
type AverageChunkIterator struct {
	cntIt chunkenc.Iterator
	sumIt chunkenc.Iterator
	t     int64
	v     float64
	err   error
}

func NewAverageChunkIterator(cnt, sum chunkenc.Iterator) *AverageChunkIterator {
	return &AverageChunkIterator{cntIt: cnt, sumIt: sum}
}

// TODO(rabenhorst): Native histogram support needs to be added, float type is hardcoded.
func (it *AverageChunkIterator) Next() chunkenc.ValueType {
	cok, sok := it.cntIt.Next(), it.sumIt.Next()
	if cok != sok {
		it.err = errors.New("sum and count iterator not aligned")
		return chunkenc.ValNone
	}
	if cok == chunkenc.ValNone {
		return chunkenc.ValNone
	}

	cntT, cntV := it.cntIt.At()
	sumT, sumV := it.sumIt.At()
	if cntT != sumT {
		it.err = errors.New("sum and count timestamps not aligned")
		return chunkenc.ValNone
	}
	it.t, it.v = cntT, sumV/cntV
	return chunkenc.ValFloat
}

func (it *AverageChunkIterator) Seek(t int64) chunkenc.ValueType {
	it.err = errors.New("seek used, but not implemented")
	return chunkenc.ValNone
}

func (it *AverageChunkIterator) At() (int64, float64) {
	return it.t, it.v
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (it *AverageChunkIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (it *AverageChunkIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (it *AverageChunkIterator) AtT() int64 {
	return it.t
}

func (it *AverageChunkIterator) Err() error {
	if it.cntIt.Err() != nil {
		return it.cntIt.Err()
	}
	if it.sumIt.Err() != nil {
		return it.sumIt.Err()
	}
	return it.err
}

// SamplesFromTSDBSamples converts tsdbutil.Sample slice to samples.
func SamplesFromTSDBSamples(samples []chunks.Sample) []sample {
	res := make([]sample, len(samples))
	for i, s := range samples {
		res[i] = sample{t: s.T(), v: s.F()}
	}
	return res
}

// GatherNoDownsampleMarkFilter is a block.Fetcher filter that passes all metas.
// While doing it, it gathers all no-downsample-mark.json markers.
type GatherNoDownsampleMarkFilter struct {
	logger                log.Logger
	bkt                   objstore.InstrumentedBucketReader
	noDownsampleMarkedMap map[ulid.ULID]*metadata.NoDownsampleMark
	concurrency           int
	mtx                   sync.Mutex
}

// NewGatherNoDownsampleMarkFilter creates GatherNoDownsampleMarkFilter.
func NewGatherNoDownsampleMarkFilter(logger log.Logger, bkt objstore.InstrumentedBucketReader) *GatherNoDownsampleMarkFilter {
	return &GatherNoDownsampleMarkFilter{
		logger:      logger,
		bkt:         bkt,
		concurrency: 1,
	}
}

// NoDownsampleMarkedBlocks returns block ids that were marked for no downsample.
func (f *GatherNoDownsampleMarkFilter) NoDownsampleMarkedBlocks() map[ulid.ULID]*metadata.NoDownsampleMark {
	f.mtx.Lock()
	copiedNoDownsampleMarked := make(map[ulid.ULID]*metadata.NoDownsampleMark, len(f.noDownsampleMarkedMap))
	for k, v := range f.noDownsampleMarkedMap {
		copiedNoDownsampleMarked[k] = v
	}
	f.mtx.Unlock()

	return copiedNoDownsampleMarked
}

// TODO (@rohitkochhar): reduce code duplication here by combining
// this code with that of GatherNoCompactionMarkFilter
// Filter passes all metas, while gathering no downsample markers.
func (f *GatherNoDownsampleMarkFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, modified block.GaugeVec) error {
	f.mtx.Lock()
	f.noDownsampleMarkedMap = make(map[ulid.ULID]*metadata.NoDownsampleMark)
	f.mtx.Unlock()

	// Make a copy of block IDs to check, in order to avoid concurrency issues
	// between the scheduler and workers.
	blockIDs := make([]ulid.ULID, 0, len(metas))
	for id := range metas {
		blockIDs = append(blockIDs, id)
	}

	var (
		eg errgroup.Group
		ch = make(chan ulid.ULID, f.concurrency)
	)

	for i := 0; i < f.concurrency; i++ {
		eg.Go(func() error {
			var lastErr error
			for id := range ch {
				m := &metadata.NoDownsampleMark{}

				if err := metadata.ReadMarker(ctx, f.logger, f.bkt, id.String(), m); err != nil {
					if errors.Cause(err) == metadata.ErrorMarkerNotFound {
						continue
					}
					if errors.Cause(err) == metadata.ErrorUnmarshalMarker {
						level.Warn(f.logger).Log("msg", "found partial no-downsample-mark.json; if we will see it happening often for the same block, consider manually deleting no-downsample-mark.json from the object storage", "block", id, "err", err)
						continue
					}
					// Remember the last error and continue draining the channel.
					lastErr = err
					continue
				}

				f.mtx.Lock()
				f.noDownsampleMarkedMap[id] = m
				f.mtx.Unlock()
				synced.WithLabelValues(block.MarkedForNoDownsampleMeta).Inc()
			}

			return lastErr
		})
	}

	// Workers scheduled, distribute blocks.
	eg.Go(func() error {
		defer close(ch)

		for _, id := range blockIDs {
			select {
			case ch <- id:
				// Nothing to do.
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "filter blocks marked for no downsample")
	}

	return nil
}
