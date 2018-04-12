package downsample

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/tsdb/chunkenc"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// Standard downsampling resolution levels in Thanos.
const (
	ResLevel0 = int64(0)              // raw data
	ResLevel1 = int64(5 * 60 * 1000)  // 5 minutes in milliseconds
	ResLevel2 = int64(60 * 60 * 1000) // 1 hour in milliseconds
)

// Downsample downsamples the given block. It writes a new block into dir and returns its ID.
func Downsample(
	ctx context.Context,
	origMeta *block.Meta,
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
	defer indexr.Close()

	chunkr, err := b.Chunks()
	if err != nil {
		return id, errors.Wrap(err, "open chunk reader")
	}
	defer chunkr.Close()

	rng := origMeta.MaxTime - origMeta.MinTime

	// Write downsampled data in a custom memory block where we have fine-grained control
	// over created chunks.
	// This is necessary since we need to inject special values at the end of chunks for
	// some aggregations.
	newb := newMemBlock()

	pall, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return id, errors.Wrap(err, "get all postings list")
	}
	var (
		aggrChunks []AggrChunk
		all        []sample
		chks       []chunks.Meta
	)
	for pall.Next() {
		var lset labels.Labels
		chks = chks[:0]
		all = all[:0]
		aggrChunks = aggrChunks[:0]

		// Get series labels and chunks. Downsampled data is sensitive to chunk boundaries
		// and we need to preserve them to properly downsample previously downsampled data.
		if err := indexr.Series(pall.At(), &lset, &chks); err != nil {
			return id, errors.Wrapf(err, "get series %d", pall.At())
		}
		// While #183 exists, we sanitize the chunks we retrieved from the block
		// before retrieving their samples.
		for i, c := range chks {
			chk, err := chunkr.Chunk(c.Ref)
			if err != nil {
				return id, errors.Wrapf(err, "get chunk %d", c.Ref)
			}
			chks[i].Chunk = chk
		}

		// Raw and already downsampled data need different processing.
		if origMeta.Thanos.Downsample.Resolution == 0 {
			for _, c := range chks {
				if err := expandChunkIterator(c.Chunk.Iterator(), &all); err != nil {
					return id, errors.Wrapf(err, "expand chunk %d", c.Ref)
				}
			}
			newb.addSeries(&series{lset: lset, chunks: downsampleRaw(all, resolution)})
			continue
		}

		// Downsample a block that contains aggregate chunks already.
		for _, c := range chks {
			aggrChunks = append(aggrChunks, c.Chunk.(AggrChunk))
		}
		res, err := downsampleAggr(
			aggrChunks,
			&all,
			chks[0].MinTime,
			chks[len(chks)-1].MaxTime,
			origMeta.Thanos.Downsample.Resolution,
			resolution,
		)
		if err != nil {
			return id, errors.Wrap(err, "downsample aggregate block")
		}
		newb.addSeries(&series{lset: lset, chunks: res})
	}
	if pall.Err() != nil {
		return id, errors.Wrap(pall.Err(), "iterate series set")
	}
	comp, err := tsdb.NewLeveledCompactor(nil, log.NewNopLogger(), []int64{rng}, NewPool())
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}
	id, err = comp.Write(dir, newb, origMeta.MinTime, origMeta.MaxTime)
	if err != nil {
		return id, errors.Wrap(err, "compact head")
	}
	bdir := filepath.Join(dir, id.String())

	meta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return id, errors.Wrap(err, "read block meta")
	}
	meta.Thanos.Labels = origMeta.Thanos.Labels
	meta.Thanos.Downsample.Resolution = resolution
	meta.Compaction = origMeta.Compaction

	os.Remove(filepath.Join(bdir, "tombstones"))

	if err := block.WriteMetaFile(bdir, meta); err != nil {
		return id, errors.Wrap(err, "write block meta")
	}
	return id, nil
}

// memBlock is an in-memory block that implements a subset of the tsdb.BlockReader interface
// to allow tsdb.LeveledCompactor to persist the data as a block.
type memBlock struct {
	// Dummies to implement unused methods.
	tsdb.IndexReader

	symbols  map[string]struct{}
	postings []uint64
	series   []*series
	chunks   []chunkenc.Chunk
}

func newMemBlock() *memBlock {
	return &memBlock{symbols: map[string]struct{}{}}
}

func (b *memBlock) addSeries(s *series) {
	sid := uint64(len(b.series))
	b.postings = append(b.postings, sid)
	b.series = append(b.series, s)

	for _, l := range s.lset {
		b.symbols[l.Name] = struct{}{}
		b.symbols[l.Value] = struct{}{}
	}

	for i, cm := range s.chunks {
		cid := uint64(len(b.chunks))
		s.chunks[i].Ref = cid
		b.chunks = append(b.chunks, cm.Chunk)
	}
}

func (b *memBlock) Postings(name, val string) (index.Postings, error) {
	allName, allVal := index.AllPostingsKey()

	if name != allName || val != allVal {
		return nil, errors.New("unsupported call to Postings()")
	}
	sort.Slice(b.postings, func(i, j int) bool {
		return labels.Compare(b.series[b.postings[i]].lset, b.series[b.postings[j]].lset) < 0
	})
	return index.NewListPostings(b.postings), nil
}

func (b *memBlock) Series(id uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	if id >= uint64(len(b.series)) {
		return errors.Wrapf(tsdb.ErrNotFound, "series with ID %d does not exist", id)
	}
	s := b.series[id]

	*lset = append((*lset)[:0], s.lset...)
	*chks = append((*chks)[:0], s.chunks...)

	return nil
}

func (b *memBlock) Chunk(id uint64) (chunkenc.Chunk, error) {
	if id >= uint64(len(b.chunks)) {
		return nil, errors.Wrapf(tsdb.ErrNotFound, "chunk with ID %d does not exist", id)
	}
	return b.chunks[id], nil
}

func (b *memBlock) Symbols() (map[string]struct{}, error) {
	return b.symbols, nil
}

func (b *memBlock) SortedPostings(p index.Postings) index.Postings {
	return p
}

func (b *memBlock) Index() (tsdb.IndexReader, error) {
	return b, nil
}

func (b *memBlock) Chunks() (tsdb.ChunkReader, error) {
	return b, nil
}

func (b *memBlock) Tombstones() (tsdb.TombstoneReader, error) {
	return tsdb.EmptyTombstoneReader(), nil
}

func (b *memBlock) Close() error {
	return nil
}

// currentWindow returns the end timestamp of the window that t falls into.
func currentWindow(t, r int64) int64 {
	// The next timestamp is the next number after s.t that's aligned with window.
	// We substract 1 because block ranges are [from, to) and the last sample would
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

// aggregator collects commulative stats for a stream of values.
type aggregator struct {
	total   int     // total samples processed
	count   int     // samples in current window
	sum     float64 // value sum of current window
	min     float64 // min of current window
	max     float64 // max of current window
	counter float64 // total counter state since beginning
	resets  int     // number of counter resests since beginning
	last    float64 // last added value
}

// Reset the stats to start a new aggregation window.
func (a *aggregator) reset() {
	a.count = 0
	a.sum = 0
	a.min = math.MaxFloat64
	a.max = -math.MaxFloat64
}

func (a *aggregator) add(v float64) {
	if a.total > 0 {
		if v < a.last {
			// Counter Reset, correct the value.
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
	isCounter  bool
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

func (b *aggrChunkBuilder) finalizeChunk(lastT int64, trueSample float64) {
	b.apps[AggrCounter].Append(lastT, trueSample)
}

func (b *aggrChunkBuilder) encode() chunks.Meta {
	return chunks.Meta{
		MinTime: b.mint,
		MaxTime: b.maxt,
		Chunk:   EncodeAggrChunk(b.chunks),
	}
}

// downsampleRaw create a series of aggregation chunks for the given sample data.
func downsampleRaw(data []sample, resolution int64) []chunks.Meta {
	if len(data) == 0 {
		return nil
	}
	var (
		mint, maxt = data[0].t, data[len(data)-1].t
		// We assume a raw resolution of 1 minute. In practice it will often be lower
		// but this is sufficient for our heuristic to produce well-sized chunks.
		numChunks = targetChunkCount(mint, maxt, 1*60*1000, resolution, len(data))
		chks      = make([]chunks.Meta, 0, numChunks)
		batchSize = (len(data) / numChunks) + 1
	)

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

		ab := newAggrChunkBuilder()
		batch := data[:j]
		data = data[j:]

		lastT := downsampleBatch(batch, resolution, ab.add)

		// Finalize the chunk's counter aggregate with the last true sample.
		ab.finalizeChunk(lastT, batch[len(batch)-1].v)

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
func downsampleAggr(chks []AggrChunk, buf *[]sample, mint, maxt, inRes, outRes int64) ([]chunks.Meta, error) {
	// We downsample aggregates only along chunk boundaries. This is required for counters
	// to be downsampled correctly since a chunks' last counter value is the true last value
	// of the original series. We need to preserve it even across multiple aggregation iterations.
	var numSamples int
	for _, c := range chks {
		numSamples += c.NumSamples()
	}
	var (
		numChunks = targetChunkCount(mint, maxt, inRes, outRes, numSamples)
		res       = make([]chunks.Meta, 0, numChunks)
		batchSize = len(chks) / numChunks
	)

	for len(chks) > 0 {
		j := batchSize
		if j > len(chks) {
			j = len(chks)
		}
		part := chks[:j]
		chks = chks[j:]

		chk, err := downsampleAggrBatch(part, buf, outRes)
		if err != nil {
			return nil, err
		}
		res = append(res, chk)
	}
	return res, nil
}

// expandChunkIterator reads all samples from the iterater and appends them to buf.
// Stale markers and out of order samples are skipped.
func expandChunkIterator(it chunkenc.Iterator, buf *[]sample) error {
	// For safety reasons, we check for each sample that it does not go back in time.
	// If it does, we skip it.
	lastT := int64(0)

	for it.Next() {
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

func downsampleAggrBatch(chks []AggrChunk, buf *[]sample, resolution int64) (chk chunks.Meta, err error) {
	ab := &aggrChunkBuilder{}
	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)

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
			if err := expandChunkIterator(c.Iterator(), buf); err != nil {
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

	// Handle counters by reading them properly.
	acs := make([]chunkenc.Iterator, 0, len(chks))
	for _, achk := range chks {
		c, err := achk.Get(AggrCounter)
		if err == ErrAggrNotExist {
			continue
		} else if err != nil {
			return chk, err
		}
		acs = append(acs, c.Iterator())
	}
	*buf = (*buf)[:0]
	it := NewCounterSeriesIterator(acs...)

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

	lastT := downsampleBatch(*buf, resolution, func(t int64, a *aggregator) {
		if t < mint {
			mint = t
		} else if t > maxt {
			maxt = t
		}
		ab.apps[AggrCounter].Append(t, a.counter)
	})
	ab.apps[AggrCounter].Append(lastT, it.lastV)

	ab.mint = mint
	ab.maxt = maxt
	return ab.encode(), nil
}

type sample struct {
	t int64
	v float64
}

type series struct {
	lset   labels.Labels
	chunks []chunks.Meta
}

// CounterSeriesIterator iterates over an ordered sequence of chunks and treats decreasing
// values as counter reset.
// Additionally, it can deal with downsampled counter chunks, which set the last value of a chunk
// to the original last value. The last value can be detected by checking whether the timestamp
// did not increase w.r.t to the previous sample
type CounterSeriesIterator struct {
	chks   []chunkenc.Iterator
	i      int     // current chunk
	total  int     // total number of processed samples
	lastT  int64   // timestamp of the last sample
	lastV  float64 // value of the last sample
	totalV float64 // total counter state since beginning of series
}

func NewCounterSeriesIterator(chks ...chunkenc.Iterator) *CounterSeriesIterator {
	return &CounterSeriesIterator{chks: chks}
}

func (it *CounterSeriesIterator) Next() bool {
	if it.i >= len(it.chks) {
		return false
	}
	if ok := it.chks[it.i].Next(); !ok {
		it.i++
		// While iterators are ordered, they are not generally guaranteed to be
		// non-overlapping. Ensure that the series does not go back in time by seeking at least
		// to the next timestamp.
		return it.Seek(it.lastT + 1)
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
		return true
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
		return true
	}
	// We hit a sample that indicates what the true last value was. For the
	// next chunk we use it to determine whether there was a counter reset between them.
	if t == it.lastT {
		it.lastV = v
	}
	// Otherwise the series went back in time and we just keep moving forward.

	return it.Next()
}

func (it *CounterSeriesIterator) At() (t int64, v float64) {
	return it.lastT, it.totalV
}

func (it *CounterSeriesIterator) Seek(x int64) bool {
	for {
		ok := it.Next()
		if !ok {
			return false
		}
		if t, _ := it.At(); t >= x {
			return true
		}
	}
}

func (it *CounterSeriesIterator) Err() error {
	if it.i >= len(it.chks) {
		return nil
	}
	return it.chks[it.i].Err()
}

// AverageChunkIterator emits an artifical series of average samples based in aggregate
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

func (it *AverageChunkIterator) Next() bool {
	cok, sok := it.cntIt.Next(), it.sumIt.Next()
	if cok != sok {
		it.err = errors.New("sum and count iterator not aligned")
		return false
	}
	if !cok {
		return false
	}

	cntT, cntV := it.cntIt.At()
	sumT, sumV := it.sumIt.At()
	if cntT != sumT {
		it.err = errors.New("sum and count timestamps not aligned")
		return false
	}
	it.t, it.v = cntT, sumV/cntV
	return true
}

func (it *AverageChunkIterator) At() (int64, float64) {
	return it.t, it.v
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
