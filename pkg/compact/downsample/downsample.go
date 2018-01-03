package compact

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/prometheus/tsdb/chunkenc"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// Downsample downsamples the given block. It writes a new block into dir and returns its ID.
func Downsample(ctx context.Context, b *tsdb.Block, dir string, window int64) (id ulid.ULID, err error) {
	origMeta, err := block.ReadMetaFile(b.Dir())
	if err != nil {
		return id, errors.Wrap(err, "read block meta")
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

	// Write downsampled data in a custom memory block where we have fine-grained control
	// over created chunks.
	// This is necessary since we need to inject special values at the end of chunks for
	// some aggregations.
	newb := newMemBlock()

	pall, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return id, errors.Wrap(err, "get all postings list")
	}
	var all []sample
	var chunked [][]sample

	for i := 0; pall.Next() && i < 3000; i++ {
		var lset labels.Labels
		var chks []chunks.Meta

		// Get series labels and chunks. Downsampled data is sensitive to chunk boundaries
		// and we need to preserve them to properly downsample previously downsampled data.
		if err := indexr.Series(pall.At(), &lset, &chks); err != nil {
			return id, errors.Wrapf(err, "get series %d", pall.At())
		}
		for _, c := range chks {
			chk, err := chunkr.Chunk(c.Ref)
			if err != nil {
				return id, errors.Wrapf(err, "get chunk %d", c.Ref)
			}
			k := len(all)
			it := chk.Iterator()

			for it.Next() {
				t, v := it.At()
				all = append(all, sample{t, v})
			}
			if it.Err() != nil {
				return id, errors.Wrapf(it.Err(), "expand chunk %d", c.Ref)
			}
			chunked = append(chunked, all[k:])
		}
		// fmt.Println(i, pall.At())
		// Raw and already downsampled data need different processing.
		if origMeta.Thanos.DownsamplingWindow == 0 {
			for _, s := range downsampleRaw(lset, all, window) {
				newb.addSeries(s)
			}
		} else {
			s, err := downsampleAggr(lset, all, chunked, window)
			if err != nil {
				return id, errors.Wrap(err, "downsample aggregate block")
			}
			newb.addSeries(s)
		}
	}
	if pall.Err() != nil {
		return id, errors.Wrap(pall.Err(), "iterate series set")
	}
	rng := b.Meta().MaxTime - b.Meta().MinTime
	comp, err := tsdb.NewLeveledCompactor(nil, log.NewNopLogger(), []int64{rng}, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}
	id, err = comp.Write(dir, newb, b.Meta().MinTime, b.Meta().MaxTime)
	if err != nil {
		return id, errors.Wrap(err, "compact head")
	}
	bdir := filepath.Join(dir, id.String())

	meta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return id, errors.Wrap(err, "read block meta")
	}
	meta.Thanos.Labels = origMeta.Thanos.Labels
	meta.Thanos.DownsamplingWindow = window
	meta.Compaction = origMeta.Compaction

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

func aggrLset(lset labels.Labels, aggr string) labels.Labels {
	res := make(labels.Labels, len(lset))
	copy(res, lset)

	for i, l := range res {
		if l.Name == "__name__" {
			res[i].Value = fmt.Sprintf("%s$%s", l.Value, aggr)
		}
	}
	return res
}

func isCounter(lset labels.Labels) bool {
	metric := lset.Get("__name__")
	return strings.HasSuffix(metric, "_total") ||
		strings.HasSuffix(metric, "_bucket") ||
		strings.HasSuffix(metric, "_sum")
}

func targetChunkSize(l int) int {
	c := 1
	for ; l/c > 250; c++ {
	}
	return l / c
}

type sample struct {
	t int64
	v float64
}

// series is a mutable series that creates XOR chunk with a configurable target amount
// of samples per chunk.
type series struct {
	lset   labels.Labels
	chunks []chunks.Meta

	l, n       int
	cur        chunkenc.Chunk
	app        chunkenc.Appender
	cmin, cmax int64
}

// newSeries returns a new series and automatically cuts a new chunk after n samples if
// n is greater than 0.
func newSeries(lset labels.Labels, n int) *series {
	return &series{lset: lset, n: n}
}

// newSeries returns a new series and automatically cuts a new chunk after n samples if
// n is greater than 0.
// It extends the metric name of the label set by the aggregation modifier.
func newAggrSeries(lset labels.Labels, aggr string, n int) *series {
	return &series{lset: aggrLset(lset, aggr), n: n}
}

func (s *series) add(t int64, v float64) {
	if s.n > 0 && s.l >= s.n {
		s.cut()
	}
	if s.cur == nil {
		s.cur = chunkenc.NewXORChunk()
		s.cmin = t
		s.app, _ = s.cur.Appender()
	}
	s.app.Append(t, v)
	s.cmax = t
}

func (s *series) cut() {
	s.chunks = append(s.chunks, chunks.Meta{
		MinTime: s.cmin,
		MaxTime: s.cmax,
		Chunk:   s.cur,
	})
	s.cur = nil
}

func (s *series) close() {
	if s.cur == nil {
		return
	}
	s.chunks = append(s.chunks, chunks.Meta{
		MinTime: s.cmin,
		MaxTime: s.cmax,
		Chunk:   s.cur,
	})
}

type listSeriesIterator struct {
	l []sample
	i int
}

func newListSeriesIterator(l []sample) *listSeriesIterator {
	return &listSeriesIterator{l: l, i: -1}
}

func (it *listSeriesIterator) Err() error {
	return nil
}

func (it *listSeriesIterator) Next() bool {
	if it.i >= len(it.l)-1 {
		return false
	}
	it.i++
	return true
}

func (it *listSeriesIterator) Seek(int64) bool {
	panic("unexpected")
}

func (it *listSeriesIterator) At() (t int64, v float64) {
	return it.l[it.i].t, it.l[it.i].v
}

func downsampleCounter(ser *series, data []sample, window int64) {
	n := targetChunkSize(len(data))
	nextT := int64(-1)
	var last float64
	var counter float64

	for i, s := range data {
		if s.t > nextT {
			if i > 0 && i%n == 0 {
				ser.add(nextT, last)
				ser.cut()
			}
			if nextT != -1 {
				ser.add(nextT, counter)
			}
			nextT = s.t - (s.t % window) + window - 1
		}
		if i == 0 {
			// First sample sets the counter.
			counter = s.v
		} else {
			if s.v < last {
				// Counter reset, correct the value.
				counter += s.v
			} else {
				// Add delta with last value to the counter.
				counter += s.v - last
			}
		}
		last = s.v
	}
	ser.add(nextT, counter)
	ser.add(nextT, last)
}

func downsampleCounterCounter(ser *series, all []sample, chunked [][]sample, window int64) {
	// To further downsample downsampled counter aggregates, we have to preserve chunk boundaries since
	// they contain a true last sample at the very end.
	// To provide a more downsampled chunk with a last sample, we must make it end at one of the input chunks.
	k := (len(chunked) / len(all) / targetChunkSize(len(all))) + 1

	nextT := int64(-1)
	var last float64
	var counter float64

	for len(chunked) > 0 {
		j := k
		if k > len(chunked) {
			k = len(chunked)
		}
		part := chunked[:j]
		chunked = chunked[j:]

		for i, p := range part {
			for _, s := range p {
				if s.t > nextT {
					if nextT != -1 {
						ser.add(nextT, counter)
					}
					nextT = s.t - (s.t % window) + window - 1
				}
				if i == 0 {
					// First sample sets the counter.
					counter = s.v
				} else {
					if s.v < last {
						// Counter reset, correct the value.
						counter += s.v
					} else {
						// Add delta with last value to the counter.
						counter += s.v - last
					}
				}
				last = s.v
			}
			ser.add(nextT, last)
			ser.cut()
		}
	}
}

func downsampleCount(ser *series, data []sample, window int64) {
	nextT := int64(-1)
	var count int

	for _, s := range data {
		if s.t > nextT {
			if nextT != -1 {
				ser.add(nextT, float64(count))
			}
			count = 0
			nextT = s.t - (s.t % window) + window - 1
		}
		count++
	}
	ser.add(nextT, float64(count))
}

func downsampleSum(ser *series, data []sample, window int64) {
	nextT := int64(-1)
	var sum float64

	for _, s := range data {
		if s.t > nextT {
			if nextT != -1 {
				ser.add(nextT, sum)
			}
			sum = 0
			nextT = s.t - (s.t % window) + window - 1
		}
		sum += s.v
	}
	ser.add(nextT, sum)
}

func downsampleMin(ser *series, data []sample, window int64) {
	nextT := int64(-1)
	min := math.MaxFloat64

	for _, s := range data {
		if s.t > nextT {
			if nextT != -1 {
				ser.add(nextT, min)
			}
			min = math.MaxFloat64
			nextT = s.t - (s.t % window) + window - 1
		}
		if s.v < min {
			min = s.v
		}
	}
	ser.add(nextT, min)
}

func downsampleMax(ser *series, data []sample, window int64) {
	nextT := int64(-1)
	max := -math.MaxFloat64

	for _, s := range data {
		if s.t > nextT {
			if nextT != -1 {
				ser.add(nextT, max)
			}
			max = -math.MaxFloat64
			nextT = s.t - (s.t % window) + window - 1
		}
		if s.v > max {
			max = s.v
		}
	}
	ser.add(nextT, max)
}

func downsampleRaw(lset labels.Labels, data []sample, window int64) []*series {
	if len(data) == 0 {
		return nil
	}
	n := targetChunkSize(len(data))

	countSeries := newAggrSeries(lset, "count", n)
	sumSeries := newAggrSeries(lset, "sum", n)
	minSeries := newAggrSeries(lset, "min", n)
	maxSeries := newAggrSeries(lset, "max", n)
	counterSeries := newAggrSeries(lset, "counter", 0)

	var aggr aggregator

	nextT := int64(-1)

	gauge := !isCounter(lset)

	var t int64
	var v float64

	addAll := func(t int64) {
		if gauge {
			sumSeries.add(t, aggr.sum)
			minSeries.add(t, aggr.min)
			maxSeries.add(t, aggr.max)
		}
		countSeries.add(t, float64(aggr.count))
		counterSeries.add(t, aggr.counter)
	}

	for _, s := range data {
		t, v = s.t, s.v
		if t > nextT {
			if nextT != -1 {
				addAll(nextT)
			}
			aggr.reset()
			nextT = t - (t % window) + window - 1
		}
		aggr.add(v)
	}
	counterSeries.add(nextT, v)

	// if !isCounter(lset) {
	// 	downsampleSum(sumSeries, data, window)
	// 	downsampleMin(minSeries, data, window)
	// 	downsampleMax(maxSeries, data, window)
	// }
	// downsampleCount(countSeries, data, window)
	// downsampleCounter(counterSeries, data, window)

	res := []*series{countSeries, counterSeries}
	if gauge {
		res = append(res, sumSeries, minSeries, maxSeries)
	}
	for _, s := range res {
		s.close()
	}
	return res
}

func downsampleAggr(lset labels.Labels, all []sample, chunked [][]sample, window int64) (*series, error) {
	if len(all) == 0 {
		return nil, nil
	}
	metric := lset.Get("__name__")
	ser := newSeries(lset, targetChunkSize(len(all))+1)

	if strings.HasSuffix(metric, "$sum") {
		downsampleSum(ser, all, window)
	} else if strings.HasSuffix(metric, "$count") {
		// Downsampling a count aggregate is equivalent to downsampling a sum.
		downsampleSum(ser, all, window)
	} else if strings.HasSuffix(metric, "$min") {
		downsampleMin(ser, all, window)
	} else if strings.HasSuffix(metric, "$max") {
		downsampleMax(ser, all, window)
	} else if strings.HasSuffix(metric, "$counter") {
		downsampleCounterCounter(ser, all, chunked, window)
	} else {
		return nil, errors.Errorf("unknown aggregate metric %q", metric)
	}
	return nil, nil
}

// countChunkSeriesIterator iterates over an ordered sequence of chunks and treats decreasing
// values as counter reset.
// Additionally, it can deal with downsampled counter chunks, which set the last value of a chunk
// to the original last value. The last value can be detected by checking whether the timestamp
// did not increase w.r.t to the previous sample
type countChunkSeriesIterator struct {
	chks   []chunkenc.Iterator
	i      int     // current chunk
	total  int     // total number of processed samples
	lastT  int64   // timestamp of the last sample
	lastV  float64 // value of the last sample
	totalV float64 // total counter state since beginning of series
}

func (it *countChunkSeriesIterator) LastSample() (int64, float64) {
	return it.lastT, it.lastV
}

func (it *countChunkSeriesIterator) Next() bool {
	if it.i >= len(it.chks) {
		return false
	}
	// Chunk ends without special sample. It was not a downsampled counter series.
	if ok := it.chks[it.i].Next(); !ok {
		it.i++
		return it.Next()
	}
	t, v := it.chks[it.i].At()
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
	// We hit the last sample that indicates what the true last value was. For the
	// next chunk we use it to determine whether there was a counter reset between them.
	it.lastT, it.lastV = t, v
	it.i++

	return it.Next()
}

func (it *countChunkSeriesIterator) At() (t int64, v float64) {
	return it.lastT, it.totalV
}

func (it *countChunkSeriesIterator) Seek(x int64) bool {
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

func (it *countChunkSeriesIterator) Err() error {
	if it.i >= len(it.chks) {
		return nil
	}
	return it.chks[it.i].Err()
}

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

func (a *aggregator) get() float64 {
	return a.sum / float64(a.count)
}
