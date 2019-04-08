package block

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/improbable-eng/thanos/pkg/block/metadata"

	"github.com/prometheus/tsdb/fileutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// IndexCacheFilename is the canonical name for index cache files.
const IndexCacheFilename = "index.cache.json"

type postingsRange struct {
	Name, Value string
	Start, End  int64
}

type indexCache struct {
	Version     int
	Symbols     map[uint32]string
	LabelValues map[string][]string
	Postings    []postingsRange
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) index.ByteSlice {
	return b[start:end]
}

func getSymbolTable(b index.ByteSlice) (map[uint32]string, error) {
	version := int(b.Range(4, 5)[0])

	if version != 1 && version != 2 {
		return nil, errors.Errorf("unknown index file version %d", version)
	}

	toc, err := index.NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	symbolsV2, symbolsV1, err := index.ReadSymbols(b, version, int(toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	symbolsTable := make(map[uint32]string, len(symbolsV1)+len(symbolsV2))
	for o, s := range symbolsV1 {
		symbolsTable[o] = s
	}
	for o, s := range symbolsV2 {
		symbolsTable[uint32(o)] = s
	}

	return symbolsTable, nil
}

// WriteIndexCache writes a cache file containing the first lookup stages
// for an index file.
func WriteIndexCache(logger log.Logger, indexFn string, fn string) error {
	indexFile, err := fileutil.OpenMmapFile(indexFn)
	if err != nil {
		return errors.Wrapf(err, "open mmap index file %s", indexFn)
	}
	defer runutil.CloseWithLogOnErr(logger, indexFile, "close index cache mmap file from %s", indexFn)

	b := realByteSlice(indexFile.Bytes())
	indexr, err := index.NewReader(b)
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithLogOnErr(logger, indexr, "load index cache reader")

	// We assume reader verified index already.
	symbols, err := getSymbolTable(b)
	if err != nil {
		return err
	}

	f, err := os.Create(fn)
	if err != nil {
		return errors.Wrap(err, "create index cache file")
	}
	defer runutil.CloseWithLogOnErr(logger, f, "index cache writer")

	v := indexCache{
		Version:     indexr.Version(),
		Symbols:     symbols,
		LabelValues: map[string][]string{},
	}

	// Extract label value indices.
	lnames, err := indexr.LabelIndices()
	if err != nil {
		return errors.Wrap(err, "read label indices")
	}
	for _, lns := range lnames {
		if len(lns) != 1 {
			continue
		}
		ln := lns[0]

		tpls, err := indexr.LabelValues(ln)
		if err != nil {
			return errors.Wrap(err, "get label values")
		}
		vals := make([]string, 0, tpls.Len())

		for i := 0; i < tpls.Len(); i++ {
			v, err := tpls.At(i)
			if err != nil {
				return errors.Wrap(err, "get label value")
			}
			if len(v) != 1 {
				return errors.Errorf("unexpected tuple length %d", len(v))
			}
			vals = append(vals, v[0])
		}
		v.LabelValues[ln] = vals
	}

	// Extract postings ranges.
	pranges, err := indexr.PostingsRanges()
	if err != nil {
		return errors.Wrap(err, "read postings ranges")
	}
	for l, rng := range pranges {
		v.Postings = append(v.Postings, postingsRange{
			Name:  l.Name,
			Value: l.Value,
			Start: rng.Start,
			End:   rng.End,
		})
	}

	if err := json.NewEncoder(f).Encode(&v); err != nil {
		return errors.Wrap(err, "encode file")
	}
	return nil
}

// ReadIndexCache reads an index cache file.
func ReadIndexCache(logger log.Logger, fn string) (
	version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error,
) {
	f, err := os.Open(fn)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "open file")
	}
	defer runutil.CloseWithLogOnErr(logger, f, "index reader")

	var v indexCache
	if err := json.NewDecoder(f).Decode(&v); err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "decode file")
	}
	strs := map[string]string{}
	lvals = make(map[string][]string, len(v.LabelValues))
	postings = make(map[labels.Label]index.Range, len(v.Postings))

	// Most strings we encounter are duplicates. Dedup string objects that we keep
	// around after the function returns to reduce total memory usage.
	// NOTE(fabxc): it could even make sense to deduplicate globally.
	getStr := func(s string) string {
		if cs, ok := strs[s]; ok {
			return cs
		}
		strs[s] = s
		return s
	}

	for o, s := range v.Symbols {
		v.Symbols[o] = getStr(s)
	}
	for ln, vals := range v.LabelValues {
		for i := range vals {
			vals[i] = getStr(vals[i])
		}
		lvals[getStr(ln)] = vals
	}
	for _, e := range v.Postings {
		l := labels.Label{
			Name:  getStr(e.Name),
			Value: getStr(e.Value),
		}
		postings[l] = index.Range{Start: e.Start, End: e.End}
	}
	return v.Version, v.Symbols, lvals, postings, nil
}

// VerifyIndex does a full run over a block index and verifies that it fulfills the order invariants.
func VerifyIndex(logger log.Logger, fn string, minTime int64, maxTime int64) error {
	stats, err := GatherIndexIssueStats(logger, fn, minTime, maxTime)
	if err != nil {
		return err
	}

	return stats.AnyErr()
}

type Stats struct {
	// TotalSeries represents total number of series in block.
	TotalSeries int
	// OutOfOrderSeries represents number of series that have out of order chunks.
	OutOfOrderSeries int

	// OutOfOrderChunks represents number of chunks that are out of order (older time range is after younger one)
	OutOfOrderChunks int
	// DuplicatedChunks represents number of chunks with same time ranges within same series, potential duplicates.
	DuplicatedChunks int
	// OutsideChunks represents number of all chunks that are before or after time range specified in block meta.
	OutsideChunks int
	// CompleteOutsideChunks is subset of OutsideChunks that will be never accessed. They are completely out of time range specified in block meta.
	CompleteOutsideChunks int
	// Issue347OutsideChunks represents subset of OutsideChunks that are outsiders caused by https://github.com/prometheus/tsdb/issues/347
	// and is something that Thanos handle.
	//
	// Specifically we mean here chunks with minTime == block.maxTime and maxTime > block.MaxTime. These are
	// are segregated into separate counters. These chunks are safe to be deleted, since they are duplicated across 2 blocks.
	Issue347OutsideChunks int
	// OutOfOrderLabels represents the number of postings that contained out
	// of order labels, a bug present in Prometheus 2.8.0 and below.
	OutOfOrderLabels int
}

// PrometheusIssue5372Err returns an error if the Stats object indicates
// postings with out of order labels.  This is corrected by Prometheus Issue
// #5372 and affects Prometheus versions 2.8.0 and below.
func (i Stats) PrometheusIssue5372Err() error {
	if i.OutOfOrderLabels > 0 {
		return errors.Errorf("index contains %d postings with out of order labels",
			i.OutOfOrderLabels)
	}
	return nil
}

// Issue347OutsideChunksErr returns error if stats indicates issue347 block issue, that is repaired explicitly before compaction (on plan block).
func (i Stats) Issue347OutsideChunksErr() error {
	if i.Issue347OutsideChunks > 0 {
		return errors.Errorf("found %d chunks outside the block time range introduced by https://github.com/prometheus/tsdb/issues/347", i.Issue347OutsideChunks)
	}
	return nil
}

// CriticalErr returns error if stats indicates critical block issue, that might solved only by manual repair procedure.
func (i Stats) CriticalErr() error {
	var errMsg []string

	if i.OutOfOrderSeries > 0 {
		errMsg = append(errMsg, fmt.Sprintf(
			"%d/%d series have an average of %.3f out-of-order chunks: "+
				"%.3f of these are exact duplicates (in terms of data and time range)",
			i.OutOfOrderSeries,
			i.TotalSeries,
			float64(i.OutOfOrderChunks)/float64(i.OutOfOrderSeries),
			float64(i.DuplicatedChunks)/float64(i.OutOfOrderChunks),
		))
	}

	n := i.OutsideChunks - (i.CompleteOutsideChunks + i.Issue347OutsideChunks)
	if n > 0 {
		errMsg = append(errMsg, fmt.Sprintf("found %d chunks non-completely outside the block time range", n))
	}

	if i.CompleteOutsideChunks > 0 {
		errMsg = append(errMsg, fmt.Sprintf("found %d chunks completely outside the block time range", i.CompleteOutsideChunks))
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// AnyErr returns error if stats indicates any block issue.
func (i Stats) AnyErr() error {
	var errMsg []string

	if err := i.CriticalErr(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if err := i.Issue347OutsideChunksErr(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if err := i.PrometheusIssue5372Err(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// GatherIndexIssueStats returns useful counters as well as outsider chunks (chunks outside of block time range) that
// helps to assess index health.
// It considers https://github.com/prometheus/tsdb/issues/347 as something that Thanos can handle.
// See Stats.Issue347OutsideChunks for details.
func GatherIndexIssueStats(logger log.Logger, fn string, minTime int64, maxTime int64) (stats Stats, err error) {
	r, err := index.NewFileReader(fn)
	if err != nil {
		return stats, errors.Wrap(err, "open index file")
	}
	defer runutil.CloseWithErrCapture(logger, &err, r, "gather index issue file reader")

	p, err := r.Postings(index.AllPostingsKey())
	if err != nil {
		return stats, errors.Wrap(err, "get all postings")
	}
	var (
		lastLset labels.Labels
		lset     labels.Labels
		chks     []chunks.Meta
	)

	// Per series.
	for p.Next() {
		lastLset = append(lastLset[:0], lset...)

		id := p.At()
		stats.TotalSeries++

		if err := r.Series(id, &lset, &chks); err != nil {
			return stats, errors.Wrap(err, "read series")
		}
		if len(lset) == 0 {
			return stats, errors.Errorf("empty label set detected for series %d", id)
		}
		if lastLset != nil && labels.Compare(lastLset, lset) >= 0 {
			return stats, errors.Errorf("series %v out of order; previous %v", lset, lastLset)
		}
		l0 := lset[0]
		for _, l := range lset[1:] {
			if l.Name < l0.Name {
				stats.OutOfOrderLabels++
				level.Warn(logger).Log("msg",
					"out-of-order label set: known bug in Prometheus 2.8.0 and below",
					"labelset", fmt.Sprintf("%s", lset),
					"series", fmt.Sprintf("%d", id),
				)
			}
			l0 = l
		}
		if len(chks) == 0 {
			return stats, errors.Errorf("empty chunks for series %d", id)
		}

		ooo := 0
		// Per chunk in series.
		for i, c := range chks {
			// Chunk vs the block ranges.
			if c.MinTime < minTime || c.MaxTime > maxTime {
				stats.OutsideChunks++
				if c.MinTime > maxTime || c.MaxTime < minTime {
					stats.CompleteOutsideChunks++
				} else if c.MinTime == maxTime {
					stats.Issue347OutsideChunks++
				}
			}

			if i == 0 {
				continue
			}

			c0 := chks[i-1]

			// Chunk order within block.
			if c.MinTime > c0.MaxTime {
				continue
			}

			if c.MinTime == c0.MinTime && c.MaxTime == c0.MaxTime {
				// TODO(bplotka): Calc and check checksum from chunks itself.
				// The chunks can overlap 1:1 in time, but does not have same data.
				// We assume same data for simplicity, but it can be a symptom of error.
				stats.DuplicatedChunks++
				continue
			}
			// Chunks partly overlaps or out of order.
			ooo++
		}
		if ooo > 0 {
			stats.OutOfOrderSeries++
			stats.OutOfOrderChunks += ooo
		}
	}
	if p.Err() != nil {
		return stats, errors.Wrap(err, "walk postings")
	}

	return stats, nil
}

type ignoreFnType func(mint, maxt int64, prev *chunks.Meta, curr *chunks.Meta) (bool, error)

// Repair open the block with given id in dir and creates a new one with fixed data.
// It:
// - removes out of order duplicates
// - all "complete" outsiders (they will not accessed anyway)
// - removes all near "complete" outside chunks introduced by https://github.com/prometheus/tsdb/issues/347.
// Fixable inconsistencies are resolved in the new block.
// TODO(bplotka): https://github.com/improbable-eng/thanos/issues/378
func Repair(logger log.Logger, dir string, id ulid.ULID, source metadata.SourceType, ignoreChkFns ...ignoreFnType) (resid ulid.ULID, err error) {
	if len(ignoreChkFns) == 0 {
		return resid, errors.New("no ignore chunk function specified")
	}

	bdir := filepath.Join(dir, id.String())
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	resid = ulid.MustNew(ulid.Now(), entropy)

	meta, err := metadata.Read(bdir)
	if err != nil {
		return resid, errors.Wrap(err, "read meta file")
	}
	if meta.Thanos.Downsample.Resolution > 0 {
		return resid, errors.New("cannot repair downsampled block")
	}

	b, err := tsdb.OpenBlock(logger, bdir, nil)
	if err != nil {
		return resid, errors.Wrap(err, "open block")
	}
	defer runutil.CloseWithErrCapture(logger, &err, b, "repair block reader")

	indexr, err := b.Index()
	if err != nil {
		return resid, errors.Wrap(err, "open index")
	}
	defer runutil.CloseWithErrCapture(logger, &err, indexr, "repair index reader")

	chunkr, err := b.Chunks()
	if err != nil {
		return resid, errors.Wrap(err, "open chunks")
	}
	defer runutil.CloseWithErrCapture(logger, &err, chunkr, "repair chunk reader")

	resdir := filepath.Join(dir, resid.String())

	chunkw, err := chunks.NewWriter(filepath.Join(resdir, ChunksDirname))
	if err != nil {
		return resid, errors.Wrap(err, "open chunk writer")
	}
	defer runutil.CloseWithErrCapture(logger, &err, chunkw, "repair chunk writer")

	indexw, err := index.NewWriter(filepath.Join(resdir, IndexFilename))
	if err != nil {
		return resid, errors.Wrap(err, "open index writer")
	}
	defer runutil.CloseWithErrCapture(logger, &err, indexw, "repair index writer")

	// TODO(fabxc): adapt so we properly handle the version once we update to an upstream
	// that has multiple.
	resmeta := *meta
	resmeta.ULID = resid
	resmeta.Stats = tsdb.BlockStats{} // reset stats
	resmeta.Thanos.Source = source    // update source

	if err := rewrite(indexr, chunkr, indexw, chunkw, &resmeta, ignoreChkFns); err != nil {
		return resid, errors.Wrap(err, "rewrite block")
	}
	if err := metadata.Write(logger, resdir, &resmeta); err != nil {
		return resid, err
	}
	return resid, nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func IgnoreCompleteOutsideChunk(mint int64, maxt int64, _ *chunks.Meta, curr *chunks.Meta) (bool, error) {
	if curr.MinTime > maxt || curr.MaxTime < mint {
		// "Complete" outsider. Ignore.
		return true, nil
	}
	return false, nil
}

func IgnoreIssue347OutsideChunk(_ int64, maxt int64, _ *chunks.Meta, curr *chunks.Meta) (bool, error) {
	if curr.MinTime == maxt {
		// "Near" outsider from issue https://github.com/prometheus/tsdb/issues/347. Ignore.
		return true, nil
	}
	return false, nil
}

func IgnoreDuplicateOutsideChunk(_ int64, _ int64, last *chunks.Meta, curr *chunks.Meta) (bool, error) {
	if last == nil {
		return false, nil
	}

	if curr.MinTime > last.MaxTime {
		return false, nil
	}

	// Verify that the overlapping chunks are exact copies so we can safely discard
	// the current one.
	if curr.MinTime != last.MinTime || curr.MaxTime != last.MaxTime {
		return false, errors.Errorf("non-sequential chunks not equal: [%d, %d] and [%d, %d]",
			last.MaxTime, last.MaxTime, curr.MinTime, curr.MaxTime)
	}
	ca := crc32.Checksum(last.Chunk.Bytes(), castagnoli)
	cb := crc32.Checksum(curr.Chunk.Bytes(), castagnoli)

	if ca != cb {
		return false, errors.Errorf("non-sequential chunks not equal: %x and %x", ca, cb)
	}

	return true, nil
}

// sanitizeChunkSequence ensures order of the input chunks and drops any duplicates.
// It errors if the sequence contains non-dedupable overlaps.
func sanitizeChunkSequence(chks []chunks.Meta, mint int64, maxt int64, ignoreChkFns []ignoreFnType) ([]chunks.Meta, error) {
	if len(chks) == 0 {
		return nil, nil
	}
	// First, ensure that chunks are ordered by their start time.
	sort.Slice(chks, func(i, j int) bool {
		return chks[i].MinTime < chks[j].MinTime
	})

	// Remove duplicates, complete outsiders and near outsiders.
	repl := make([]chunks.Meta, 0, len(chks))
	var last *chunks.Meta

OUTER:
	for _, c := range chks {
		for _, ignoreChkFn := range ignoreChkFns {
			ignore, err := ignoreChkFn(mint, maxt, last, &c)
			if err != nil {
				return nil, errors.Wrap(err, "ignore function")
			}

			if ignore {
				continue OUTER
			}
		}

		last = &c
		repl = append(repl, c)
	}

	return repl, nil
}

// rewrite writes all data from the readers back into the writers while cleaning
// up mis-ordered and duplicated chunks.
func rewrite(
	indexr tsdb.IndexReader, chunkr tsdb.ChunkReader,
	indexw tsdb.IndexWriter, chunkw tsdb.ChunkWriter,
	meta *metadata.Meta,
	ignoreChkFns []ignoreFnType,
) error {
	symbols, err := indexr.Symbols()
	if err != nil {
		return err
	}
	if err := indexw.AddSymbols(symbols); err != nil {
		return err
	}

	all, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return err
	}
	all = indexr.SortedPostings(all)

	// We fully rebuild the postings list index from merged series.
	var (
		postings = index.NewMemPostings()
		values   = map[string]stringset{}
		i        = uint64(0)
	)

	var lset labels.Labels
	var chks []chunks.Meta

	for all.Next() {
		id := all.At()

		if err := indexr.Series(id, &lset, &chks); err != nil {
			return err
		}
		for i, c := range chks {
			chks[i].Chunk, err = chunkr.Chunk(c.Ref)
			if err != nil {
				return err
			}
		}

		chks, err := sanitizeChunkSequence(chks, meta.MinTime, meta.MaxTime, ignoreChkFns)
		if err != nil {
			return err
		}

		if len(chks) == 0 {
			continue
		}

		if err := chunkw.WriteChunks(chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		if err := indexw.AddSeries(i, lset, chks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		meta.Stats.NumChunks += uint64(len(chks))
		meta.Stats.NumSeries++

		for _, chk := range chks {
			meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}

		for _, l := range lset {
			valset, ok := values[l.Name]
			if !ok {
				valset = stringset{}
				values[l.Name] = valset
			}
			valset.set(l.Value)
		}
		postings.Add(i, lset)
		i++
	}
	if all.Err() != nil {
		return errors.Wrap(all.Err(), "iterate series")
	}

	s := make([]string, 0, 256)
	for n, v := range values {
		s = s[:0]

		for x := range v {
			s = append(s, x)
		}
		if err := indexw.WriteLabelIndex([]string{n}, s); err != nil {
			return errors.Wrap(err, "write label index")
		}
	}

	for _, l := range postings.SortedKeys() {
		if err := indexw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}
	return nil
}

type stringset map[string]struct{}

func (ss stringset) set(s string) {
	ss[s] = struct{}{}
}

func (ss stringset) has(s string) bool {
	_, ok := ss[s]
	return ok
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}
