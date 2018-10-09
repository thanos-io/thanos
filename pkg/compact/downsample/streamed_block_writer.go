package downsample

import (
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

type symbols map[string]struct{}

type labelValues map[string]struct{}

func (lv labelValues) add(value string) {
	lv[value] = struct{}{}
}

func (lv labelValues) get(set *[]string) {
	for value := range lv {
		*set = append(*set, value)
	}
}

type labelsValues map[string]labelValues

func (lv labelsValues) add(labelSet labels.Labels) {
	for _, label := range labelSet {
		values, ok := lv[label.Name]
		if !ok {
			// Add new label.
			values = labelValues{}
			lv[label.Name] = values
		}
		values.add(label.Value)
	}
}

// StreamedBlockWriter writes downsampled blocks to a new data block. Implemented to save memory consumption
// by means writing chunks data right into the files, omitting keeping them in-memory. Index and meta data should be
// flushed afterwards, when there aren't more series to process.
type StreamedBlockWriter struct {
	dir    string
	tmpDir string
	logger log.Logger
	uid    ulid.ULID

	symbols  symbols
	postings []uint64
	series   []*series

	chunkWriter  tsdb.ChunkWriter
	meta         block.Meta
	totalChunks  uint64
	totalSamples uint64
}

// NewWriter returns StreamedBlockWriter instance. Caller is responsible to finalize the writing with Flush method to write
// the meta and index file and Close all io.Closers
func NewWriter(dir string, l log.Logger, originMeta block.Meta, resolution int64) (*StreamedBlockWriter, error) {
	var err error
	var chunkWriter tsdb.ChunkWriter

	// Generate new block id.
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	// Populate chunk, meta and index files into temporary directory with
	// data of all blocks.
	dir = filepath.Join(dir, uid.String())
	tmpDir, err := createTmpDir(dir)
	if err != nil {
		return nil, err
	}

	chunkWriter, err = chunks.NewWriter(filepath.Join(tmpDir, block.ChunksDirname))
	if err != nil {
		return nil, errors.Wrap(err, "create tmp chunk StreamedBlockWriter")
	}

	originMeta.Thanos.Downsample.Resolution = resolution

	return &StreamedBlockWriter{
		logger:      l,
		dir:         dir,
		tmpDir:      tmpDir,
		symbols:     symbols{},
		chunkWriter: chunkWriter,
		uid:         uid,
		meta:        originMeta,
	}, nil
}

func (w *StreamedBlockWriter) AddSeries(s *series) error {
	if w.chunkWriter == nil {
		panic("Series can't be added, ChunkWriter has been closed")
	}
	if len(s.chunks) == 0 {
		level.Warn(w.logger).Log("empty chunks happened", s.lset)
	}

	if err := w.chunkWriter.WriteChunks(s.chunks...); err != nil {
		return errors.Wrap(err, "add series")
	}

	w.postings = append(w.postings, uint64(len(w.series)))
	w.series = append(w.series, s)

	for _, l := range s.lset {
		w.symbols[l.Name] = struct{}{}
		w.symbols[l.Value] = struct{}{}
	}

	w.totalChunks += uint64(len(s.chunks))
	for i := range s.chunks {
		chk := &s.chunks[i]
		w.totalSamples += uint64(chk.Chunk.NumSamples())
		chk.Chunk = nil
	}

	return nil
}

// Flush saves prepared index and meta data to corresponding files.
// Be sure to call this, if all series have to be handled by this moment, as
func (w *StreamedBlockWriter) Flush() (ulid.ULID, error) {
	var err error

	if err := w.chunkWriter.Close(); err != nil {
		return w.uid, errors.Wrap(err, "close chunk writer")
	}
	w.chunkWriter = nil

	indexw, err := index.NewWriter(filepath.Join(w.tmpDir, block.IndexFilename))
	if err != nil {
		return w.uid, errors.Wrap(err, "open index StreamedBlockWriter")
	}

	defer func() {
		if indexw != nil {
			if err := indexw.Close(); err != nil {
				level.Error(w.logger).Log(err, "close index StreamedBlockWriter")
			}
		}
	}()

	if err := w.populateBlock(indexw); err != nil {
		return w.uid, errors.Wrap(err, "write compaction")
	}

	if err = w.writeMetaFile(w.tmpDir); err != nil {
		return w.uid, errors.Wrap(err, "write merged meta")
	}

	if err = indexw.Close(); err != nil {
		return w.uid, errors.Wrap(err, "close index StreamedBlockWriter")
	}
	indexw = nil

	df, err := fileutil.OpenDir(w.tmpDir)
	if err != nil {
		return w.uid, errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			if err := df.Close(); err != nil {
				log.Logger(w.logger).Log(err, "close temporary block dir")
			}
		}
	}()

	if err := fileutil.Fsync(df); err != nil {
		return w.uid, errors.Wrap(err, "sync temporary dir")
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return w.uid, errors.Wrap(err, "close temporary dir")
	}
	df = nil

	// Block successfully written, make visible and remove old ones.
	err = renameFile(w.tmpDir, w.dir)
	// Assume we cleaned tmp dir up
	w.tmpDir = ""
	if err != nil {
		return w.uid, errors.Wrap(err, "rename block dir")
	}

	level.Info(w.logger).Log(
		"msg", "write downsampled block",
		"mint", w.meta.MinTime,
		"maxt", w.meta.MaxTime,
		"ulid", w.meta.ULID,
		"resolution", w.meta.Thanos.Downsample.Resolution,
	)
	return w.uid, nil
}

// populateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
func (w *StreamedBlockWriter) populateBlock(indexWriter tsdb.IndexWriter) error {
	var (
		i            = uint64(0)
		labelsValues = labelsValues{}
		memPostings  = index.NewUnorderedMemPostings()
	)

	if err := indexWriter.AddSymbols(w.symbols); err != nil {
		return errors.Wrap(err, "add symbols")
	}

	sort.Slice(w.postings, func(i, j int) bool {
		return labels.Compare(w.series[w.postings[i]].lset, w.series[w.postings[j]].lset) < 0
	})

	all := index.NewListPostings(w.postings)
	for all.Next() {
		s := w.series[i]
		// Skip the series with all deleted chunks.
		if len(s.chunks) == 0 {
			level.Info(w.logger).Log("empty chunks", i, s.lset)
			continue
		}

		if err := indexWriter.AddSeries(uint64(i), s.lset, s.chunks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		labelsValues.add(s.lset)
		memPostings.Add(i, s.lset)
		i++
	}

	s := make([]string, 0, 256)
	for n, v := range labelsValues {
		s = s[:0]
		v.get(&s)
		if err := indexWriter.WriteLabelIndex([]string{n}, s); err != nil {
			return errors.Wrap(err, "write label index")
		}
	}

	memPostings.EnsureOrder()

	for _, l := range memPostings.SortedKeys() {
		if err := indexWriter.WritePostings(l.Name, l.Value, memPostings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}
	return nil
}

// TODO probably tsdb.BlockMeta should expose method writeToFile /w encode.
func (w *StreamedBlockWriter) writeMetaFile(dest string) error {
	w.meta.ULID = w.uid
	w.meta.Version = 1
	w.meta.Thanos.Source = block.CompactorSource
	w.meta.Stats.NumChunks = w.totalChunks
	w.meta.Stats.NumSamples = w.totalSamples
	w.meta.Stats.NumSeries = uint64(len(w.series))

	// Make any changes to the file appear atomic.
	path := filepath.Join(dest, block.MetaFilename)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return errors.Wrapf(err, "create tmp meta file %s", tmp)
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	var merr tsdb.MultiError

	if merr.Add(enc.Encode(w.meta)); merr.Err() != nil {
		merr.Add(f.Close())
		return errors.Wrapf(merr.Err(), "encoding meta file to json %s", tmp)
	}
	if err := f.Close(); err != nil {
		return errors.Wrapf(err, "close tmp meta file %s", tmp)
	}

	if err := renameFile(tmp, path); err != nil {
		return errors.Wrapf(err, "rename tmp meta file %s", tmp)
	}

	return nil
}

func (w *StreamedBlockWriter) Close() error {
	var merr tsdb.MultiError

	if w.tmpDir != "" {
		merr.Add(os.RemoveAll(w.tmpDir))
	}

	if w.chunkWriter != nil {
		merr.Add(w.chunkWriter.Close())
		w.chunkWriter = nil
	}

	if merr.Err() != nil {
		return errors.Wrap(merr.Err(), "close chunk writer")
	}
	return nil
}

func renameFile(from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	var merr tsdb.MultiError
	merr.Add(fileutil.Fsync(pdir))
	merr.Add(pdir.Close())
	return merr.Err()
}

func createTmpDir(parent string) (string, error) {
	tmp := parent + ".tmp"

	if err := os.RemoveAll(tmp); err != nil {
		return "", errors.Wrap(err, "removing tmp dir")
	}

	if err := os.MkdirAll(tmp, 0777); err != nil {
		return "", errors.Wrap(err, "mkdir tmp dir")
	}

	return tmp, nil
}
