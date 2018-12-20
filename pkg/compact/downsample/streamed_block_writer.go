package downsample

import (
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path/filepath"
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
// sealed afterwards, when there aren't more series to process.
type StreamedBlockWriter struct {
	dir    string
	tmpDir string
	logger log.Logger
	uid    ulid.ULID

	// postings is a current posting position.
	postings uint64

	chunkWriter tsdb.ChunkWriter
	indexWriter tsdb.IndexWriter
	indexReader tsdb.IndexReader
	closers     []io.Closer

	meta         block.Meta
	totalChunks  uint64
	totalSamples uint64

	// labelsValues list of used label sets: name -> []values.
	labelsValues labelsValues

	// memPostings contains references from label name:value -> postings.
	memPostings *index.MemPostings
	sealed      bool
}

// NewWriter returns StreamedBlockWriter instance.
// Caller is responsible to finalize the writing with Flush method to write the meta and index file and Close all io.Closers
func NewWriter(dir string, indexReader tsdb.IndexReader, l log.Logger, originMeta block.Meta, resolution int64) (*StreamedBlockWriter, error) {
	var err error

	// change downsampling resolution to the new one.
	originMeta.Thanos.Downsample.Resolution = resolution

	// Generate new block id.
	uid := ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))

	// Populate chunk, meta and index files into temporary directory with
	// data of all blocks.
	dir = filepath.Join(dir, uid.String())
	tmpDir, err := createTmpDir(dir)
	if err != nil {
		return nil, err
	}

	sw := &StreamedBlockWriter{
		logger:       l,
		dir:          dir,
		indexReader:  indexReader,
		tmpDir:       tmpDir,
		uid:          uid,
		meta:         originMeta,
		closers:      make([]io.Closer, 0),
		labelsValues: make(labelsValues, 1024),
		memPostings:  index.NewUnorderedMemPostings(),
	}

	sw.chunkWriter, err = chunks.NewWriter(filepath.Join(tmpDir, block.ChunksDirname))
	if err != nil {
		return nil, errors.Wrap(err, "create tmp chunk StreamedBlockWriter")
	}
	sw.closers = append(sw.closers, sw.chunkWriter)

	sw.indexWriter, err = index.NewWriter(filepath.Join(tmpDir, block.IndexFilename))
	if err != nil {
		return nil, errors.Wrap(err, "open index StreamedBlockWriter")
	}
	sw.closers = append(sw.closers, sw.indexWriter)

	if err := sw.init(); err != nil {
		return nil, err
	}

	return sw, nil
}

// AddSeries writes chunks data to the chunkWriter, writes lset and chunks Metas to indexWrites and adds label sets to
// labelsValues sets and memPostings to be written on the Flush state in the end of downsampling process.
func (w *StreamedBlockWriter) AddSeries(lset labels.Labels, chunks []chunks.Meta) error {
	if w.sealed {
		panic("Series can't be added, writers has been flushed|closed")
	}

	if len(chunks) == 0 {
		level.Warn(w.logger).Log("empty chunks happened, skip series", lset)
		return nil
	}

	if err := w.chunkWriter.WriteChunks(chunks...); err != nil {
		return errors.Wrap(err, "add series")
	}

	if err := w.indexWriter.AddSeries(w.postings, lset, chunks...); err != nil {
		return errors.Wrap(err, "add series")
	}

	w.labelsValues.add(lset)
	w.memPostings.Add(w.postings, lset)
	w.postings++

	w.totalChunks += uint64(len(chunks))
	for i := range chunks {
		w.totalSamples += uint64(chunks[i].Chunk.NumSamples())
	}

	return nil
}

// Flush saves prepared index and meta data to corresponding files.
// Be sure to call this, if all series have to be handled by this moment, you can't call AddSeries afterwards.
func (w *StreamedBlockWriter) Flush() (ulid.ULID, error) {
	var err error
	w.sealed = true

	if err = w.writeLabelSets(); err != nil {
		return w.uid, errors.Wrap(err, "write label sets")
	}

	if err = w.writeMemPostings(); err != nil {
		return w.uid, errors.Wrap(err, "write mem postings")
	}

	if err = w.writeMetaFile(); err != nil {
		return w.uid, errors.Wrap(err, "write meta meta")
	}

	if err = w.finalize(); err != nil {
		return w.uid, errors.Wrap(err, "sync and rename tmp dir")
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

// Close closes all io.CLoser writers
func (w *StreamedBlockWriter) Close() error {
	var merr tsdb.MultiError
	w.sealed = true

	if w.tmpDir != "" {
		merr.Add(os.RemoveAll(w.tmpDir))
	}

	for _, cl := range w.closers {
		merr.Add(cl.Close())
	}

	w.chunkWriter = nil
	w.indexWriter = nil

	return errors.Wrap(merr.Err(), "close closers")
}

// init writes all available symbols in the beginning of the index file.
func (w *StreamedBlockWriter) init() error {
	symbols, err := w.indexReader.Symbols()
	if err != nil {
		return errors.Wrap(err, "read symbols")
	}

	if err := w.indexWriter.AddSymbols(symbols); err != nil {
		return errors.Wrap(err, "add symbols")
	}

	return nil
}

// finalize sync tmp dir on disk and rename to dir.
func (w *StreamedBlockWriter) finalize() error {
	df, err := fileutil.OpenDir(w.tmpDir)
	if err != nil {
		return errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			if err := df.Close(); err != nil {
				log.Logger(w.logger).Log(err, "close temporary block dir")
			}
		}
	}()

	if err := fileutil.Fsync(df); err != nil {
		return errors.Wrap(err, "sync temporary dir")
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return errors.Wrap(err, "close temporary dir")
	}
	df = nil

	// Block successfully written, make visible and remove old ones.
	err = renameFile(w.tmpDir, w.dir)
	// Assume we cleaned tmp dir up
	w.tmpDir = ""
	if err != nil {
		return errors.Wrap(err, "rename block dir")
	}

	return nil
}

// writeLabelSets fills the index writer with label sets.
func (w *StreamedBlockWriter) writeLabelSets() error {
	s := make([]string, 0, 256)
	for n, v := range w.labelsValues {
		s = s[:0]
		v.get(&s)
		if err := w.indexWriter.WriteLabelIndex([]string{n}, s); err != nil {
			return errors.Wrap(err, "write label index")
		}
	}
	return nil
}

// writeMemPostings fills the index writer with mem postings.
func (w *StreamedBlockWriter) writeMemPostings() error {
	w.memPostings.EnsureOrder()
	for _, l := range w.memPostings.SortedKeys() {
		if err := w.indexWriter.WritePostings(l.Name, l.Value, w.memPostings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}
	return nil
}

// TODO probably tsdb.BlockMeta should expose method writeToFile /w encode.
// writeMetaFile writes meta file
func (w *StreamedBlockWriter) writeMetaFile() error {
	var merr tsdb.MultiError

	w.meta.ULID = w.uid
	w.meta.Version = 1
	w.meta.Thanos.Source = block.CompactorSource
	w.meta.Stats.NumChunks = w.totalChunks
	w.meta.Stats.NumSamples = w.totalSamples
	w.meta.Stats.NumSeries = w.postings

	// Make any changes to the file appear atomic.
	path := filepath.Join(w.tmpDir, block.MetaFilename)

	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "create tmp meta file %s", path)
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	if merr.Add(enc.Encode(w.meta)); merr.Err() != nil {
		merr.Add(f.Close())
		return errors.Wrapf(merr.Err(), "encoding meta file to json %s", path)
	}

	merr.Add(errors.Wrapf(fileutil.Fsync(f), "sync meta file %s", path))
	merr.Add(errors.Wrapf(f.Close(), "close meta file %s", path))

	return merr.Err()
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
