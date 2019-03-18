package downsample

import (
	"io"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/runutil"
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

// streamedBlockWriter writes downsampled blocks to a new data block. Implemented to save memory consumption
// by writing chunks data right into the files, omitting keeping them in-memory. Index and meta data should be
// sealed afterwards, when there aren't more series to process.
type streamedBlockWriter struct {
	blockDir       string
	finalized      bool // set to true, if Close was called
	logger         log.Logger
	ignoreFinalize bool // if true Close does not finalize block due to internal error.
	meta           metadata.Meta
	totalChunks    uint64
	totalSamples   uint64

	chunkWriter tsdb.ChunkWriter
	indexWriter tsdb.IndexWriter
	indexReader tsdb.IndexReader
	closers     []io.Closer

	labelsValues labelsValues       // labelsValues list of used label sets: name -> []values.
	memPostings  *index.MemPostings // memPostings contains references from label name:value -> postings.
	postings     uint64             // postings is a current posting position.
}

// NewStreamedBlockWriter returns streamedBlockWriter instance, it's not concurrency safe.
// Caller is responsible to Close all io.Closers by calling the Close when downsampling is done.
// In case if error happens outside of the StreamedBlockWriter during the processing,
// index and meta files will be written anyway, so the caller is always responsible for removing block directory with
// a garbage on error.
// This approach simplifies StreamedBlockWriter interface, which is a best trade-off taking into account the error is an
// exception, not a general case.
func NewStreamedBlockWriter(
	blockDir string,
	indexReader tsdb.IndexReader,
	logger log.Logger,
	originMeta metadata.Meta,
) (w *streamedBlockWriter, err error) {
	closers := make([]io.Closer, 0, 2)

	// We should close any opened Closer up to an error.
	defer func() {
		if err != nil {
			var merr tsdb.MultiError
			merr.Add(err)
			for _, cl := range closers {
				merr.Add(cl.Close())
			}
			err = merr.Err()
		}
	}()

	chunkWriter, err := chunks.NewWriter(filepath.Join(blockDir, block.ChunksDirname))
	if err != nil {
		return nil, errors.Wrap(err, "create chunk writer in streamedBlockWriter")
	}
	closers = append(closers, chunkWriter)

	indexWriter, err := index.NewWriter(filepath.Join(blockDir, block.IndexFilename))
	if err != nil {
		return nil, errors.Wrap(err, "open index writer in streamedBlockWriter")
	}
	closers = append(closers, indexWriter)

	symbols, err := indexReader.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	err = indexWriter.AddSymbols(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "add symbols")
	}

	return &streamedBlockWriter{
		logger:       logger,
		blockDir:     blockDir,
		indexReader:  indexReader,
		indexWriter:  indexWriter,
		chunkWriter:  chunkWriter,
		meta:         originMeta,
		closers:      closers,
		labelsValues: make(labelsValues, 1024),
		memPostings:  index.NewUnorderedMemPostings(),
	}, nil
}

// WriteSeries writes chunks data to the chunkWriter, writes lset and chunks Metas to indexWrites and adds label sets to
// labelsValues sets and memPostings to be written on the finalize state in the end of downsampling process.
func (w *streamedBlockWriter) WriteSeries(lset labels.Labels, chunks []chunks.Meta) error {
	if w.finalized || w.ignoreFinalize {
		return errors.Errorf("series can't be added, writers has been closed or internal error happened")
	}

	if len(chunks) == 0 {
		level.Warn(w.logger).Log("empty chunks happened, skip series", lset)
		return nil
	}

	if err := w.chunkWriter.WriteChunks(chunks...); err != nil {
		w.ignoreFinalize = true
		return errors.Wrap(err, "add chunks")
	}

	if err := w.indexWriter.AddSeries(w.postings, lset, chunks...); err != nil {
		w.ignoreFinalize = true
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

// Close calls finalizer to complete index and meta files and closes all io.CLoser writers.
// Idempotent.
func (w *streamedBlockWriter) Close() error {
	if w.finalized {
		return nil
	}

	var merr tsdb.MultiError
	w.finalized = true

	// Finalise data block only if there wasn't any internal errors.
	if !w.ignoreFinalize {
		merr.Add(w.finalize())
	}

	for _, cl := range w.closers {
		merr.Add(cl.Close())
	}

	return errors.Wrap(merr.Err(), "close closers")
}

// finalize saves prepared index and meta data to corresponding files.
// It is called on Close. Even if an error happened outside of StreamWriter, it will finalize the block anyway,
// so it's a caller's responsibility to remove the block's directory.
func (w *streamedBlockWriter) finalize() error {
	if err := w.writeLabelSets(); err != nil {
		return errors.Wrap(err, "write label sets")
	}

	if err := w.writeMemPostings(); err != nil {
		return errors.Wrap(err, "write mem postings")
	}

	if err := w.writeMetaFile(); err != nil {
		return errors.Wrap(err, "write meta meta")
	}

	if err := w.syncDir(); err != nil {
		return errors.Wrap(err, "sync blockDir")
	}

	level.Info(w.logger).Log(
		"msg", "write downsampled block",
		"mint", w.meta.MinTime,
		"maxt", w.meta.MaxTime,
		"ulid", w.meta.ULID,
		"resolution", w.meta.Thanos.Downsample.Resolution,
	)
	return nil
}

// syncDir syncs blockDir on disk.
func (w *streamedBlockWriter) syncDir() (err error) {
	df, err := fileutil.OpenDir(w.blockDir)
	if err != nil {
		return errors.Wrap(err, "open temporary block blockDir")
	}

	defer runutil.CloseWithErrCapture(w.logger, &err, df, "close temporary block blockDir")

	if err := fileutil.Fsync(df); err != nil {
		return errors.Wrap(err, "sync temporary blockDir")
	}

	return nil
}

// writeLabelSets fills the index writer with label sets.
func (w *streamedBlockWriter) writeLabelSets() error {
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
func (w *streamedBlockWriter) writeMemPostings() error {
	w.memPostings.EnsureOrder()
	for _, l := range w.memPostings.SortedKeys() {
		if err := w.indexWriter.WritePostings(l.Name, l.Value, w.memPostings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}
	return nil
}

// writeMetaFile writes meta file.
func (w *streamedBlockWriter) writeMetaFile() error {
	w.meta.Version = metadata.MetaVersion1
	w.meta.Thanos.Source = metadata.CompactorSource
	w.meta.Stats.NumChunks = w.totalChunks
	w.meta.Stats.NumSamples = w.totalSamples
	w.meta.Stats.NumSeries = w.postings

	return metadata.Write(w.logger, w.blockDir, &w.meta)
}
