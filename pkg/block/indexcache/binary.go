package indexcache

import (
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// BinaryCache is a binary cache.
type BinaryCache struct {
	IndexCache

	logger log.Logger
}

// WriteIndexCache writes an index cache into the specified filename.
func (c *BinaryCache) WriteIndexCache(indexFn string, fn string) error {
	indexFile, err := fileutil.OpenMmapFile(indexFn)
	if err != nil {
		return errors.Wrapf(err, "open mmap index file %s", indexFn)
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexFile, "close index cache mmap file from %s", indexFn)

	b := realByteSlice(indexFile.Bytes())
	indexr, err := index.NewReader(b)
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexr, "load index cache reader")

	// We assume reader verified index already.
	symbols, err := getSymbolTable(b)
	if err != nil {
		return err
	}

	// Now it is time to write it.
	w, err := index.NewWriter(fn)
	if err != nil {
		return err
	}
	defer runutil.CloseWithLogOnErr(c.logger, w, "index writer")

	err = w.AddSymbols(symbols)
	if err != nil {
		return err
	}

	return nil
}

// ReadIndexCache reads the index cache from the specified file.
func (c *BinaryCache) ReadIndexCache(fn string) (version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error) {
	return 0, nil, nil, nil, nil
}
