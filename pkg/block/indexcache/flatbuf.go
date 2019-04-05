package indexcache

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// FBIndexCache is a FlatBuffer index cache.
type FBIndexCache struct {
	IndexCache

	logger log.Logger
}

// WriteIndexCache writes an index cache into the specified filename.
func (c *FBIndexCache) WriteIndexCache(indexFn string, fn string) error {
	return nil
}

// ReadIndexCache reads the index cache from the specified file.
func (c *FBIndexCache) ReadIndexCache(fn string) (version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error) {
	return 0, nil, nil, nil, nil
}
