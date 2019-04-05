package indexcache

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// JSONIndexCache is a JSON index cache.
type JSONIndexCache struct {
	IndexCache

	logger log.Logger
}

// WriteIndexCache writes an index cache into the specified filename.
func (c *JSONIndexCache) WriteIndexCache(indexFn string, fn string) error {
	return nil
}

// ReadIndexCache reads the index cache from the specified file.
func (c *JSONIndexCache) ReadIndexCache(fn string) (version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error) {
	return 0, nil, nil, nil, nil
}

// ToFBCache converts the JSON cache into a Flatbuffer one.
func (c *JSONIndexCache) ToFBCache(fnJSON string, fnFB string) error {
	return nil
}
