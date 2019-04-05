package indexcache

import (
	"github.com/go-kit/kit/log"
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
func (c *JSONIndexCache) ReadIndexCache(fn string) error {
	return nil
}

// ToFBCache converts the JSON cache into a Flatbuffer one.
func (c *JSONIndexCache) ToFBCache(fnJSON string, fnFB string) error {
	return nil
}
