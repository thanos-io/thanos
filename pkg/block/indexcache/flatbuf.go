package indexcache

import (
	"github.com/go-kit/kit/log"
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
func (c *FBIndexCache) ReadIndexCache(fn string) error {
	return nil
}
