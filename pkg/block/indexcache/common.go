package indexcache

import (
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// IndexCache is a provider of an index cache with a different underlying implementation.
type IndexCache interface {
	WriteIndexCache(indexFn string, fn string) error
	ReadIndexCache(fn string) (version int,
		symbols map[uint32]string,
		lvals map[string][]string,
		postings map[labels.Label]index.Range,
		err error)
}
