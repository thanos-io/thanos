package indexheader

import (
	"github.com/prometheus/prometheus/tsdb/index"
)

// NotFoundRange is a range returned by PostingsOffset when there is no posting for given name and value pairs.
// Has to be default value of index.Range.
var NotFoundRange = index.Range{}

// Reader is an interface allowing to read essential, minimal number of index entries from the small portion of index file called header.
type Reader interface {
	IndexVersion() int
	// TODO(bwplotka): Move to PostingsOffsets(name string, value ...string) []index.Range and benchmark.
	PostingsOffset(name string, value string) index.Range
	LookupSymbol(o uint32) (string, error)
	LabelValues(name string) []string
	LabelNames() []string
}
