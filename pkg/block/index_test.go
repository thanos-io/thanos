package block

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestWriteReadIndexCache(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-compact-prepare")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	b, err := testutil.CreateBlock(tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, nil, 124)
	testutil.Ok(t, err)

	fn := filepath.Join(tmpDir, "index.cache.json")
	testutil.Ok(t, WriteIndexCache(log.NewNopLogger(), filepath.Join(tmpDir, b.String(), "index"), fn))

	version, symbols, lvals, postings, err := ReadIndexCache(log.NewNopLogger(), fn)
	testutil.Ok(t, err)

	testutil.Equals(t, 2, version)
	testutil.Equals(t, 6, len(symbols))
	testutil.Equals(t, 2, len(lvals))

	vals, ok := lvals["a"]
	testutil.Assert(t, ok, "")
	testutil.Equals(t, []string{"1", "2", "3", "4"}, vals)

	vals, ok = lvals["b"]
	testutil.Assert(t, ok, "")
	testutil.Equals(t, []string{"1"}, vals)
	testutil.Equals(t, 6, len(postings))
}
