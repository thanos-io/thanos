package indexcache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestWriteReadJSONIndexCache(t *testing.T) {
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
	l := log.NewNopLogger()
	j := JSONCache{logger: l}

	testutil.Ok(t, j.WriteIndexCache(filepath.Join(tmpDir, b.String(), "index"), fn))

	version, symbols, lvals, postings, err := j.ReadIndexCache(fn)
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

func TestConvertJSONToBinaryCache(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-convert-cache")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	lbls := []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}

	b, err := testutil.CreateBlock(tmpDir, lbls, 100, 0, 1000, nil, 124)
	testutil.Ok(t, err)

	l := log.NewNopLogger()
	j := JSONCache{logger: l}
	bcache := BinaryCache{logger: l}

	fn := filepath.Join(tmpDir, "index.cache.json")
	fnB := filepath.Join(tmpDir, "index.cache.dat")
	fnBFresh := filepath.Join(tmpDir, "index.cache.fresh.dat")

	testutil.Ok(t, j.WriteIndexCache(filepath.Join(tmpDir, b.String(), "index"), fn))
	version, symbols, lvals, postings, err := j.ReadIndexCache(fn)
	testutil.Ok(t, err)
	testutil.Ok(t, j.ToBCache(fn, fnB))

	versionB, symbolsB, lvalsB, postingsB, err := bcache.ReadIndexCache(fnB)
	testutil.Ok(t, err)

	testutil.Ok(t, bcache.WriteIndexCache(filepath.Join(tmpDir, b.String(), "index"), fnBFresh))
	versionBF, symbolsBF, lvalsBF, postingsBF, err := bcache.ReadIndexCache(fnBFresh)
	testutil.Ok(t, err)

	// compare fresh binary and JSON caches
	testutil.Equals(t, symbols, symbolsBF)
	testutil.Equals(t, lvals, lvalsBF)
	testutil.Equals(t, version, versionBF)

	// Compare converted and fresh JSON cache
	testutil.Equals(t, symbolsBF, symbolsB)
	testutil.Equals(t, lvalsBF, lvalsB)
	testutil.Equals(t, versionBF, versionB)

	// the JSON -> binary function
	testutil.Equals(t, symbols, symbolsB)
	testutil.Equals(t, lvals, lvalsB)
	testutil.Equals(t, version, versionB)

	testutil.Equals(t, postings, postingsBF)
	testutil.Equals(t, postings, postingsB)
	testutil.Equals(t, postingsBF, postingsB)

}
