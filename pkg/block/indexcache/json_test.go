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

	fn := filepath.Join(tmpDir, "index.cache.json")
	fnB := filepath.Join(tmpDir, "index.cache.dat")
	l := log.NewNopLogger()
	j := JSONCache{logger: l}

	testutil.Ok(t, j.WriteIndexCache(filepath.Join(tmpDir, b.String(), "index"), fn))
	version, symbols, lvals, postings, err := j.ReadIndexCache(fn)
	testutil.Ok(t, err)
	testutil.Ok(t, j.ToBCache(fn, fnB))

	bcache := BinaryCache{logger: l}
	versionB, symbolsB, lvalsB, postingsB, err := bcache.ReadIndexCache(fnB)
	testutil.Ok(t, err)

	testutil.Equals(t, symbols, symbolsB)
	testutil.Equals(t, lvals, lvalsB)
	testutil.Equals(t, version, versionB)

	for _, ls := range lbls {
		for _, l := range ls {
			testutil.Equals(t, postings[l], postingsB[l])
		}
	}
}
