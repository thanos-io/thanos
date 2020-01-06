package indexheader

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	b, err := testutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
	}, 100, 0, 1000, nil, 124)
	testutil.Ok(t, err)

	t.Run("JSON", func(t *testing.T) {
		fn := filepath.Join(tmpDir, b.String(), "index.cache.json")
		testutil.Ok(t, WriteJSON(log.NewNopLogger(), filepath.Join(tmpDir, b.String(), "index"), fn))

		jr, err := NewJSONReader(ctx, log.NewNopLogger(), nil, tmpDir, b)
		testutil.Ok(t, err)

		testutil.Equals(t, 6, len(jr.symbols))
		testutil.Equals(t, 2, len(jr.lvals))
		testutil.Equals(t, 6, len(jr.postings))

		testReader(t, jr)
	})
}

func testReader(t *testing.T, r Reader) {
	testutil.Equals(t, 2, r.IndexVersion())
	exp := []string{"1", "2", "3", "4", "a", "b"}
	for i := range exp {
		r, err := r.LookupSymbol(uint32(i))
		testutil.Ok(t, err)
		testutil.Equals(t, exp[i], r)
	}
	_, err := r.LookupSymbol(uint32(len(exp)))
	testutil.NotOk(t, err)

	testutil.Equals(t, []string{"1", "2", "3", "4"}, r.LabelValues("a"))
	testutil.Equals(t, []string{"1"}, r.LabelValues("b"))
	testutil.Equals(t, []string{}, r.LabelValues("c"))

	testutil.Equals(t, []string{"a", "b"}, r.LabelNames())

	ptr := r.PostingsOffset("a", "1")
	testutil.Equals(t, index.Range{Start: 200, End: 212}, ptr)

	ptr = r.PostingsOffset("a", "2")
	testutil.Equals(t, index.Range{Start: 220, End: 228}, ptr)

	ptr = r.PostingsOffset("b", "2")
	testutil.Equals(t, NotFoundRange, ptr)
}
