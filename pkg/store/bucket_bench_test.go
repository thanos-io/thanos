package store

import (
	"context"
	"fmt"
	"github.com/improbable-eng/thanos/pkg/objstore/inmem"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)

func calcResponseSize(resp []storepb.Series) {
	labelsSize := 0
	encSamplesSize := 0
	for _, s := range resp {
		for _, l := range s.Labels {
			labelsSize += len(l.Name)
			labelsSize += len(l.Value)
		}

		for _, c := range s.Chunks {
			encSamplesSize += len(c.Raw.Data)
		}
	}
	// SamplesSize: 2106 All 8 series size: 112 Total: 2218 for `prepareStoreWithTestBlocks`
	fmt.Println("SamplesSize:", encSamplesSize, "Sample size:",float64(encSamplesSize)/240.0 ,"All 8 series size:", labelsSize, "Total:", encSamplesSize+labelsSize)
}

func TestBucketStore_Mem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_bench_bucketstore")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	bkt := inmem.NewBucket()
	// 3 * 2 blocks each 4 series with 10 samples each.
	// 3 * 2 * 4 * 10 = 240
	s := prepareStoreWithTestBlocks(t, dir, bkt, false, 0, 0, 20)
	defer s.Close()

	s.cache.SwapWith(noopCache{})

	// GIMME ALL!
	req := &storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".*"},
		},
		MinTime: s.minTime,
		MaxTime: s.maxTime,
	}
	expected := [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
	}

	runtime.GC()
	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	fmt.Println("GC:", m.NumGC)

	for i:=0; i<1000; i++ {
		srv := newStoreSeriesServer(ctx)
		testutil.Ok(t, s.store.Series(req, srv))
		result = srv.SeriesSet
		testutil.Equals(t, len(expected), len(result))
	}

	runtime.GC()
	m2 := runtime.MemStats{}
	runtime.ReadMemStats(&m2)
	fmt.Println("GC:", m2.NumGC)

	// heapDiff: 4802768 (4MB) data extra allocated after 1000 calls, even though 2KB of response.
	fmt.Println("AllocDiff:", m2.Alloc - m.Alloc)
	fmt.Println("TAllocDiff/op:", float64(m2.TotalAlloc - m.TotalAlloc) / 1000.0) // 130763 B/op
}

/*
=== RUN   TestBucketStore_Mem
GC: 13
overall: 750856
GC: 63
AllocDiff: 4802768
TAllocDiff/op: 123157.52
--- PASS: TestBucketStore_Mem (0.58s)
PASS
 */

func BenchmarkBucketStore(b *testing.B) {
	b.ReportAllocs()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_bench_bucketstore")
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, os.RemoveAll(dir)) }()

	bkt := inmem.NewBucket()
	s := prepareStoreWithTestBlocks(b, dir, bkt, false, 0, 0, 20)
	defer s.Close()

	s.cache.SwapWith(noopCache{})

	req := &storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".*"},
		},
		MinTime: s.minTime,
		MaxTime: s.maxTime,
	}
	expected := [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
	}

	b.ResetTimer()
	for i:=0; i<b.N; i++ {
		srv := newStoreSeriesServer(ctx)

		testutil.Ok(b, s.store.Series(req, srv))
		result = srv.SeriesSet
		testutil.Equals(b, len(expected), len(result))
	}
}

// This is to compiler hack optimizer.
var result []storepb.Series

/*
go test ./pkg/store/... -run=xxx -bench . -test.benchtime=5s
goos: linux
goarch: amd64
pkg: github.com/improbable-eng/thanos/pkg/store
BenchmarkBucketStore-12    	   50000	    144791 ns/op	   91849 B/op	     965 allocs/op
PASS
ok  	github.com/improbable-eng/thanos/pkg/store	10.684s

*/
