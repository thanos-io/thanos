package e2e

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"strings"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore/inmemcache"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
)

func TestBucketStore_e2e(t *testing.T) {
	bkt, cleanup := testutil.NewObjectStoreBucket(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_bucketstore_e2e")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "2", "b", "1"),
		labels.FromStrings("a", "2", "b", "2"),
		labels.FromStrings("a", "1", "c", "1"),
		labels.FromStrings("a", "1", "c", "2"),
		labels.FromStrings("a", "2", "c", "1"),
		labels.FromStrings("a", "2", "c", "2"),
	}
	start := time.Now()
	now := start

	minTime := int64(0)
	maxTime := int64(0)
	for i := 0; i < 3; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		if minTime == 0 {
			minTime = mint
		}
		maxTime = maxt

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := testutil.CreateBlock(dir, series[:4], 10, mint, maxt)
		testutil.Ok(t, err)
		id2, err := testutil.CreateBlock(dir, series[4:], 10, mint, maxt)
		testutil.Ok(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Add labels to the meta of the second block.
		meta, err := block.ReadMetaFile(dir2)
		testutil.Ok(t, err)
		meta.Thanos.Labels = map[string]string{"ext": "value"}
		testutil.Ok(t, block.WriteMetaFile(dir2, meta))

		// TODO(fabxc): remove the component dependency by factoring out the block interface.
		testutil.Ok(t, uploadDir(t, ctx, bkt, id1, dir1))
		testutil.Ok(t, uploadDir(t, ctx, bkt, id2, dir2))

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	var gossipMinTime, gossipMaxTime int64
	store, err := store.NewBucketStore(nil, nil, bkt, inmemcache.NewWholeObjectsBucket(nil, bkt, 512*1024), func(mint int64, maxt int64) {
		gossipMinTime = mint
		gossipMaxTime = maxt
	}, dir)
	testutil.Ok(t, err)

	go func() {
		runutil.Repeat(100*time.Millisecond, ctx.Done(), func() error {
			return store.SyncBlocks(ctx)
		})
	}()

	ctx, _ = context.WithTimeout(ctx, 30*time.Second)

	err = runutil.Retry(100*time.Millisecond, ctx.Done(), func() error {
		if store.NumBlocks() < 6 {
			return errors.New("not all blocks loaded")
		}
		return nil
	})
	testutil.Ok(t, err)

	testutil.Equals(t, minTime, gossipMinTime)
	testutil.Equals(t, maxTime, gossipMaxTime)

	vals, err := store.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "a"})
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1", "2"}, vals.Values)

	pbseries := [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
	}
	srv := &testStoreSeriesServer{ctx: ctx}

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.series))

	for i, s := range srv.series {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	pbseries = [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
	}
	srv = &testStoreSeriesServer{ctx: ctx}

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.series))

	for i, s := range srv.series {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	// Matching by external label should work as well.
	pbseries = [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
	}
	srv = &testStoreSeriesServer{ctx: ctx}

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
			{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "value"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.series))

	for i, s := range srv.series {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	srv = &testStoreSeriesServer{ctx: ctx}
	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
			{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "wrong-value"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(srv.series))
}

type testStoreSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx    context.Context
	series []storepb.Series
}

func (s *testStoreSeriesServer) Send(r *storepb.SeriesResponse) error {
	s.series = append(s.series, r.Series)
	return nil
}

func (s *testStoreSeriesServer) Context() context.Context {
	return s.ctx
}

func uploadDir(t testing.TB, ctx context.Context, bkt shipper.Bucket, id ulid.ULID, dir string) error {
	err := filepath.Walk(dir, func(src string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}

		target := filepath.Join(id.String(), strings.TrimPrefix(src, dir))
		return bkt.Upload(ctx, src, target)
	})
	if err == nil {
		return nil
	}

	// We don't want to leave partially uploaded directories behind. Cleanup everything related to it
	// and use a uncanceled context.
	if err2 := bkt.Delete(ctx, dir); err2 != nil {
		t.Logf("cleanup failed; partial data may be left behind. dir: %s. Err: %v", dir, err2)
	}
	return err
}
