package store

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/block"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/shipper"

	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"

	"github.com/improbable-eng/thanos/pkg/cluster/mocks"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestGCSStore_downloadBlocks(t *testing.T) {
	bkt, cleanup := testutil.NewObjectStoreBucket(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expBlocks := []ulid.ULID{}
	expFiles := map[string][]byte{}

	randr := rand.New(rand.NewSource(0))

	// Generate 5 blocks with random data and add it to the bucket.
	for i := int64(0); i < 3; i++ {
		id := ulid.MustNew(uint64(i), randr)
		expBlocks = append(expBlocks, id)

		for _, s := range []string{
			path.Join(id.String(), "index"),
			path.Join(id.String(), "chunks/0001"),
			path.Join(id.String(), "chunks/0002"),
			path.Join(id.String(), "meta.json"),
		} {
			w := bkt.Object(s).NewWriter(ctx)
			b := make([]byte, 64*1024)

			_, err := randr.Read(b)
			testutil.Ok(t, err)
			_, err = w.Write(b)
			testutil.Ok(t, err)
			testutil.Ok(t, w.Close())

			if strings.HasSuffix(s, "index") || strings.HasSuffix(s, "meta.json") {
				expFiles[s] = b
			}
		}
	}

	dir, err := ioutil.TempDir("", "test-syncBlocks")
	testutil.Ok(t, err)

	s, err := NewGCSStore(nil, nil, bkt, nil, dir)
	testutil.Ok(t, err)
	defer s.Close()

	testutil.Ok(t, s.downloadBlocks(ctx))

	fns, err := fileutil.ReadDir(dir)
	testutil.Ok(t, err)

	testutil.Assert(t, len(expBlocks) == len(fns), "invalid block count")
	for _, id := range expBlocks {
		_, err := os.Stat(filepath.Join(dir, id.String()))
		testutil.Assert(t, err == nil, "block %s not synced", id)
	}
	// For each block we expect a downloaded index file.
	for fn, data := range expFiles {
		b, err := ioutil.ReadFile(filepath.Join(dir, fn))
		testutil.Ok(t, err)
		testutil.Equals(t, data, b)
	}
}

func TestGCSStore_e2e(t *testing.T) {
	bkt, cleanup := testutil.NewObjectStoreBucket(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_gcsstore_e2e")
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
	remote := shipper.NewGCSRemote(log.NewNopLogger(), nil, bkt)

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
		testutil.Ok(t, remote.Upload(ctx, id1, dir1))
		testutil.Ok(t, remote.Upload(ctx, id2, dir2))

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	var gossipMinTime, gossipMaxTime int64
	store, err := NewGCSStore(nil, nil, bkt, func(mint int64, maxt int64) {
		gossipMinTime = mint
		gossipMaxTime = maxt
	}, dir)
	testutil.Ok(t, err)

	go store.SyncBlocks(ctx, 100*time.Millisecond)

	ctx, _ = context.WithTimeout(ctx, 30*time.Second)

	err = runutil.Retry(100*time.Millisecond, ctx.Done(), func() error {
		if store.numBlocks() < 6 {
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
}

func TestGCSBlock_matches(t *testing.T) {
	makeMeta := func(mint, maxt int64, lset map[string]string) *block.Meta {
		return &block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: mint,
				MaxTime: maxt,
			},
			Thanos: block.ThanosMeta{Labels: lset},
		}
	}
	cases := []struct {
		meta       *block.Meta
		mint, maxt int64
		matchers   []labels.Matcher
		ok         bool
	}{
		{
			meta: makeMeta(100, 200, nil),
			mint: 0,
			maxt: 99,
			ok:   false,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 0,
			maxt: 100,
			ok:   true,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 201,
			maxt: 250,
			ok:   false,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 200,
			maxt: 250,
			ok:   true,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			ok: true,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			ok: true,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "c"),
			},
			ok: false,
		},
	}

	for i, c := range cases {
		b := &gcsBlock{meta: c.meta}
		ok := b.matches(c.mint, c.maxt, c.matchers...)
		testutil.Assert(t, c.ok == ok, "test case %d failed", i)
	}
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
